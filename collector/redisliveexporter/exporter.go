// Package redisliveexporter publishes each project's telemetry to that project's Redis channel,
// where control-plane replicas stream it to observers.
package redisliveexporter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// The resource processor stamps this from verified host claims before any exporter runs.
const projectAttribute = "durable_actors.project_id"

// Channel must match live_channel in the control plane (src/request_traces/otlp.rs).
func Channel(project, signal string) string {
	return "durable-actors:v1:live:" + project + ":" + signal
}

type liveExporter struct {
	config *Config
	store  liveStore
}

func (e *liveExporter) start(context.Context, component.Host) error {
	options, err := redis.ParseURL(string(e.config.Endpoint))
	if err != nil {
		return err
	}
	if e.config.CAFile != "" {
		if options.TLSConfig, err = trust(options.TLSConfig, e.config.CAFile); err != nil {
			return err
		}
	}
	e.store = redisStore{redis.NewClient(options)}
	return nil
}

func (e *liveExporter) shutdown(context.Context) error {
	if e.store == nil {
		return nil
	}
	return e.store.close()
}

func (e *liveExporter) pushTraces(ctx context.Context, traces ptrace.Traces) error {
	payloads, err := tracesByProject(traces)
	if err != nil {
		return err
	}
	return e.publish(ctx, "traces", payloads)
}

func (e *liveExporter) pushLogs(ctx context.Context, logs plog.Logs) error {
	// Claims name projects in their records, so only trusted senders may record them.
	if e.config.ProjectFromRecords {
		if err := e.store.remember(ctx, sandboxClaims(logs)); err != nil {
			return err
		}
	}
	payloads, err := logsByProject(logs, e.config.ProjectFromRecords)
	if err != nil {
		return err
	}
	return e.publish(ctx, e.logsChannel(), payloads)
}

func (e *liveExporter) logsChannel() string {
	if e.config.LogsChannel == "" {
		return "logs"
	}
	return e.config.LogsChannel
}

func (e *liveExporter) publish(ctx context.Context, signal string, payloads map[string][]byte) error {
	if len(payloads) == 0 {
		return nil
	}
	messages := make(map[string][]byte, len(payloads))
	for project, payload := range payloads {
		messages[Channel(project, signal)] = payload
	}
	return e.store.publish(ctx, messages)
}

func tracesByProject(traces ptrace.Traces) (map[string][]byte, error) {
	batches := map[string]ptrace.Traces{}
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		spans := traces.ResourceSpans().At(i)
		project := projectOf(spans.Resource().Attributes().Get(projectAttribute))
		if project == "" {
			continue
		}
		batch, found := batches[project]
		if !found {
			batch = ptrace.NewTraces()
			batches[project] = batch
		}
		spans.CopyTo(batch.ResourceSpans().AppendEmpty())
	}
	payloads := make(map[string][]byte, len(batches))
	for project, batch := range batches {
		payload, err := ptraceotlp.NewExportRequestFromTraces(batch).MarshalProto()
		if err != nil {
			return nil, err
		}
		payloads[project] = payload
	}
	return payloads, nil
}

func logsByProject(logs plog.Logs, fromRecords bool) (map[string][]byte, error) {
	batches := map[string]plog.Logs{}
	batch := func(project string) plog.Logs {
		found, ok := batches[project]
		if !ok {
			found = plog.NewLogs()
			batches[project] = found
		}
		return found
	}
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		records := logs.ResourceLogs().At(i)
		if project := projectOf(records.Resource().Attributes().Get(projectAttribute)); project != "" {
			records.CopyTo(batch(project).ResourceLogs().AppendEmpty())
		} else if fromRecords {
			splitRecords(records, batch)
		}
	}
	return marshalLogs(batches)
}

func marshalLogs(batches map[string]plog.Logs) (map[string][]byte, error) {
	payloads := make(map[string][]byte, len(batches))
	for project, batch := range batches {
		payload, err := plogotlp.NewExportRequestFromLogs(batch).MarshalProto()
		if err != nil {
			return nil, err
		}
		payloads[project] = payload
	}
	return payloads, nil
}

// splitRecords files each record under its own project, keeping its resource and scope.
func splitRecords(records plog.ResourceLogs, batch func(string) plog.Logs) {
	for j := 0; j < records.ScopeLogs().Len(); j++ {
		scope := records.ScopeLogs().At(j)
		for k := 0; k < scope.LogRecords().Len(); k++ {
			record := scope.LogRecords().At(k)
			project := projectOf(record.Attributes().Get(projectAttribute))
			if project == "" {
				continue
			}
			target := batch(project).ResourceLogs().AppendEmpty()
			records.Resource().CopyTo(target.Resource())
			scoped := target.ScopeLogs().AppendEmpty()
			scope.Scope().CopyTo(scoped.Scope())
			record.CopyTo(scoped.LogRecords().AppendEmpty())
		}
	}
}

func projectOf(value interface{ AsString() string }, found bool) string {
	if !found {
		return ""
	}
	return value.AsString()
}

func trust(config *tls.Config, caFile string) (*tls.Config, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("ca_file contains no certificates")
	}
	if config == nil {
		config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	config.RootCAs = roots
	return config, nil
}
