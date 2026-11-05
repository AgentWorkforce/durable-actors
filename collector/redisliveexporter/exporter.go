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
	client *redis.Client
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
	e.client = redis.NewClient(options)
	return nil
}

func (e *liveExporter) shutdown(context.Context) error {
	if e.client == nil {
		return nil
	}
	return e.client.Close()
}

func (e *liveExporter) pushTraces(ctx context.Context, traces ptrace.Traces) error {
	payloads, err := tracesByProject(traces)
	if err != nil {
		return err
	}
	return e.publish(ctx, "traces", payloads)
}

func (e *liveExporter) pushLogs(ctx context.Context, logs plog.Logs) error {
	payloads, err := logsByProject(logs)
	if err != nil {
		return err
	}
	return e.publish(ctx, "logs", payloads)
}

func (e *liveExporter) publish(ctx context.Context, signal string, payloads map[string][]byte) error {
	if len(payloads) == 0 {
		return nil
	}
	pipeline := e.client.Pipeline()
	for project, payload := range payloads {
		pipeline.Publish(ctx, Channel(project, signal), payload)
	}
	_, err := pipeline.Exec(ctx)
	return err
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

func logsByProject(logs plog.Logs) (map[string][]byte, error) {
	batches := map[string]plog.Logs{}
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		records := logs.ResourceLogs().At(i)
		project := projectOf(records.Resource().Attributes().Get(projectAttribute))
		if project == "" {
			continue
		}
		batch, found := batches[project]
		if !found {
			batch = plog.NewLogs()
			batches[project] = batch
		}
		records.CopyTo(batch.ResourceLogs().AppendEmpty())
	}
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
