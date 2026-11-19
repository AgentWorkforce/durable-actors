package redisliveexporter

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	claimedEvent       = "durable_actors.sandbox.claimed"
	usageEvent         = "durable_actors.sandbox.usage"
	actorNameAttribute = "durable_actors.actor_name"
	actorIDAttribute   = "durable_actors.actor_id"
	sandboxAttribute   = "durable_actors.sandbox_id"
	podAttribute       = "k8s.pod.name"
	cpuMetric          = "container.cpu.usage"
	memoryMetric       = "container.memory.working_set"
)

// sandboxActor is the actor a sandbox was claimed for, as Redis keeps it.
type sandboxActor struct {
	ProjectID string `json:"projectId"`
	ActorName string `json:"actorName"`
	ActorID   string `json:"actorId"`
}

// pushMetrics publishes each claimed sandbox's usage to its actor's project. Usage of a sandbox
// no claim names yet, such as a spare, is dropped.
func (e *liveExporter) pushMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	samples := sandboxSamples(metrics)
	if len(samples) == 0 {
		return nil
	}
	actors, err := e.store.recall(ctx, sandboxesOf(samples))
	if err != nil {
		return err
	}
	payloads, err := usageByProject(samples, actors)
	if err != nil {
		return err
	}
	return e.publish(ctx, e.logsChannel(), payloads)
}

// sandboxClaims maps each sandbox the logs claim to its actor, encoded as Redis keeps it.
func sandboxClaims(logs plog.Logs) map[string]string {
	claims := map[string]string{}
	for _, record := range logRecords(logs) {
		if record.EventName() != claimedEvent {
			continue
		}
		attributes := record.Attributes()
		sandbox := stringOf(attributes, sandboxAttribute)
		actor := sandboxActor{
			ProjectID: stringOf(attributes, projectAttribute),
			ActorName: stringOf(attributes, actorNameAttribute),
			ActorID:   stringOf(attributes, actorIDAttribute),
		}
		if encoded, err := json.Marshal(actor); err == nil && sandbox != "" && actor.ProjectID != "" {
			claims[sandbox] = string(encoded)
		}
	}
	return claims
}

func logRecords(logs plog.Logs) []plog.LogRecord {
	var records []plog.LogRecord
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		scopes := logs.ResourceLogs().At(i).ScopeLogs()
		for j := 0; j < scopes.Len(); j++ {
			for k := 0; k < scopes.At(j).LogRecords().Len(); k++ {
				records = append(records, scopes.At(j).LogRecords().At(k))
			}
		}
	}
	return records
}

// usage is one sandbox's CPU and memory at one instant, as the usage record's body carries it.
type usage struct {
	CPUCores    *float64 `json:"cpuCores,omitempty"`
	MemoryBytes *int64   `json:"memoryBytes,omitempty"`
}

type sampleKey struct {
	sandbox string
	at      pcommon.Timestamp
}

type sample struct {
	sampleKey
	usage usage
}

// sandboxSamples joins each sandbox's CPU and memory points that share a timestamp.
func sandboxSamples(metrics pmetric.Metrics) []*sample {
	var samples []*sample
	index := map[sampleKey]*sample{}
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		resource := metrics.ResourceMetrics().At(i)
		sandbox := stringOf(resource.Resource().Attributes(), podAttribute)
		if sandbox == "" {
			continue
		}
		for _, point := range usagePoints(resource) {
			key := sampleKey{sandbox, point.Timestamp()}
			found, ok := index[key]
			if !ok {
				found = &sample{sampleKey: key}
				index[key] = found
				samples = append(samples, found)
			}
			found.usage.set(point.metric, point.NumberDataPoint)
		}
	}
	return samples
}

type usagePoint struct {
	pmetric.NumberDataPoint
	metric string
}

func usagePoints(resource pmetric.ResourceMetrics) []usagePoint {
	var points []usagePoint
	for i := 0; i < resource.ScopeMetrics().Len(); i++ {
		metrics := resource.ScopeMetrics().At(i).Metrics()
		for j := 0; j < metrics.Len(); j++ {
			metric := metrics.At(j)
			if metric.Type() != pmetric.MetricTypeGauge || (metric.Name() != cpuMetric && metric.Name() != memoryMetric) {
				continue
			}
			for k := 0; k < metric.Gauge().DataPoints().Len(); k++ {
				points = append(points, usagePoint{metric.Gauge().DataPoints().At(k), metric.Name()})
			}
		}
	}
	return points
}

func (u *usage) set(metric string, point pmetric.NumberDataPoint) {
	if metric == cpuMetric {
		cores := point.DoubleValue()
		if point.ValueType() == pmetric.NumberDataPointValueTypeInt {
			cores = float64(point.IntValue())
		}
		u.CPUCores = &cores
		return
	}
	bytes := point.IntValue()
	if point.ValueType() == pmetric.NumberDataPointValueTypeDouble {
		bytes = int64(point.DoubleValue())
	}
	u.MemoryBytes = &bytes
}

func sandboxesOf(samples []*sample) []string {
	seen := map[string]bool{}
	var sandboxes []string
	for _, sample := range samples {
		if !seen[sample.sandbox] {
			seen[sample.sandbox] = true
			sandboxes = append(sandboxes, sample.sandbox)
		}
	}
	return sandboxes
}

// usageByProject turns each claimed sample into a usage record of its actor. The records never
// name the sandbox: which one ran an actor is the platform's concern, not the observer's.
func usageByProject(samples []*sample, actors map[string]string) (map[string][]byte, error) {
	batches := map[string]plog.Logs{}
	for _, sample := range samples {
		var actor sandboxActor
		if json.Unmarshal([]byte(actors[sample.sandbox]), &actor) != nil || actor.ProjectID == "" {
			continue
		}
		if err := sample.writeTo(recordsOf(batches, actor.ProjectID).AppendEmpty(), actor); err != nil {
			return nil, err
		}
	}
	return marshalLogs(batches)
}

func recordsOf(batches map[string]plog.Logs, project string) plog.LogRecordSlice {
	batch, found := batches[project]
	if !found {
		batch = plog.NewLogs()
		batch.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		batches[project] = batch
	}
	return batch.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
}

func (s *sample) writeTo(record plog.LogRecord, actor sandboxActor) error {
	body, err := json.Marshal(s.usage)
	if err != nil {
		return err
	}
	record.SetEventName(usageEvent)
	record.SetTimestamp(s.at)
	record.Attributes().PutStr(projectAttribute, actor.ProjectID)
	record.Attributes().PutStr(actorNameAttribute, actor.ActorName)
	record.Attributes().PutStr(actorIDAttribute, actor.ActorID)
	record.Body().SetStr(string(body))
	return nil
}

func stringOf(attributes pcommon.Map, key string) string {
	return projectOf(attributes.Get(key))
}
