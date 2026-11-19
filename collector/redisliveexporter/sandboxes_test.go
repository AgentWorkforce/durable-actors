package redisliveexporter

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestUsageReachesTheProjectOfTheClaimedActor(t *testing.T) {
	store := &memoryStore{}
	live := &liveExporter{config: &Config{LogsChannel: "platform", ProjectFromRecords: true}, store: store}
	if err := live.pushLogs(context.Background(), claim("do-spare-a1", "alpha", "Counter", "42")); err != nil {
		t.Fatal(err)
	}
	store.published = nil

	if err := live.pushMetrics(context.Background(), podUsage("do-spare-a1", "do-spare-b2")); err != nil {
		t.Fatal(err)
	}

	request := plogotlp.NewExportRequest()
	if len(store.published) != 1 || request.UnmarshalProto(store.published[Channel("alpha", "platform")]) != nil {
		t.Fatalf("expected one message on alpha's platform channel, got %v", store.published)
	}
	if request.Logs().LogRecordCount() != 1 {
		t.Fatalf("expected one usage record, got %d", request.Logs().LogRecordCount())
	}
	record := request.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	actorID, _ := record.Attributes().Get(actorIDAttribute)
	if record.EventName() != usageEvent || actorID.AsString() != "42" || record.Body().AsString() != `{"cpuCores":0.25,"memoryBytes":1048576}` {
		t.Fatalf("unexpected usage record %q %q %q", record.EventName(), actorID.AsString(), record.Body().AsString())
	}
	if _, named := record.Attributes().Get(sandboxAttribute); named || record.Attributes().Len() != 3 {
		t.Fatal("usage records must not name the sandbox")
	}
}

func TestUsageOfUnclaimedSandboxesIsDropped(t *testing.T) {
	store := &memoryStore{}
	untrusted := &liveExporter{config: &Config{}, store: store}
	if err := untrusted.pushLogs(context.Background(), claim("do-spare-a1", "alpha", "Counter", "42")); err != nil {
		t.Fatal(err)
	}
	store.published = nil

	if err := untrusted.pushMetrics(context.Background(), podUsage("do-spare-a1")); err != nil {
		t.Fatal(err)
	}

	if len(store.actors) != 0 || len(store.published) != 0 {
		t.Fatalf("an untrusted claim must not route usage, got %v and %v", store.actors, store.published)
	}
}

func claim(sandbox, project, actorName, actorID string) plog.Logs {
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetEventName(claimedEvent)
	record.Attributes().PutStr(projectAttribute, project)
	record.Attributes().PutStr(actorNameAttribute, actorName)
	record.Attributes().PutStr(actorIDAttribute, actorID)
	record.Attributes().PutStr(sandboxAttribute, sandbox)
	return logs
}

func podUsage(pods ...string) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	at := pcommon.Timestamp(1e9)
	for _, pod := range pods {
		resource := metrics.ResourceMetrics().AppendEmpty()
		resource.Resource().Attributes().PutStr(podAttribute, pod)
		scope := resource.ScopeMetrics().AppendEmpty().Metrics()
		cpu := scope.AppendEmpty()
		cpu.SetName(cpuMetric)
		point := cpu.SetEmptyGauge().DataPoints().AppendEmpty()
		point.SetTimestamp(at)
		point.SetDoubleValue(0.25)
		memory := scope.AppendEmpty()
		memory.SetName(memoryMetric)
		point = memory.SetEmptyGauge().DataPoints().AppendEmpty()
		point.SetTimestamp(at)
		point.SetIntValue(1 << 20)
	}
	return metrics
}

type memoryStore struct {
	published map[string][]byte
	actors    map[string]string
}

func (s *memoryStore) publish(_ context.Context, messages map[string][]byte) error {
	if s.published == nil {
		s.published = map[string][]byte{}
	}
	for channel, payload := range messages {
		s.published[channel] = payload
	}
	return nil
}

func (s *memoryStore) remember(_ context.Context, actors map[string]string) error {
	if s.actors == nil {
		s.actors = map[string]string{}
	}
	for sandbox, actor := range actors {
		s.actors[sandbox] = actor
	}
	return nil
}

func (s *memoryStore) recall(_ context.Context, sandboxes []string) (map[string]string, error) {
	found := map[string]string{}
	for _, sandbox := range sandboxes {
		if actor, ok := s.actors[sandbox]; ok {
			found[sandbox] = actor
		}
	}
	return found, nil
}

func (s *memoryStore) close() error {
	return nil
}
