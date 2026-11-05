package redisliveexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func TestTelemetryIsSplitByStampedProject(t *testing.T) {
	traces := ptrace.NewTraces()
	logs := plog.NewLogs()
	for _, project := range []string{"alpha", "beta", "alpha", ""} {
		spans := traces.ResourceSpans().AppendEmpty()
		records := logs.ResourceLogs().AppendEmpty()
		if project != "" {
			spans.Resource().Attributes().PutStr(projectAttribute, project)
			records.Resource().Attributes().PutStr(projectAttribute, project)
		}
		spans.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName(project)
		records.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(project)
	}

	spanPayloads, err := tracesByProject(traces)
	if err != nil {
		t.Fatal(err)
	}
	spanRequest := ptraceotlp.NewExportRequest()
	if err := spanRequest.UnmarshalProto(spanPayloads["alpha"]); err != nil {
		t.Fatal(err)
	}
	if len(spanPayloads) != 2 || spanRequest.Traces().SpanCount() != 2 {
		t.Fatalf("expected alpha and beta with both alpha spans, got %d projects", len(spanPayloads))
	}

	logPayloads, err := logsByProject(logs)
	if err != nil {
		t.Fatal(err)
	}
	logRequest := plogotlp.NewExportRequest()
	if err := logRequest.UnmarshalProto(logPayloads["beta"]); err != nil {
		t.Fatal(err)
	}
	if len(logPayloads) != 2 || logRequest.Logs().LogRecordCount() != 1 {
		t.Fatalf("expected alpha and beta with one beta line, got %d projects", len(logPayloads))
	}
}
