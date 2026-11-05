package redisliveexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const typeName = "redislive"

// NewFactory builds an exporter without a queue or retries: live updates are best effort,
// and the ClickHouse exporter remains the durable path.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeName),
		func() component.Config { return &Config{} },
		exporter.WithTraces(createTraces, component.StabilityLevelDevelopment),
		exporter.WithLogs(createLogs, component.StabilityLevelDevelopment),
	)
}

func createTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	live := &liveExporter{config: cfg.(*Config)}
	return exporterhelper.NewTraces(ctx, set, cfg, live.pushTraces,
		exporterhelper.WithStart(live.start), exporterhelper.WithShutdown(live.shutdown))
}

func createLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	live := &liveExporter{config: cfg.(*Config)}
	return exporterhelper.NewLogs(ctx, set, cfg, live.pushLogs,
		exporterhelper.WithStart(live.start), exporterhelper.WithShutdown(live.shutdown))
}
