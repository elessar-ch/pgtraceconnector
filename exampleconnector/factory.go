package exampleconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const (
	defaultVal = "request.n"
	// this is the name used to refer to the connector in the config.yaml
	typeStr = "example"
)

func createDefaultConfig() component.Config {
	return &Config{
		AttributeName: defaultVal,
	}
}

// createTracesToMetricsConnector defines the consumer type of the connector
// We want to consume traces and export metrics, therefore, define nextConsumer as metrics, since consumer is the next component in the pipeline
func createTracesToMetricsConnector(ctx context.Context, params connector.CreateSettings, cfg component.Config, nextConsumer consumer.Metrics) (connector.Traces, error) {
	c, err := newConnector(params.Logger, cfg)
	if err != nil {
		return nil, err
	}
	c.metricsConsumer = nextConsumer
	return c, nil
}

func createLogsToTracesConnector(ctx context.Context, params connector.CreateSettings, cfg component.Config, nextConsumer consumer.Traces) (connector.Logs, error) {
	c, err := newConnector(params.Logger, cfg)
	if err != nil {
		return nil, err
	}
	c.tracesConsumer = nextConsumer
	return c, nil
}

// NewFactory creates a factory for example connector.
func NewFactory() connector.Factory {
	// OpenTelemetry connector factory to make a factory for connectors
	return connector.NewFactory(
		typeStr,
		createDefaultConfig,
		connector.WithTracesToMetrics(createTracesToMetricsConnector, component.StabilityLevelAlpha),
		connector.WithLogsToTraces(createLogsToTracesConnector, component.StabilityLevelAlpha))
}
