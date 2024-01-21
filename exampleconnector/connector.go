package exampleconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// schema for connector
type connectorImp struct {
	config          Config
	metricsConsumer consumer.Metrics
	logger          *zap.Logger
	// Include these parameters if a specific implementation for the Start and Shutdown function are not needed
	component.StartFunc
	component.ShutdownFunc
}

// newConnector is a function to create a new connector
func newConnector(logger *zap.Logger, config component.Config) (*connectorImp, error) {
	logger.Info("Building exampleconnector connector")
	cfg := config.(*Config)

	return &connectorImp{
		config: *cfg,
		logger: logger,
	}, nil
}

// Capabilities implements the consumer interface.
func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeTraces method is called for each instance of a trace sent to the connector
func (c *connectorImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// loop through the levels of spans of the one trace consumed
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpan := td.ResourceSpans().At(i)

		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpan := resourceSpan.ScopeSpans().At(j)

			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				attrs := span.Attributes()
				mapping := attrs.AsRaw()
				for key := range mapping {
					if key == c.config.AttributeName {
						// create metric only if span of trace had the specific attribute
						metrics := pmetric.NewMetrics()
						return c.metricsConsumer.ConsumeMetrics(ctx, metrics)
					}
				}
			}
		}
	}
	return nil
}
