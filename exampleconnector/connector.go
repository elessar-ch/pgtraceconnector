package exampleconnector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.uber.org/zap"
)

type traceParent struct {
	version            [1]byte
	traceID            [16]byte
	parentSpanID       [8]byte
	flags              [1]byte
	versionString      string
	traceIDString      string
	parentSpanIDString string
	flagsString        string
}

// schema for connector
type connectorImp struct {
	config          Config
	metricsConsumer consumer.Metrics
	tracesConsumer  consumer.Traces
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

// ConsumeLogs method is called for each instance of a log sent to the connector
func (c *connectorImp) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	traces := ptrace.NewTraces()

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLog := ld.ResourceLogs().At(i)
		resourceSpan := traces.ResourceSpans().AppendEmpty()
		dbResource := resourceSpan.Resource()
		dbAttrs := dbResource.Attributes()

		dbAttrs.PutStr(string(semconv.DBSystemKey), semconv.DBSystemPostgreSQL.Value.AsString())
		dbAttrs.PutStr(string(semconv.DBNameKey), "knexdb")
		dbAttrs.PutStr(string(semconv.ServiceNameKey), "knexdb")

		for j := 0; j < resourceLog.ScopeLogs().Len(); j++ {
			scopeLog := resourceLog.ScopeLogs().At(j)

			scopeSpans := resourceSpan.ScopeSpans().AppendEmpty()
			scopeSpans.Scope().SetName("dbquery")
			scopeSpans.Scope().SetVersion("0.0.1")

			for k := 0; k < scopeLog.LogRecords().Len(); k++ {
				logRecord := scopeLog.LogRecords().At(k)
				attrs := logRecord.Attributes()
				mapping := attrs.AsRaw()

				m, containsMessage := mapping["message"]
				if !containsMessage {
					c.logger.Warn("Log does not contain a message attribute")
					continue
				}
				message, ok := m.(string)
				if !ok {
					c.logger.Warn("Message is not a string")
					continue
				}

				// messages containing a plan start with the following string followed by a json object as a string that contains the plan
				regex := regexp.MustCompile(`^duration: \d+\.\d+ ms  plan:`)
				if !regex.MatchString(message) {
					c.logger.Info("Message does not contain a query plan", zap.String("message", message))
					continue
				}

				// extract duration
				duration := regexp.MustCompile(`\d+\.\d+`).FindString(message)
				startTime := time.Now()

				parsedDuration, _ := time.ParseDuration(duration + "ms")
				endTime := startTime.Add(parsedDuration)

				traceParentRe := regexp.MustCompile(`(?m)traceparent='([\da-f]{2})-([\da-f]{32})-([\da-f]{16})-([\da-f]{2})'`)

				traceParentResult := traceParentRe.FindAllStringSubmatch(message, -1)
				traceParent := traceParent{
					versionString:      traceParentResult[0][1],
					traceIDString:      traceParentResult[0][2],
					parentSpanIDString: traceParentResult[0][3],
					flagsString:        traceParentResult[0][4],
				}
				hex.Decode(traceParent.traceID[:], []byte(traceParent.traceIDString))
				hex.Decode(traceParent.parentSpanID[:], []byte(traceParent.parentSpanIDString))
				hex.Decode(traceParent.flags[:], []byte(traceParent.flagsString))
				hex.Decode(traceParent.version[:], []byte(traceParent.versionString))


				// create a brand new trace with a new trace id

				span := scopeSpans.Spans().AppendEmpty()

				var sid [8]byte
				rand.Read(sid[:])
				span.SetSpanID(pcommon.SpanID(sid))

				span.SetParentSpanID(pcommon.SpanID(traceParent.parentSpanID))

				span.SetStartTimestamp(pcommon.NewTimestampFromTime(startTime))
				span.SetEndTimestamp(pcommon.NewTimestampFromTime(endTime))
				span.SetName("dbquery")
				span.SetKind(ptrace.SpanKindClient)

				span.SetTraceID(traceParent.traceID)

				return c.tracesConsumer.ConsumeTraces(ctx, traces)
			}
		}
	}
	return nil
}
