package exampleconnector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
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
	config         Config
	tracesConsumer consumer.Traces
	logger         *zap.Logger
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

				// span := scopeSpans.Spans().AppendEmpty()
				spanSlice := parseQueryPlan(message, startTime, endTime, traceParent)
				spanSlice.MoveAndAppendTo(scopeSpans.Spans())

				return c.tracesConsumer.ConsumeTraces(ctx, traces)
			}
		}
	}
	return nil
}

// Method to parse the query plan from JSON and create trace spans for each step
func parseQueryPlan(message string, startTime time.Time, endTime time.Time, tp traceParent) ptrace.SpanSlice {
	slice := ptrace.NewSpanSlice()
	span := slice.AppendEmpty()

	span.SetTraceID(tp.traceID)
	span.SetParentSpanID(pcommon.SpanID(tp.parentSpanID))

	var sid [8]byte
	rand.Read(sid[:])
	span.SetSpanID(pcommon.SpanID(sid))

	span.SetStartTimestamp(pcommon.NewTimestampFromTime(startTime))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(endTime))
	span.SetName("Query Plan")
	span.SetKind(ptrace.SpanKindClient)

	// extract the part that comes after the regex match
	// this is the query plan
	queryPlanJson := regexp.MustCompile(`(?ms){.*}`).FindStringSubmatch(message)[0]
	// regexp.MustCompile(`(?ms){.*}`)
	// parse json
	// queryPlanJson is a string, so we need to convert it to a byte array
	// then we can unmarshal it into a map
	queryPlanJsonBytes := []byte(queryPlanJson)
	var queryPlanMap map[string]interface{}
	json.Unmarshal(queryPlanJsonBytes, &queryPlanMap)

	newTraceParent := traceParent{
		version:      tp.version,
		traceID:      tp.traceID,
		parentSpanID: sid,
		flags:        tp.flags,
	}

	queryPlan, hasPlan := queryPlanMap["Plan"]
	if !hasPlan {
		return slice
	}

	processPlanStep(queryPlan.(map[string]interface{}), startTime, endTime, newTraceParent).MoveAndAppendTo(slice)

	return slice
}

func processPlanStep(planStep map[string]interface{}, startTime time.Time, endTime time.Time, tp traceParent) ptrace.SpanSlice {
	slice := ptrace.NewSpanSlice()
	span := slice.AppendEmpty()

	span.SetTraceID(tp.traceID)
	span.SetParentSpanID(pcommon.SpanID(tp.parentSpanID))

	var sid [8]byte
	rand.Read(sid[:])
	span.SetSpanID(pcommon.SpanID(sid))

	span.SetStartTimestamp(pcommon.NewTimestampFromTime(startTime))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(endTime))
	span.SetName("Plan Step")
	span.SetKind(ptrace.SpanKindClient)

	extractPlanAttributes(planStep, span, tp, sid, startTime, endTime, slice)

	if _, hasPlans := planStep["Plans"]; hasPlans {
		plans := planStep["Plans"].([]interface{})

		newTraceParent := traceParent{
			version:      tp.version,
			traceID:      tp.traceID,
			parentSpanID: sid,
			flags:        tp.flags,
		}

		for _, plan := range plans {
			processPlanStep(plan.(map[string]interface{}), startTime, endTime, newTraceParent).MoveAndAppendTo(slice)
		}
	}

	return slice
}

func extractPlanAttributes(planStep map[string]interface{}, span ptrace.Span, tp traceParent, sid [8]byte, startTime time.Time, endTime time.Time, slice ptrace.SpanSlice) {
	for key, value := range planStep {
		switch key {
		case "Node Type":
			span.Attributes().PutStr("node_type", value.(string))
			span.SetName(value.(string))
		case "Relation Name":
			span.Attributes().PutStr("relation_name", value.(string))
		case "Alias":
			span.Attributes().PutStr("alias", value.(string))
		case "Startup Cost":
			span.Attributes().PutDouble("startup_cost", value.(float64))
		case "Total Cost":
			span.Attributes().PutDouble("total_cost", value.(float64))
		case "Plan Rows":
			span.Attributes().PutInt("plan_rows", int64(value.(float64)))
		case "Plan Width":
			span.Attributes().PutInt("plan_width", int64(value.(float64)))
		case "Actual Startup Time":
			span.Attributes().PutDouble("actual_startup_time", value.(float64))
		case "Actual Total Time":
			span.Attributes().PutDouble("actual_total_time", value.(float64))
		case "Actual Rows":
			span.Attributes().PutInt("actual_rows", int64(value.(float64)))
		case "Actual Loops":
			span.Attributes().PutInt("actual_loops", int64(value.(float64)))
		case "Output":
			span.Attributes().PutStr("output", value.(string))
		case "Filter":
			span.Attributes().PutStr("filter", value.(string))
		case "Recheck Cond":
			span.Attributes().PutStr("recheck_cond", value.(string))
		case "Rows Removed by Filter":
			span.Attributes().PutInt("rows_removed_by_filter", int64(value.(float64)))
		case "Inner Unique":
			span.Attributes().PutBool("inner_unique", value.(bool))
		case "Index Name":
			span.Attributes().PutStr("index_name", value.(string))
		case "Index Cond":
			span.Attributes().PutStr("index_cond", value.(string))
		case "Join Type":
			span.Attributes().PutStr("join_type", value.(string))
		case "Hash Cond":
			span.Attributes().PutStr("hash_cond", value.(string))
		case "Hash Buckets":
			span.Attributes().PutInt("hash_buckets", int64(value.(float64)))
		case "Hash Batches":
			span.Attributes().PutInt("hash_batches", int64(value.(float64)))
		case "Group Key":
			span.Attributes().PutStr("group_key", value.(string))
		case "Plans":
			break
		default:
			switch value.(type) {
			case int:
				span.Attributes().PutInt(key, int64(value.(int)))
			case float64:
				span.Attributes().PutDouble(key, value.(float64))
			case bool:
				span.Attributes().PutBool(key, value.(bool))
			case string:
				span.Attributes().PutStr(key, value.(string))
			}
		}
	}
}
