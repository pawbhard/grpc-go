/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package xdsaccesslog

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	otlpcommonpb "go.opentelemetry.io/proto/otlp/common/v1"
	otlplogspb "go.opentelemetry.io/proto/otlp/logs/v1"
	otlpresourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// exportMethod is the OTLP logs export method.
//
// The request is sent as a LogsData message. LogsData has the same wire format
// as ExportLogsServiceRequest (field 1: repeated ResourceLogs). This avoids a
// dependency on the collector package, which also contains grpc-gateway code.
const exportMethod = "/opentelemetry.proto.collector.logs.v1.LogsService/Export"

// errorLogInterval is the minimum time between two export error logs.
const errorLogInterval = 10 * time.Second

var errShutdown = errors.New("xdsaccesslog: exporter is shut down")

// exporter is an sdklog.Exporter that sends OTLP logs to a collector on a
// gRPC channel. The channel applies the xDS channel and call credentials.
type exporter struct {
	cc      *grpc.ClientConn
	timeout time.Duration
	md      metadata.MD

	stopped      atomic.Bool
	lastErrorLog atomic.Int64 // Unix nanoseconds.
}

// Export sends records synchronously on the BatchProcessor goroutine. The
// returned error tells the SDK that the export failed.
func (e *exporter) Export(ctx context.Context, records []sdklog.Record) error {
	if e.stopped.Load() {
		return errShutdown
	}
	if len(records) == 0 {
		return nil
	}
	if e.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.timeout)
		defer cancel()
	}
	if len(e.md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, e.md)
	}
	resp := &emptypb.Empty{}
	if err := e.cc.Invoke(ctx, exportMethod, toLogsData(records), resp); err != nil {
		e.logError("xdsaccesslog: failed to export %d access log records to %q: %v", len(records), e.cc.CanonicalTarget(), err)
		return err
	}
	// The response fields are kept as unknown fields of the Empty message.
	if rejected, msg := partialSuccess(resp.ProtoReflect().GetUnknown()); rejected > 0 || msg != "" {
		e.logError("xdsaccesslog: collector %q rejected %d access log records: %s", e.cc.CanonicalTarget(), rejected, msg)
	}
	return nil
}

func (e *exporter) logError(format string, args ...any) {
	now := time.Now().UnixNano()
	last := e.lastErrorLog.Load()
	if now-last < int64(errorLogInterval) || !e.lastErrorLog.CompareAndSwap(last, now) {
		return
	}
	logger.Warningf(format, args...)
}

// Shutdown makes later exports fail. The sink closes the channel.
func (e *exporter) Shutdown(context.Context) error {
	e.stopped.Store(true)
	return nil
}

// ForceFlush does nothing: Export is synchronous.
func (e *exporter) ForceFlush(context.Context) error {
	return nil
}

// partialSuccess parses the partial_success field (1) of an
// ExportLogsServiceResponse: rejected_log_records (1) and error_message (2).
func partialSuccess(b []byte) (rejected int64, msg string) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, ""
		}
		b = b[n:]
		if num != 1 || typ != protowire.BytesType {
			if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return 0, ""
			}
			b = b[n:]
			continue
		}
		ps, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return 0, ""
		}
		b = b[n:]
		for len(ps) > 0 {
			num, typ, n := protowire.ConsumeTag(ps)
			if n < 0 {
				return 0, ""
			}
			ps = ps[n:]
			switch {
			case num == 1 && typ == protowire.VarintType:
				v, n := protowire.ConsumeVarint(ps)
				if n < 0 {
					return 0, ""
				}
				rejected = int64(v)
				ps = ps[n:]
			case num == 2 && typ == protowire.BytesType:
				v, n := protowire.ConsumeBytes(ps)
				if n < 0 {
					return 0, ""
				}
				msg = string(v)
				ps = ps[n:]
			default:
				if n = protowire.ConsumeFieldValue(num, typ, ps); n < 0 {
					return 0, ""
				}
				ps = ps[n:]
			}
		}
	}
	return rejected, msg
}

// toLogsData converts records to OTLP, grouped by resource and then by
// instrumentation scope, in the order of first appearance.
func toLogsData(records []sdklog.Record) *otlplogspb.LogsData {
	type scopeKey struct {
		name, version, schemaURL string
		attrs                    attribute.Distinct
	}
	data := &otlplogspb.LogsData{}
	resourceIndex := make(map[*resource.Resource]int)
	scopeIndex := make(map[*resource.Resource]map[scopeKey]int)
	for i := range records {
		r := &records[i]
		res := r.Resource()
		ri, ok := resourceIndex[res]
		if !ok {
			ri = len(data.ResourceLogs)
			resourceIndex[res] = ri
			scopeIndex[res] = make(map[scopeKey]int)
			rl := &otlplogspb.ResourceLogs{Resource: &otlpresourcepb.Resource{}}
			if res != nil {
				rl.Resource.Attributes = toKeyValues(res.Attributes())
				rl.SchemaUrl = res.SchemaURL()
			}
			data.ResourceLogs = append(data.ResourceLogs, rl)
		}
		rl := data.ResourceLogs[ri]
		scope := r.InstrumentationScope()
		key := scopeKey{name: scope.Name, version: scope.Version, schemaURL: scope.SchemaURL, attrs: scope.Attributes.Equivalent()}
		si, ok := scopeIndex[res][key]
		if !ok {
			si = len(rl.ScopeLogs)
			scopeIndex[res][key] = si
			rl.ScopeLogs = append(rl.ScopeLogs, &otlplogspb.ScopeLogs{
				Scope: &otlpcommonpb.InstrumentationScope{
					Name:       scope.Name,
					Version:    scope.Version,
					Attributes: toKeyValues(scope.Attributes.ToSlice()),
				},
				SchemaUrl: scope.SchemaURL,
			})
		}
		rl.ScopeLogs[si].LogRecords = append(rl.ScopeLogs[si].LogRecords, toLogRecord(r))
	}
	return data
}

func toLogRecord(r *sdklog.Record) *otlplogspb.LogRecord {
	lr := &otlplogspb.LogRecord{
		TimeUnixNano:           unixNano(r.Timestamp()),
		ObservedTimeUnixNano:   unixNano(r.ObservedTimestamp()),
		SeverityNumber:         otlplogspb.SeverityNumber(r.Severity()),
		SeverityText:           r.SeverityText(),
		DroppedAttributesCount: uint32(r.DroppedAttributes()),
		Flags:                  uint32(r.TraceFlags()),
		EventName:              r.EventName(),
	}
	if body := r.Body(); body.Type() != attribute.EMPTY {
		lr.Body = toAnyValue(body)
	}
	if n := r.AttributesLen(); n > 0 {
		lr.Attributes = make([]*otlpcommonpb.KeyValue, 0, n)
		r.WalkAttributes(func(kv attribute.KeyValue) bool {
			lr.Attributes = append(lr.Attributes, &otlpcommonpb.KeyValue{Key: string(kv.Key), Value: toAnyValue(kv.Value)})
			return true
		})
	}
	if id := r.TraceID(); id.IsValid() {
		lr.TraceId = id[:]
	}
	if id := r.SpanID(); id.IsValid() {
		lr.SpanId = id[:]
	}
	return lr
}

func unixNano(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}

func toKeyValues(kvs []attribute.KeyValue) []*otlpcommonpb.KeyValue {
	if len(kvs) == 0 {
		return nil
	}
	out := make([]*otlpcommonpb.KeyValue, len(kvs))
	for i, kv := range kvs {
		out[i] = &otlpcommonpb.KeyValue{Key: string(kv.Key), Value: toAnyValue(kv.Value)}
	}
	return out
}

func toAnyValue(v attribute.Value) *otlpcommonpb.AnyValue {
	av := &otlpcommonpb.AnyValue{}
	switch v.Type() {
	case attribute.BOOL:
		av.Value = &otlpcommonpb.AnyValue_BoolValue{BoolValue: v.AsBool()}
	case attribute.INT64:
		av.Value = &otlpcommonpb.AnyValue_IntValue{IntValue: v.AsInt64()}
	case attribute.FLOAT64:
		av.Value = &otlpcommonpb.AnyValue_DoubleValue{DoubleValue: v.AsFloat64()}
	case attribute.STRING:
		av.Value = &otlpcommonpb.AnyValue_StringValue{StringValue: v.AsString()}
	case attribute.BYTESLICE:
		av.Value = &otlpcommonpb.AnyValue_BytesValue{BytesValue: v.AsByteSlice()}
	case attribute.BOOLSLICE:
		vs := v.AsBoolSlice()
		arr := make([]*otlpcommonpb.AnyValue, len(vs))
		for i, e := range vs {
			arr[i] = toAnyValue(attribute.BoolValue(e))
		}
		av.Value = &otlpcommonpb.AnyValue_ArrayValue{ArrayValue: &otlpcommonpb.ArrayValue{Values: arr}}
	case attribute.INT64SLICE:
		vs := v.AsInt64Slice()
		arr := make([]*otlpcommonpb.AnyValue, len(vs))
		for i, e := range vs {
			arr[i] = toAnyValue(attribute.Int64Value(e))
		}
		av.Value = &otlpcommonpb.AnyValue_ArrayValue{ArrayValue: &otlpcommonpb.ArrayValue{Values: arr}}
	case attribute.FLOAT64SLICE:
		vs := v.AsFloat64Slice()
		arr := make([]*otlpcommonpb.AnyValue, len(vs))
		for i, e := range vs {
			arr[i] = toAnyValue(attribute.Float64Value(e))
		}
		av.Value = &otlpcommonpb.AnyValue_ArrayValue{ArrayValue: &otlpcommonpb.ArrayValue{Values: arr}}
	case attribute.STRINGSLICE:
		vs := v.AsStringSlice()
		arr := make([]*otlpcommonpb.AnyValue, len(vs))
		for i, e := range vs {
			arr[i] = toAnyValue(attribute.StringValue(e))
		}
		av.Value = &otlpcommonpb.AnyValue_ArrayValue{ArrayValue: &otlpcommonpb.ArrayValue{Values: arr}}
	case attribute.SLICE:
		vs := v.AsSlice()
		arr := make([]*otlpcommonpb.AnyValue, len(vs))
		for i, e := range vs {
			arr[i] = toAnyValue(e)
		}
		av.Value = &otlpcommonpb.AnyValue_ArrayValue{ArrayValue: &otlpcommonpb.ArrayValue{Values: arr}}
	case attribute.MAP:
		av.Value = &otlpcommonpb.AnyValue_KvlistValue{KvlistValue: &otlpcommonpb.KeyValueList{Values: toKeyValues(v.AsMap())}}
	}
	return av
}
