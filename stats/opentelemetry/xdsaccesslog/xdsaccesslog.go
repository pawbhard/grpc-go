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

// Package xdsaccesslog exports xDS-configured access logs with the
// OpenTelemetry Logs SDK.
//
// gRPC writes one log record per RPC for each OpenTelemetryAccessLogConfig in
// the access_log field of the xDS HttpConnectionManager, on xDS clients and
// xDS-enabled servers. The records are exported with OTLP over gRPC to the
// collector that the config names.
//
// To enable the feature:
//
//  1. Set the environment variable GRPC_EXPERIMENTAL_XDS_OTEL_ACCESS_LOG to
//     "true".
//
//  2. Import this package for its side effects:
//
//     import _ "google.golang.org/grpc/stats/opentelemetry/xdsaccesslog"
//
// If the feature is enabled and this package is not imported, gRPC validates
// the config but drops the access logs.
//
// # Experimental
//
// Notice: This package is EXPERIMENTAL and may be changed or removed in a
// later release.
package xdsaccesslog

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal/xds/httpfilter/accesslog"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

var logger = grpclog.Component("xds")

const (
	// defaultLoggerName is the logger and instrumentation scope name if the
	// config has no log name.
	defaultLoggerName = "grpc.access_log"
	// estimatedRecordSizeBytes converts buffer_size_bytes to a record count,
	// because the SDK limits records, not bytes.
	estimatedRecordSizeBytes = 256
	// maxExportBatchSize is the SDK default maximum queue size. A batch is
	// never larger than the queue.
	maxExportBatchSize = 2048
	// exportTimeout is the upper bound on the time that one export waits for
	// the collector.
	exportTimeout = 30 * time.Second
	// shutdownTimeout is the upper bound on the time that Close waits for the
	// final export.
	shutdownTimeout = 30 * time.Second
)

func init() {
	accesslog.SetSinkBuilder(newSink)
}

// sink owns one LoggerProvider with a BatchProcessor, and the channel to the
// collector.
type sink struct {
	cc       *grpc.ClientConn
	provider *sdklog.LoggerProvider
	logger   otellog.Logger
}

func newSink(opts accesslog.SinkOptions) (accesslog.Sink, error) {
	cc, err := opts.Collector.Dial()
	if err != nil {
		return nil, fmt.Errorf("xdsaccesslog: failed to create a channel to collector %q: %v", opts.Collector.TargetURI, err)
	}
	exp := &exporter{cc: cc, timeout: opts.Collector.Timeout, md: opts.Collector.InitialMetadata}
	processor := sdklog.NewBatchProcessor(exp, batchProcessorOptions(opts)...)
	// NewSchemaless does not add the SDK default attributes
	// (telemetry.sdk.*, service.name), so the resource matches Envoy.
	res := resource.NewSchemaless(toAttributes(opts.ResourceAttributes)...)
	provider := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(processor))
	name := opts.LogName
	if name == "" {
		name = defaultLoggerName
	}
	return &sink{
		cc:       cc,
		provider: provider,
		logger:   provider.Logger(name, otellog.WithInstrumentationVersion(grpc.Version)),
	}, nil
}

// batchProcessorOptions maps the Envoy buffer settings to the SDK batch
// settings.
func batchProcessorOptions(opts accesslog.SinkOptions) []sdklog.BatchProcessorOption {
	return []sdklog.BatchProcessorOption{
		sdklog.WithExportInterval(max(time.Millisecond, opts.FlushInterval)),
		sdklog.WithExportMaxBatchSize(exportBatchSize(opts.BufferSizeBytes)),
		sdklog.WithExportTimeout(exportTimeout),
	}
}

// exportBatchSize converts buffer_size_bytes to a record count in
// [1, maxExportBatchSize].
func exportBatchSize(bufferSizeBytes uint32) int {
	return int(max(1, min(bufferSizeBytes/estimatedRecordSizeBytes, maxExportBatchSize)))
}

// Log enqueues the record. The BatchProcessor does not block.
func (s *sink) Log(r accesslog.Record) {
	var rec otellog.Record
	rec.SetTimestamp(r.Timestamp)
	if r.Body != nil {
		rec.SetBody(toAttributeValue(*r.Body))
	}
	if len(r.Attributes) > 0 {
		rec.AddAttributes(toAttributes(r.Attributes)...)
	}
	s.logger.Emit(context.Background(), rec)
}

// Close exports the buffered records and closes the channel. It blocks for at
// most shutdownTimeout plus the time to close the channel.
func (s *sink) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := s.provider.Shutdown(ctx); err != nil {
		logger.Warningf("xdsaccesslog: failed to flush access logs on shutdown: %v", err)
	}
	s.cc.Close()
}

func toAttributes(kvs []accesslog.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, len(kvs))
	for i, kv := range kvs {
		out[i] = attribute.KeyValue{Key: attribute.Key(kv.Key), Value: toAttributeValue(kv.Value)}
	}
	return out
}

func toAttributeValue(v accesslog.Value) attribute.Value {
	switch v.Kind {
	case accesslog.KindString:
		return attribute.StringValue(v.String)
	case accesslog.KindInt64:
		return attribute.Int64Value(v.Int64)
	case accesslog.KindBool:
		return attribute.BoolValue(v.Bool)
	case accesslog.KindFloat64:
		return attribute.Float64Value(v.Float64)
	case accesslog.KindBytes:
		return attribute.ByteSliceValue(v.Bytes)
	case accesslog.KindSlice:
		vs := make([]attribute.Value, len(v.Slice))
		for i, e := range v.Slice {
			vs[i] = toAttributeValue(e)
		}
		return attribute.SliceValue(vs...)
	case accesslog.KindMap:
		return attribute.MapValue(toAttributes(v.Map)...)
	}
	return attribute.Value{}
}
