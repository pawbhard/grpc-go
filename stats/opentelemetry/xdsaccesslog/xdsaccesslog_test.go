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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal/grpctest"
	"google.golang.org/grpc/internal/testutils/otlplogs"
	xdscreds "google.golang.org/grpc/internal/xds/credentials"
	"google.golang.org/grpc/internal/xds/grpcservice"
	"google.golang.org/grpc/internal/xds/httpfilter/accesslog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"

	otlpcommonpb "go.opentelemetry.io/proto/otlp/common/v1"
	otlplogspb "go.opentelemetry.io/proto/otlp/logs/v1"
	otlpresourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

const defaultTestTimeout = 10 * time.Second

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

func collectorConfig(addr string, md metadata.MD) *grpcservice.Config {
	return &grpcservice.Config{
		TargetURI:          "passthrough:///" + addr,
		Timeout:            5 * time.Second,
		InitialMetadata:    md,
		ChannelCredentials: xdscreds.NewChannelCreds(insecure.NewBundle(), xdscreds.Identity{Type: "insecure"}, nil),
	}
}

func receiveRequest(ctx context.Context, t *testing.T, c *otlplogs.Collector) *otlplogs.Request {
	t.Helper()
	select {
	case r := <-c.Requests:
		return r
	case <-ctx.Done():
		t.Fatal("Timeout waiting for an export request")
	}
	return nil
}

func strValue(s string) *otlpcommonpb.AnyValue {
	return &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_StringValue{StringValue: s}}
}

func (s) TestSinkExportsRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	collector := otlplogs.StartCollector(t)

	sink, err := newSink(accesslog.SinkOptions{
		Collector: collectorConfig(collector.Address, metadata.Pairs("x-api-key", "secret")),
		LogName:   "my-log",
		ResourceAttributes: []accesslog.KeyValue{
			{Key: "log_name", Value: accesslog.Value{Kind: accesslog.KindString, String: "my-log"}},
			{Key: "count", Value: accesslog.Value{Kind: accesslog.KindInt64, Int64: 3}},
		},
		FlushInterval:   10 * time.Millisecond,
		BufferSizeBytes: 16384,
	})
	if err != nil {
		t.Fatalf("newSink() failed: %v", err)
	}
	defer sink.Close()

	ts := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	sink.Log(accesslog.Record{
		Timestamp: ts,
		Body: &accesslog.Value{Kind: accesslog.KindMap, Map: []accesslog.KeyValue{
			{Key: "path", Value: accesslog.Value{Kind: accesslog.KindString, String: "/s/m"}},
			{Key: "list", Value: accesslog.Value{Kind: accesslog.KindSlice, Slice: []accesslog.Value{
				{Kind: accesslog.KindBool, Bool: true},
				{Kind: accesslog.KindFloat64, Float64: 1.5},
				{Kind: accesslog.KindBytes, Bytes: []byte{1}},
			}}},
		}},
		Attributes: []accesslog.KeyValue{{Key: "status", Value: accesslog.Value{Kind: accesslog.KindString, String: "OK"}}},
	})

	req := receiveRequest(ctx, t, collector)
	if got := req.Metadata.Get("x-api-key"); len(got) != 1 || got[0] != "secret" {
		t.Errorf("Export metadata x-api-key = %v, want [secret]", got)
	}
	want := &otlplogspb.LogsData{ResourceLogs: []*otlplogspb.ResourceLogs{{
		Resource: &otlpresourcepb.Resource{Attributes: []*otlpcommonpb.KeyValue{
			{Key: "count", Value: &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_IntValue{IntValue: 3}}},
			{Key: "log_name", Value: strValue("my-log")},
		}},
		ScopeLogs: []*otlplogspb.ScopeLogs{{
			Scope: &otlpcommonpb.InstrumentationScope{Name: "my-log", Version: grpc.Version},
			LogRecords: []*otlplogspb.LogRecord{{
				TimeUnixNano: uint64(ts.UnixNano()),
				Body: &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_KvlistValue{KvlistValue: &otlpcommonpb.KeyValueList{Values: []*otlpcommonpb.KeyValue{
					// The SDK sorts map entries by key.
					{Key: "list", Value: &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_ArrayValue{ArrayValue: &otlpcommonpb.ArrayValue{Values: []*otlpcommonpb.AnyValue{
						{Value: &otlpcommonpb.AnyValue_BoolValue{BoolValue: true}},
						{Value: &otlpcommonpb.AnyValue_DoubleValue{DoubleValue: 1.5}},
						{Value: &otlpcommonpb.AnyValue_BytesValue{BytesValue: []byte{1}}},
					}}}}},
					{Key: "path", Value: strValue("/s/m")},
				}}}},
				Attributes: []*otlpcommonpb.KeyValue{{Key: "status", Value: strValue("OK")}},
			}},
		}},
	}}}
	// The SDK sets the observed timestamp.
	ignoreObserved := protocmp.IgnoreFields(&otlplogspb.LogRecord{}, "observed_time_unix_nano")
	if diff := cmp.Diff(want, req.Data, protocmp.Transform(), ignoreObserved); diff != "" {
		t.Errorf("Exported logs mismatch (-want +got):\n%s", diff)
	}
}

func (s) TestSinkCloseFlushes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	collector := otlplogs.StartCollector(t)

	sink, err := newSink(accesslog.SinkOptions{
		Collector:       collectorConfig(collector.Address, nil),
		FlushInterval:   time.Hour,
		BufferSizeBytes: 16384,
	})
	if err != nil {
		t.Fatalf("newSink() failed: %v", err)
	}
	sink.Log(accesslog.Record{Timestamp: time.Now(), Body: &accesslog.Value{Kind: accesslog.KindString, String: "last"}})
	sink.Close()

	req := receiveRequest(ctx, t, collector)
	records := otlplogs.LogRecords(req.Data)
	if len(records) != 1 || records[0].GetBody().GetStringValue() != "last" {
		t.Fatalf("Exported records = %v, want one record with body %q", records, "last")
	}
	if got := req.Data.GetResourceLogs()[0].GetScopeLogs()[0].GetScope().GetName(); got != defaultLoggerName {
		t.Errorf("Scope name = %q, want %q", got, defaultLoggerName)
	}
}

func (s) TestSinkExportFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	collector := otlplogs.StartCollector(t)
	collector.Response = func(*otlplogs.Request) (*emptypb.Empty, error) {
		return nil, status.Error(codes.Unavailable, "collector down")
	}
	grpctest.ExpectWarning("failed to export")

	sink, err := newSink(accesslog.SinkOptions{
		Collector:       collectorConfig(collector.Address, nil),
		FlushInterval:   10 * time.Millisecond,
		BufferSizeBytes: 16384,
	})
	if err != nil {
		t.Fatalf("newSink() failed: %v", err)
	}
	defer sink.Close()
	sink.Log(accesslog.Record{Timestamp: time.Now()})
	receiveRequest(ctx, t, collector)
}

func (s) TestPartialSuccess(t *testing.T) {
	var inner []byte
	inner = protowire.AppendTag(inner, 1, protowire.VarintType)
	inner = protowire.AppendVarint(inner, 4)
	inner = protowire.AppendTag(inner, 2, protowire.BytesType)
	inner = protowire.AppendString(inner, "too big")
	inner = protowire.AppendTag(inner, 3, protowire.VarintType) // Unknown field.
	inner = protowire.AppendVarint(inner, 1)
	var b []byte
	b = protowire.AppendTag(b, 5, protowire.BytesType) // Unknown field.
	b = protowire.AppendBytes(b, []byte("x"))
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, inner)

	if rejected, msg := partialSuccess(b); rejected != 4 || msg != "too big" {
		t.Errorf("partialSuccess() = (%d, %q), want (4, %q)", rejected, msg, "too big")
	}
	if rejected, msg := partialSuccess(nil); rejected != 0 || msg != "" {
		t.Errorf("partialSuccess(nil) = (%d, %q), want (0, \"\")", rejected, msg)
	}
	if rejected, msg := partialSuccess([]byte{0xff}); rejected != 0 || msg != "" {
		t.Errorf("partialSuccess(invalid) = (%d, %q), want (0, \"\")", rejected, msg)
	}
}

func (s) TestExportBatchSize(t *testing.T) {
	tests := []struct {
		bytes uint32
		want  int
	}{
		{bytes: 0, want: 1},
		{bytes: 255, want: 1},
		{bytes: 16384, want: 64},
		{bytes: 1 << 30, want: maxExportBatchSize},
	}
	for _, test := range tests {
		if got := exportBatchSize(test.bytes); got != test.want {
			t.Errorf("exportBatchSize(%d) = %d, want %d", test.bytes, got, test.want)
		}
	}
}
