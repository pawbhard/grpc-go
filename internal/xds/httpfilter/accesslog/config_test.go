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

package accesslog

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/internal/xds/bootstrap"
	"google.golang.org/grpc/internal/xds/httpfilter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v3xdsxdstypepb "github.com/cncf/xds/go/xds/type/v3"
	v3accesslogpb "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	v3grpcaccesslogpb "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/grpc/v3"
	v3otelaccesslogpb "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/open_telemetry/v3"
	v3streampb "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/stream/v3"
	otlpcommonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

const (
	collectorTarget      = "dns:///collector.example.com:4317"
	insecureCredsTypeURL = "type.googleapis.com/envoy.extensions.grpc_service.channel_credentials.insecure.v3.InsecureCredentials"
)

// testParseOptions returns parse options with a bootstrap node and a trusted
// management server. It also enables the feature.
func testParseOptions(t *testing.T, trusted bool) httpfilter.ParseOptions {
	t.Helper()
	testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, true)
	contents, err := bootstrap.NewContentsForTesting(bootstrap.ConfigOptionsForTesting{
		Servers:             json.RawMessage(`[{"server_uri":"td.googleapis.com:443","channel_creds":[{"type":"insecure"}]}]`),
		Node:                json.RawMessage(`{"id":"node-id","cluster":"node-cluster","locality":{"zone":"node-zone"}}`),
		AllowedGRPCServices: json.RawMessage(`{"dns:///allowed.example.com:4317":{"channel_creds":[{"type":"insecure"}]}}`),
	})
	if err != nil {
		t.Fatalf("NewContentsForTesting() failed: %v", err)
	}
	bc, err := bootstrap.NewConfigFromContents(contents)
	if err != nil {
		t.Fatalf("NewConfigFromContents() failed: %v", err)
	}
	opts := bootstrap.ServerConfigTestingOptions{URI: "td.googleapis.com:443"}
	if trusted {
		opts.ServerFeatures = []string{"trusted_xds_server"}
	}
	sc, err := bootstrap.ServerConfigForTesting(opts)
	if err != nil {
		t.Fatalf("ServerConfigForTesting() failed: %v", err)
	}
	return httpfilter.ParseOptions{BootstrapConfig: bc, ServerConfig: sc}
}

func grpcService(target string) *v3corepb.GrpcService {
	return &v3corepb.GrpcService{
		TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{
			GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
				TargetUri:                target,
				ChannelCredentialsPlugin: []*anypb.Any{{TypeUrl: insecureCredsTypeURL}},
			},
		},
	}
}

func stringValue(s string) *otlpcommonpb.AnyValue {
	return &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_StringValue{StringValue: s}}
}

func intValue(i int64) *otlpcommonpb.AnyValue {
	return &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_IntValue{IntValue: i}}
}

func otelAccessLog(t *testing.T, c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig) *v3accesslogpb.AccessLog {
	return &v3accesslogpb.AccessLog{
		Name:       "envoy.access_loggers.open_telemetry",
		ConfigType: &v3accesslogpb.AccessLog_TypedConfig{TypedConfig: testutils.MarshalAny(t, c)},
	}
}

func closeConfig(c *Config) {
	if c == nil {
		return
	}
	for _, l := range c.Loggers {
		l.Collector.Close()
	}
}

func (s) TestParseAccessLogs_Valid(t *testing.T) {
	opts := testParseOptions(t, true)
	logs := []*v3accesslogpb.AccessLog{
		// Ignored: another logger type.
		{
			Name:       "stdout",
			ConfigType: &v3accesslogpb.AccessLog_TypedConfig{TypedConfig: testutils.MarshalAny(t, &v3streampb.StdoutAccessLog{})},
		},
		// Ignored: has an AccessLogFilter.
		{
			Name:       "filtered",
			Filter:     &v3accesslogpb.AccessLogFilter{},
			ConfigType: &v3accesslogpb.AccessLog_TypedConfig{TypedConfig: testutils.MarshalAny(t, &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{GrpcService: grpcService(collectorTarget)})},
		},
		// Ignored: only http_service.
		otelAccessLog(t, &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{HttpService: &v3corepb.HttpService{}}),
		// Ignored: no typed_config.
		{Name: "empty"},
		// Used.
		otelAccessLog(t, &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{
			GrpcService:         grpcService(collectorTarget),
			LogName:             "my-log",
			BufferFlushInterval: durationpb.New(5 * time.Second),
			BufferSizeBytes:     wrapperspb.UInt32(1024),
			Body:                stringValue("%REQ(:PATH)% %GRPC_STATUS%"),
			Attributes: &otlpcommonpb.KeyValueList{Values: []*otlpcommonpb.KeyValue{
				{Key: "status", Value: stringValue("%GRPC_STATUS_NUMBER%")},
				{Key: "static", Value: intValue(42)},
				{Key: "nested", Value: &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_KvlistValue{KvlistValue: &otlpcommonpb.KeyValueList{
					Values: []*otlpcommonpb.KeyValue{{Key: "bytes", Value: stringValue("%BYTES_SENT%")}},
				}}}},
				{Key: "list", Value: &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_ArrayValue{ArrayValue: &otlpcommonpb.ArrayValue{
					Values: []*otlpcommonpb.AnyValue{stringValue("%RESP(x-r)%"), intValue(1)},
				}}}},
			}},
			ResourceAttributes: &otlpcommonpb.KeyValueList{Values: []*otlpcommonpb.KeyValue{
				{Key: "zone_name", Value: stringValue("override-zone")},
				{Key: "service", Value: stringValue("svc")},
			}},
		}),
		// Used: deprecated common_config, builtin labels disabled.
		otelAccessLog(t, &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{
			CommonConfig: &v3grpcaccesslogpb.CommonGrpcAccessLogConfig{
				LogName:     "common-log",
				GrpcService: grpcService(collectorTarget),
			},
			DisableBuiltinLabels: true,
		}),
	}
	cfg, err := ParseAccessLogs(logs, opts)
	if err != nil {
		t.Fatalf("ParseAccessLogs() failed: %v", err)
	}
	defer closeConfig(cfg)
	if got := len(cfg.Loggers); got != 2 {
		t.Fatalf("ParseAccessLogs() returned %d loggers, want 2: %v", got, cfg)
	}

	l := cfg.Loggers[0]
	if l.Collector.TargetURI != collectorTarget || l.LogName != "my-log" || l.FlushInterval != 5*time.Second || l.BufferSizeBytes != 1024 {
		t.Errorf("Logger 0 = %v, want collector %q, log name %q, flush 5s, buffer 1024", l, collectorTarget, "my-log")
	}
	wantResource := []KeyValue{
		{Key: "log_name", Value: Value{Kind: KindString, String: "my-log"}},
		{Key: "zone_name", Value: Value{Kind: KindString, String: "override-zone"}},
		{Key: "cluster_name", Value: Value{Kind: KindString, String: "node-cluster"}},
		{Key: "node_name", Value: Value{Kind: KindString, String: "node-id"}},
		{Key: "service", Value: Value{Kind: KindString, String: "svc"}},
	}
	if diff := cmp.Diff(wantResource, l.ResourceAttributes); diff != "" {
		t.Errorf("Logger 0 resource attributes mismatch (-want +got):\n%s", diff)
	}
	data := &CallData{
		RequestHeaders:  map[string]string{":path": "/s/m"},
		ResponseHeaders: map[string]string{"x-r": "r"},
		StatusCode:      codes.NotFound,
		HasStatus:       true,
		BytesSent:       7,
	}
	if got, want := l.body.evaluate(data), (Value{Kind: KindString, String: "/s/m NotFound"}); !cmp.Equal(got, want) {
		t.Errorf("Logger 0 body = %+v, want %+v", got, want)
	}
	wantAttrs := []KeyValue{
		{Key: "status", Value: Value{Kind: KindString, String: "5"}},
		{Key: "static", Value: Value{Kind: KindInt64, Int64: 42}},
		{Key: "nested", Value: Value{Kind: KindMap, Map: []KeyValue{{Key: "bytes", Value: Value{Kind: KindString, String: "7"}}}}},
		{Key: "list", Value: Value{Kind: KindSlice, Slice: []Value{{Kind: KindString, String: "r"}, {Kind: KindInt64, Int64: 1}}}},
	}
	if diff := cmp.Diff(wantAttrs, evaluateKeyValues(l.attributes, data)); diff != "" {
		t.Errorf("Logger 0 attributes mismatch (-want +got):\n%s", diff)
	}

	l = cfg.Loggers[1]
	if l.LogName != "common-log" || l.FlushInterval != defaultFlushInterval || l.BufferSizeBytes != defaultBufferSizeBytes || l.body != nil || len(l.ResourceAttributes) != 0 {
		t.Errorf("Logger 1 = %v, want log name %q, default buffer settings, no body, no resource attributes", l, "common-log")
	}

	// Usage covers all templates.
	if !cfg.usage.bytes {
		t.Error("Config usage.bytes = false, want true")
	}
	if !cfg.usage.headers.request[":path"] || !cfg.usage.headers.response["x-r"] {
		t.Errorf("Config usage headers = %+v, want :path and x-r", cfg.usage.headers)
	}

	// The two loggers have different sink settings.
	if cfg.Loggers[0].sinkKey() == cfg.Loggers[1].sinkKey() {
		t.Errorf("Loggers have the same sink key %q", cfg.Loggers[0].sinkKey())
	}
}

func (s) TestParseAccessLogs_NoLoggers(t *testing.T) {
	stdout := &v3accesslogpb.AccessLog{
		ConfigType: &v3accesslogpb.AccessLog_TypedConfig{TypedConfig: testutils.MarshalAny(t, &v3streampb.StdoutAccessLog{})},
	}
	otel := otelAccessLog(t, &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{GrpcService: grpcService(collectorTarget)})

	t.Run("feature_disabled", func(t *testing.T) {
		opts := testParseOptions(t, true)
		testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, false)
		if cfg, err := ParseAccessLogs([]*v3accesslogpb.AccessLog{otel}, opts); cfg != nil || err != nil {
			t.Fatalf("ParseAccessLogs() = (%v, %v), want (nil, nil)", cfg, err)
		}
	})
	t.Run("only_other_loggers", func(t *testing.T) {
		opts := testParseOptions(t, true)
		if cfg, err := ParseAccessLogs([]*v3accesslogpb.AccessLog{stdout}, opts); cfg != nil || err != nil {
			t.Fatalf("ParseAccessLogs() = (%v, %v), want (nil, nil)", cfg, err)
		}
	})
	t.Run("empty", func(t *testing.T) {
		opts := testParseOptions(t, true)
		if cfg, err := ParseAccessLogs(nil, opts); cfg != nil || err != nil {
			t.Fatalf("ParseAccessLogs() = (%v, %v), want (nil, nil)", cfg, err)
		}
	})
}

func (s) TestParseAccessLogs_UntrustedServer(t *testing.T) {
	opts := testParseOptions(t, false)

	cfg, err := ParseAccessLogs([]*v3accesslogpb.AccessLog{
		otelAccessLog(t, &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{GrpcService: grpcService("dns:///allowed.example.com:4317")}),
	}, opts)
	if err != nil {
		t.Fatalf("ParseAccessLogs() with an allowed target failed: %v", err)
	}
	closeConfig(cfg)

	_, err = ParseAccessLogs([]*v3accesslogpb.AccessLog{
		otelAccessLog(t, &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{GrpcService: grpcService(collectorTarget)}),
	}, opts)
	if err == nil || !strings.Contains(err.Error(), "allowed_grpc_services") {
		t.Fatalf("ParseAccessLogs() with a target that is not allowed returned error %v, want an allowed_grpc_services error", err)
	}
}

func (s) TestParseAccessLogs_Errors(t *testing.T) {
	valid := func() *v3otelaccesslogpb.OpenTelemetryAccessLogConfig {
		return &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{GrpcService: grpcService(collectorTarget)}
	}
	with := func(f func(*v3otelaccesslogpb.OpenTelemetryAccessLogConfig)) *v3otelaccesslogpb.OpenTelemetryAccessLogConfig {
		c := valid()
		f(c)
		return c
	}
	nestedResource := &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_ArrayValue{ArrayValue: &otlpcommonpb.ArrayValue{}}}
	tests := []struct {
		name    string
		config  *v3otelaccesslogpb.OpenTelemetryAccessLogConfig
		wantErr string
	}{
		{
			name:    "no_grpc_service",
			config:  &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{},
			wantErr: "grpc_service: field not present",
		},
		{
			name:    "envoy_grpc_service",
			config:  &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{GrpcService: &v3corepb.GrpcService{TargetSpecifier: &v3corepb.GrpcService_EnvoyGrpc_{}}},
			wantErr: "only google_grpc",
		},
		{
			name:    "bad_body_template",
			config:  with(func(c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig) { c.Body = stringValue("%NOPE%") }),
			wantErr: "body: string_value",
		},
		{
			name:    "empty_body_value",
			config:  with(func(c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig) { c.Body = &otlpcommonpb.AnyValue{} }),
			wantErr: "body: field not present",
		},
		{
			name: "bad_nested_attribute_template",
			config: with(func(c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig) {
				c.Attributes = &otlpcommonpb.KeyValueList{Values: []*otlpcommonpb.KeyValue{{
					Key: "a",
					Value: &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_ArrayValue{ArrayValue: &otlpcommonpb.ArrayValue{
						Values: []*otlpcommonpb.AnyValue{stringValue("%REQ()%")},
					}}},
				}}}
			}),
			wantErr: "attributes: values[0].value: array_value.values[0]",
		},
		{
			name: "empty_attribute_key",
			config: with(func(c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig) {
				c.Attributes = &otlpcommonpb.KeyValueList{Values: []*otlpcommonpb.KeyValue{{Value: intValue(1)}}}
			}),
			wantErr: "attributes: values[0].key: must be non-empty",
		},
		{
			name: "nested_resource_attribute",
			config: with(func(c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig) {
				c.ResourceAttributes = &otlpcommonpb.KeyValueList{Values: []*otlpcommonpb.KeyValue{{Key: "a", Value: nestedResource}}}
			}),
			wantErr: "resource_attributes: values[0].value: unsupported value type",
		},
		{
			name:    "zero_flush_interval",
			config:  with(func(c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig) { c.BufferFlushInterval = durationpb.New(0) }),
			wantErr: "buffer_flush_interval: must be greater than zero",
		},
		{
			name: "invalid_flush_interval",
			config: with(func(c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig) {
				c.BufferFlushInterval = &durationpb.Duration{Nanos: -1, Seconds: 1}
			}),
			wantErr: "buffer_flush_interval",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := testParseOptions(t, true)
			// A valid logger first checks that its credentials are released
			// on error.
			logs := []*v3accesslogpb.AccessLog{otelAccessLog(t, valid()), otelAccessLog(t, test.config)}
			cfg, err := ParseAccessLogs(logs, opts)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				closeConfig(cfg)
				t.Fatalf("ParseAccessLogs() returned error %v, want error containing %q", err, test.wantErr)
			}
			if !strings.Contains(err.Error(), "access_log[1]") {
				t.Errorf("ParseAccessLogs() error %q does not name entry access_log[1]", err)
			}
		})
	}
}

func (s) TestParseAccessLogs_TypedStruct(t *testing.T) {
	opts := testParseOptions(t, true)
	wrap := func(typeURL string) *v3accesslogpb.AccessLog {
		ts := &v3xdsxdstypepb.TypedStruct{TypeUrl: typeURL, Value: &structpb.Struct{}}
		return &v3accesslogpb.AccessLog{ConfigType: &v3accesslogpb.AccessLog_TypedConfig{TypedConfig: testutils.MarshalAny(t, ts)}}
	}
	if _, err := ParseAccessLogs([]*v3accesslogpb.AccessLog{wrap(TypeURL)}, opts); err == nil || !strings.Contains(err.Error(), "TypedStruct is not supported") {
		t.Errorf("ParseAccessLogs(TypedStruct(%q)) returned error %v, want TypedStruct error", TypeURL, err)
	}
	if cfg, err := ParseAccessLogs([]*v3accesslogpb.AccessLog{wrap("type.googleapis.com/some.other.Logger")}, opts); cfg != nil || err != nil {
		t.Errorf("ParseAccessLogs(TypedStruct(other)) = (%v, %v), want (nil, nil)", cfg, err)
	}
}

func (s) TestBuilderRejectsHTTPFilterUse(t *testing.T) {
	b := Builder()
	if got := b.TypeURLs(); len(got) != 1 || got[0] != TypeURL {
		t.Errorf("TypeURLs() = %v, want [%q]", got, TypeURL)
	}
	if _, err := b.ParseFilterConfig(proto.Message(nil), httpfilter.ParseOptions{}); err == nil {
		t.Error("ParseFilterConfig() succeeded, want error")
	}
	if _, err := b.ParseFilterConfigOverride(proto.Message(nil), httpfilter.ParseOptions{}); err == nil {
		t.Error("ParseFilterConfigOverride() succeeded, want error")
	}
	if b.IsTerminal() {
		t.Error("IsTerminal() = true, want false")
	}
}
