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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	v1xdsudpatypepb "github.com/cncf/xds/go/udpa/type/v1"
	v3xdsxdstypepb "github.com/cncf/xds/go/xds/type/v3"
	v3accesslogpb "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	v3otelaccesslogpb "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/open_telemetry/v3"
	otlpcommonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/xds/grpcservice"
	"google.golang.org/grpc/internal/xds/httpfilter"
	"google.golang.org/protobuf/types/known/anypb"
)

var logger = grpclog.Component("xds")

const (
	// TypeURL is the type URL of the OpenTelemetry access logger config.
	TypeURL = "type.googleapis.com/envoy.extensions.access_loggers.open_telemetry.v3.OpenTelemetryAccessLogConfig"

	// FilterName is the name of the access log filter in the HTTP filter
	// list. The name is reserved for gRPC; HTTP filter overrides with this
	// name are ignored.
	FilterName = "grpc.internal.xds_access_log"

	// defaultFlushInterval and defaultBufferSizeBytes are the Envoy defaults.
	defaultFlushInterval   = time.Second
	defaultBufferSizeBytes = 16384
)

// LoggerConfig is the parsed form of one OpenTelemetryAccessLogConfig.
type LoggerConfig struct {
	// Collector is the OTLP collector to export to. It owns built
	// credentials.
	Collector *grpcservice.Config
	// LogName is the log name. It may be empty.
	LogName string
	// ResourceAttributes are the static resource attributes, including the
	// built-in labels. Values are never of KindSlice or KindMap.
	ResourceAttributes []KeyValue
	// FlushInterval is the maximum time a record waits before export.
	FlushInterval time.Duration
	// BufferSizeBytes is the requested buffer size.
	BufferSizeBytes uint32

	body       *valueTemplate
	attributes []keyValueTemplate
}

// sinkKey returns a string that identifies the sink settings of c. Two
// loggers with the same key share one sink. The templates are evaluated by
// the filter, not by the sink, so they are not part of the key.
func (c *LoggerConfig) sinkKey() string {
	var b strings.Builder
	b.WriteString("{collector=")
	b.WriteString(c.Collector.Key())
	b.WriteString(", log_name=")
	b.WriteString(strconv.Quote(c.LogName))
	b.WriteString(", resource_attributes=")
	formatKeyValues(&b, c.ResourceAttributes)
	fmt.Fprintf(&b, ", flush_interval=%v, buffer_size_bytes=%d}", c.FlushInterval, c.BufferSizeBytes)
	return b.String()
}

func (c *LoggerConfig) String() string {
	var b strings.Builder
	b.WriteString("{collector=")
	b.WriteString(strconv.Quote(c.Collector.TargetURI))
	b.WriteString(", log_name=")
	b.WriteString(strconv.Quote(c.LogName))
	b.WriteString(", resource_attributes=")
	formatKeyValues(&b, c.ResourceAttributes)
	fmt.Fprintf(&b, ", flush_interval=%v, buffer_size_bytes=%d", c.FlushInterval, c.BufferSizeBytes)
	if c.body != nil {
		b.WriteString(", body=")
		c.body.format(&b)
	}
	if len(c.attributes) > 0 {
		b.WriteString(", attributes=")
		formatKeyValueTemplates(&b, c.attributes)
	}
	b.WriteByte('}')
	return b.String()
}

// Config is the filter config of the access log filter. It holds all the
// OpenTelemetry access loggers of one HttpConnectionManager.
type Config struct {
	httpfilter.FilterConfig
	// Loggers are the loggers in config order. It is never empty.
	Loggers []*LoggerConfig

	usage fieldUsage
}

func (c *Config) String() string {
	parts := make([]string, len(c.Loggers))
	for i, l := range c.Loggers {
		parts[i] = l.String()
	}
	return "{loggers=[" + strings.Join(parts, ", ") + "]}"
}

// ParseAccessLogs parses the access_log field of an HttpConnectionManager.
//
// It returns nil and no error if the feature is disabled, or if no entry is a
// supported OpenTelemetry access logger. Entries of other logger types,
// entries with an AccessLogFilter, and entries with only an http_service are
// ignored. An invalid OpenTelemetry access logger entry is an error.
func ParseAccessLogs(logs []*v3accesslogpb.AccessLog, opts httpfilter.ParseOptions) (_ *Config, err error) {
	if len(logs) == 0 || !envconfig.XDSOTelAccessLogEnabled {
		return nil, nil
	}
	cfg := &Config{usage: fieldUsage{headers: newHeaderNames()}}
	defer func() {
		// Release the credentials of loggers parsed before the error.
		if err != nil {
			for _, l := range cfg.Loggers {
				l.Collector.Close()
			}
		}
	}()
	for i, al := range logs {
		// AccessLogFilter is not supported. Ignore the entry rather than log
		// every RPC.
		if al.GetFilter() != nil {
			logger.Infof("Ignoring access log entry %d (%q) with an unsupported filter", i, al.GetName())
			continue
		}
		tc := al.GetTypedConfig()
		if tc == nil {
			continue
		}
		typeURL, err := unwrapTypeURL(tc)
		if err != nil {
			return nil, fmt.Errorf("access_log[%d].typed_config: %v", i, err)
		}
		// Unsupported access logger types are ignored.
		if typeURL != TypeURL {
			continue
		}
		if tc.GetTypeUrl() != TypeURL {
			return nil, fmt.Errorf("access_log[%d].typed_config: TypedStruct is not supported for %q", i, TypeURL)
		}
		otelCfg := &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{}
		if err := tc.UnmarshalTo(otelCfg); err != nil {
			return nil, fmt.Errorf("access_log[%d].typed_config: failed to unmarshal OpenTelemetryAccessLogConfig: %v", i, err)
		}
		lc, err := parseLoggerConfig(otelCfg, opts)
		if err != nil {
			return nil, fmt.Errorf("access_log[%d].typed_config: %v", i, err)
		}
		if lc == nil {
			logger.Infof("Ignoring access log entry %d (%q) with only an http_service", i, al.GetName())
			continue
		}
		cfg.Loggers = append(cfg.Loggers, lc)
		if lc.body != nil {
			lc.body.addUsage(&cfg.usage)
		}
		for j := range lc.attributes {
			lc.attributes[j].value.addUsage(&cfg.usage)
		}
	}
	if len(cfg.Loggers) == 0 {
		return nil, nil
	}
	return cfg, nil
}

// unwrapTypeURL returns the type URL of the config in tc, looking inside
// TypedStruct messages.
func unwrapTypeURL(tc *anypb.Any) (string, error) {
	switch {
	case tc.MessageIs(&v3xdsxdstypepb.TypedStruct{}):
		s := &v3xdsxdstypepb.TypedStruct{}
		if err := tc.UnmarshalTo(s); err != nil {
			return "", fmt.Errorf("failed to unmarshal TypedStruct: %v", err)
		}
		return s.GetTypeUrl(), nil
	case tc.MessageIs(&v1xdsudpatypepb.TypedStruct{}):
		s := &v1xdsudpatypepb.TypedStruct{}
		if err := tc.UnmarshalTo(s); err != nil {
			return "", fmt.Errorf("failed to unmarshal TypedStruct: %v", err)
		}
		return s.GetTypeUrl(), nil
	}
	return tc.GetTypeUrl(), nil
}

// parseLoggerConfig returns nil and no error if the entry must be ignored.
func parseLoggerConfig(c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig, opts httpfilter.ParseOptions) (_ *LoggerConfig, err error) {
	lc := &LoggerConfig{
		LogName:         c.GetLogName(),
		FlushInterval:   defaultFlushInterval,
		BufferSizeBytes: defaultBufferSizeBytes,
	}
	// grpc_service and log_name, with a fallback to the deprecated
	// common_config.
	gs, gsField := c.GetGrpcService(), "grpc_service"
	if gs == nil && c.GetCommonConfig() != nil {
		gs, gsField = c.GetCommonConfig().GetGrpcService(), "common_config.grpc_service"
	}
	if lc.LogName == "" {
		lc.LogName = c.GetCommonConfig().GetLogName()
	}
	if gs == nil {
		if c.GetHttpService() != nil {
			// OTLP/HTTP is not supported.
			return nil, nil
		}
		return nil, errors.New("grpc_service: field not present")
	}
	if opts.BootstrapConfig == nil || opts.ServerConfig == nil {
		return nil, fmt.Errorf("%s: no bootstrap or server config to parse with", gsField)
	}

	if d := c.GetBufferFlushInterval(); d != nil {
		if err := d.CheckValid(); err != nil {
			return nil, fmt.Errorf("buffer_flush_interval: %v", err)
		}
		if lc.FlushInterval = d.AsDuration(); lc.FlushInterval <= 0 {
			return nil, fmt.Errorf("buffer_flush_interval: must be greater than zero, got %v", lc.FlushInterval)
		}
	}
	if v := c.GetBufferSizeBytes(); v != nil {
		lc.BufferSizeBytes = v.GetValue()
	}
	if c.Body != nil {
		t, err := parseValueTemplate(c.GetBody())
		if err != nil {
			return nil, fmt.Errorf("body: %v", err)
		}
		lc.body = &t
	}
	if lc.attributes, err = parseKeyValueTemplates(c.GetAttributes()); err != nil {
		return nil, fmt.Errorf("attributes: %v", err)
	}
	if lc.ResourceAttributes, err = parseResourceAttributes(c, lc.LogName, opts); err != nil {
		return nil, fmt.Errorf("resource_attributes: %v", err)
	}

	// The collector is parsed last, so that no error path drops built
	// credentials.
	if lc.Collector, err = grpcservice.Parse(gs, opts.BootstrapConfig, opts.ServerConfig); err != nil {
		return nil, fmt.Errorf("%s: %v", gsField, err)
	}
	return lc, nil
}

// parseResourceAttributes returns the built-in labels followed by the
// configured resource attributes. A configured attribute replaces a built-in
// label with the same key.
func parseResourceAttributes(c *v3otelaccesslogpb.OpenTelemetryAccessLogConfig, logName string, opts httpfilter.ParseOptions) ([]KeyValue, error) {
	var attrs []KeyValue
	if !c.GetDisableBuiltinLabels() {
		node := opts.BootstrapConfig.Node()
		attrs = []KeyValue{
			{Key: "log_name", Value: Value{Kind: KindString, String: logName}},
			{Key: "zone_name", Value: Value{Kind: KindString, String: node.GetLocality().GetZone()}},
			{Key: "cluster_name", Value: Value{Kind: KindString, String: node.GetCluster()}},
			{Key: "node_name", Value: Value{Kind: KindString, String: node.GetId()}},
		}
	}
	for i, kv := range c.GetResourceAttributes().GetValues() {
		if kv.GetKey() == "" {
			return nil, fmt.Errorf("values[%d].key: must be non-empty", i)
		}
		v, err := parseStaticScalar(kv.GetValue())
		if err != nil {
			return nil, fmt.Errorf("values[%d].value: %v", i, err)
		}
		replaced := false
		for j := range attrs {
			if attrs[j].Key == kv.GetKey() {
				attrs[j].Value = v
				replaced = true
				break
			}
		}
		if !replaced {
			attrs = append(attrs, KeyValue{Key: kv.GetKey(), Value: v})
		}
	}
	return attrs, nil
}

// parseStaticScalar parses a resource attribute value. Resource attributes
// are static, and OpenTelemetry resources support only scalar values here.
func parseStaticScalar(v *otlpcommonpb.AnyValue) (Value, error) {
	switch x := v.GetValue().(type) {
	case *otlpcommonpb.AnyValue_StringValue:
		return Value{Kind: KindString, String: x.StringValue}, nil
	case *otlpcommonpb.AnyValue_IntValue:
		return Value{Kind: KindInt64, Int64: x.IntValue}, nil
	case *otlpcommonpb.AnyValue_BoolValue:
		return Value{Kind: KindBool, Bool: x.BoolValue}, nil
	case *otlpcommonpb.AnyValue_DoubleValue:
		return Value{Kind: KindFloat64, Float64: x.DoubleValue}, nil
	case nil:
		return Value{}, errors.New("field not present")
	}
	return Value{}, errors.New("unsupported value type: only string_value, int_value, bool_value, and double_value are supported")
}

// parseValueTemplate parses a body or attribute value. String values are
// substitution templates.
func parseValueTemplate(v *otlpcommonpb.AnyValue) (valueTemplate, error) {
	switch x := v.GetValue().(type) {
	case *otlpcommonpb.AnyValue_StringValue:
		f, err := ParseFormat(x.StringValue)
		if err != nil {
			return valueTemplate{}, fmt.Errorf("string_value: %v", err)
		}
		return valueTemplate{kind: KindString, formatter: f}, nil
	case *otlpcommonpb.AnyValue_IntValue:
		return valueTemplate{kind: KindInt64, static: Value{Kind: KindInt64, Int64: x.IntValue}}, nil
	case *otlpcommonpb.AnyValue_BoolValue:
		return valueTemplate{kind: KindBool, static: Value{Kind: KindBool, Bool: x.BoolValue}}, nil
	case *otlpcommonpb.AnyValue_DoubleValue:
		return valueTemplate{kind: KindFloat64, static: Value{Kind: KindFloat64, Float64: x.DoubleValue}}, nil
	case *otlpcommonpb.AnyValue_BytesValue:
		return valueTemplate{kind: KindBytes, static: Value{Kind: KindBytes, Bytes: x.BytesValue}}, nil
	case *otlpcommonpb.AnyValue_ArrayValue:
		vs := x.ArrayValue.GetValues()
		t := valueTemplate{kind: KindSlice, slice: make([]valueTemplate, len(vs))}
		for i, e := range vs {
			var err error
			if t.slice[i], err = parseValueTemplate(e); err != nil {
				return valueTemplate{}, fmt.Errorf("array_value.values[%d]: %v", i, err)
			}
		}
		return t, nil
	case *otlpcommonpb.AnyValue_KvlistValue:
		kvs, err := parseKeyValueTemplates(x.KvlistValue)
		if err != nil {
			return valueTemplate{}, fmt.Errorf("kvlist_value: %v", err)
		}
		return valueTemplate{kind: KindMap, kvs: kvs}, nil
	case nil:
		return valueTemplate{}, errors.New("field not present")
	}
	return valueTemplate{}, fmt.Errorf("unsupported value type %T", v.GetValue())
}

func parseKeyValueTemplates(l *otlpcommonpb.KeyValueList) ([]keyValueTemplate, error) {
	var out []keyValueTemplate
	for i, kv := range l.GetValues() {
		if kv.GetKey() == "" {
			return nil, fmt.Errorf("values[%d].key: must be non-empty", i)
		}
		t, err := parseValueTemplate(kv.GetValue())
		if err != nil {
			return nil, fmt.Errorf("values[%d].value: %v", i, err)
		}
		out = append(out, keyValueTemplate{key: kv.GetKey(), value: t})
	}
	return out, nil
}
