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

package xdsresource

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/internal/testutils/xds/e2e"
	"google.golang.org/grpc/internal/xds/bootstrap"
	"google.golang.org/grpc/internal/xds/httpfilter/accesslog"
	"google.golang.org/protobuf/types/known/anypb"

	v3accesslogpb "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	v3listenerpb "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	v3routepb "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	v3otelaccesslogpb "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/open_telemetry/v3"
	v3httppb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	otlpcommonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

func accessLogTestConfigs(t *testing.T) (*bootstrap.Config, *bootstrap.ServerConfig) {
	t.Helper()
	contents, err := bootstrap.NewContentsForTesting(bootstrap.ConfigOptionsForTesting{
		Servers: json.RawMessage(`[{"server_uri":"td.googleapis.com:443","channel_creds":[{"type":"insecure"}]}]`),
		Node:    json.RawMessage(`{"id":"node-id"}`),
	})
	if err != nil {
		t.Fatalf("NewContentsForTesting() failed: %v", err)
	}
	bc, err := bootstrap.NewConfigFromContents(contents)
	if err != nil {
		t.Fatalf("NewConfigFromContents() failed: %v", err)
	}
	sc, err := bootstrap.ServerConfigForTesting(bootstrap.ServerConfigTestingOptions{
		URI:            "td.googleapis.com:443",
		ServerFeatures: []string{"trusted_xds_server"},
	})
	if err != nil {
		t.Fatalf("ServerConfigForTesting() failed: %v", err)
	}
	return bc, sc
}

func otelAccessLogEntry(t *testing.T, body string) *v3accesslogpb.AccessLog {
	return &v3accesslogpb.AccessLog{
		Name: "otel",
		ConfigType: &v3accesslogpb.AccessLog_TypedConfig{TypedConfig: testutils.MarshalAny(t, &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{
			GrpcService: &v3corepb.GrpcService{
				TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
					TargetUri: "dns:///collector.example.com:4317",
					ChannelCredentialsPlugin: []*anypb.Any{{
						TypeUrl: "type.googleapis.com/envoy.extensions.grpc_service.channel_credentials.insecure.v3.InsecureCredentials",
					}},
				}},
			},
			Body: &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_StringValue{StringValue: body}},
		})},
	}
}

func clientListenerWithAccessLog(t *testing.T, httpFilters []*v3httppb.HttpFilter, logs ...*v3accesslogpb.AccessLog) *anypb.Any {
	return testutils.MarshalAny(t, &v3listenerpb.Listener{
		Name: "lds.target.good:3333",
		ApiListener: &v3listenerpb.ApiListener{
			ApiListener: testutils.MarshalAny(t, &v3httppb.HttpConnectionManager{
				RouteSpecifier: &v3httppb.HttpConnectionManager_Rds{
					Rds: &v3httppb.Rds{
						ConfigSource:    &v3corepb.ConfigSource{ConfigSourceSpecifier: &v3corepb.ConfigSource_Ads{Ads: &v3corepb.AggregatedConfigSource{}}},
						RouteConfigName: "route-config-name",
					},
				},
				HttpFilters: httpFilters,
				AccessLog:   logs,
			}),
		},
	})
}

func serverListenerWithAccessLog(t *testing.T, logs ...*v3accesslogpb.AccessLog) *anypb.Any {
	routeConfig := &v3routepb.RouteConfiguration{
		Name: "routeName",
		VirtualHosts: []*v3routepb.VirtualHost{{
			Domains: []string{"*"},
			Routes: []*v3routepb.Route{{
				Match:  &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: "/"}},
				Action: &v3routepb.Route_NonForwardingAction{},
			}},
		}},
	}
	return testutils.MarshalAny(t, &v3listenerpb.Listener{
		Name:    "grpc/server?xds.resource.listening_address=0.0.0.0:9999",
		Address: localSocketAddress,
		DefaultFilterChain: &v3listenerpb.FilterChain{
			Name: "default-filter-chain",
			Filters: []*v3listenerpb.Filter{{
				Name: "hcm",
				ConfigType: &v3listenerpb.Filter_TypedConfig{
					TypedConfig: testutils.MarshalAny(t, &v3httppb.HttpConnectionManager{
						RouteSpecifier: &v3httppb.HttpConnectionManager_RouteConfig{RouteConfig: routeConfig},
						HttpFilters:    []*v3httppb.HttpFilter{e2e.RouterHTTPFilter},
						AccessLog:      logs,
					}),
				},
			}},
		},
	})
}

func closeAccessLogFilters(filters []HTTPFilter) {
	for _, f := range filters {
		if cfg, ok := f.Config.(*accesslog.Config); ok {
			for _, l := range cfg.Loggers {
				l.Collector.Close()
			}
		}
	}
}

// checkAccessLogFilterFirst verifies that the access log filter is the first
// of the two filters, followed by the router.
func checkAccessLogFilterFirst(t *testing.T, filters []HTTPFilter) {
	t.Helper()
	if len(filters) != 2 {
		t.Fatalf("Got %d HTTP filters, want 2 (access log and router): %+v", len(filters), filters)
	}
	if filters[0].Name != accesslog.FilterName {
		t.Errorf("First HTTP filter name = %q, want %q", filters[0].Name, accesslog.FilterName)
	}
	cfg, ok := filters[0].Config.(*accesslog.Config)
	if !ok || len(cfg.Loggers) != 1 {
		t.Errorf("First HTTP filter config = %v, want *accesslog.Config with 1 logger", filters[0].Config)
	}
	if !filters[1].Filter.IsTerminal() {
		t.Errorf("Second HTTP filter %q is not terminal", filters[1].Name)
	}
}

func (s) TestUnmarshalListener_AccessLog(t *testing.T) {
	bc, sc := accessLogTestConfigs(t)

	t.Run("client_side", func(t *testing.T) {
		testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, true)
		lis := clientListenerWithAccessLog(t, []*v3httppb.HttpFilter{e2e.RouterHTTPFilter}, otelAccessLogEntry(t, "%GRPC_STATUS%"))
		_, update, err := unmarshalListenerResource(lis, bc, sc)
		if err != nil {
			t.Fatalf("unmarshalListenerResource() failed: %v", err)
		}
		defer closeAccessLogFilters(update.APIListener.HTTPFilters)
		checkAccessLogFilterFirst(t, update.APIListener.HTTPFilters)
	})

	t.Run("server_side", func(t *testing.T) {
		testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, true)
		lis := serverListenerWithAccessLog(t, otelAccessLogEntry(t, "%DOWNSTREAM_REMOTE_ADDRESS%"))
		_, update, err := unmarshalListenerResource(lis, bc, sc)
		if err != nil {
			t.Fatalf("unmarshalListenerResource() failed: %v", err)
		}
		filters := update.TCPListener.DefaultFilterChain.HTTPConnMgr.HTTPFilters
		defer closeAccessLogFilters(filters)
		checkAccessLogFilterFirst(t, filters)
	})

	t.Run("feature_disabled", func(t *testing.T) {
		testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, false)
		// The invalid template is not parsed when the feature is disabled.
		lis := clientListenerWithAccessLog(t, []*v3httppb.HttpFilter{e2e.RouterHTTPFilter}, otelAccessLogEntry(t, "%NOPE%"))
		_, update, err := unmarshalListenerResource(lis, bc, sc)
		if err != nil {
			t.Fatalf("unmarshalListenerResource() failed: %v", err)
		}
		if got := len(update.APIListener.HTTPFilters); got != 1 {
			t.Fatalf("Got %d HTTP filters, want 1 (router only)", got)
		}
	})

	t.Run("invalid_config_nacks_client", func(t *testing.T) {
		testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, true)
		lis := clientListenerWithAccessLog(t, []*v3httppb.HttpFilter{e2e.RouterHTTPFilter}, otelAccessLogEntry(t, "%NOPE%"))
		if _, _, err := unmarshalListenerResource(lis, bc, sc); err == nil || !strings.Contains(err.Error(), "invalid access_log") {
			t.Fatalf("unmarshalListenerResource() returned error %v, want an invalid access_log error", err)
		}
	})

	t.Run("invalid_config_nacks_server", func(t *testing.T) {
		testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, true)
		lis := serverListenerWithAccessLog(t, otelAccessLogEntry(t, "%NOPE%"))
		if _, _, err := unmarshalListenerResource(lis, bc, sc); err == nil || !strings.Contains(err.Error(), "invalid access_log") {
			t.Fatalf("unmarshalListenerResource() returned error %v, want an invalid access_log error", err)
		}
	})

	t.Run("reserved_filter_name_nacks", func(t *testing.T) {
		testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, true)
		reserved := &v3httppb.HttpFilter{Name: accesslog.FilterName, ConfigType: e2e.RouterHTTPFilter.ConfigType}
		lis := clientListenerWithAccessLog(t, []*v3httppb.HttpFilter{reserved}, otelAccessLogEntry(t, "%GRPC_STATUS%"))
		if _, _, err := unmarshalListenerResource(lis, bc, sc); err == nil || !strings.Contains(err.Error(), "is reserved") {
			t.Fatalf("unmarshalListenerResource() returned error %v, want a reserved name error", err)
		}
	})
}
