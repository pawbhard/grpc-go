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
 *
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package xds_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/internal/testutils/otlplogs"
	"google.golang.org/grpc/internal/testutils/xds/e2e"
	"google.golang.org/grpc/internal/testutils/xds/e2e/setup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/xds"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	v3accesslogpb "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	v3listenerpb "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	v3rbacconfigpb "github.com/envoyproxy/go-control-plane/envoy/config/rbac/v3"
	v3routepb "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	v3otelaccesslogpb "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/open_telemetry/v3"
	v3rbacpb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/rbac/v3"
	v3httppb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	otlpcommonpb "go.opentelemetry.io/proto/otlp/common/v1"
	otlplogspb "go.opentelemetry.io/proto/otlp/logs/v1"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"

	_ "google.golang.org/grpc/stats/opentelemetry/xdsaccesslog" // Register the OpenTelemetry access log sink.
)

const accessLogBody = "%REQ(:PATH)% %REQ(X-USER)% %GRPC_STATUS% %RESP(X-RESP)% %TRAILER(X-TRAILER)%"

// otelAccessLogConfig returns an HCM access_log entry that exports to the
// collector at addr.
func otelAccessLogConfig(t *testing.T, addr string) *v3accesslogpb.AccessLog {
	t.Helper()
	return &v3accesslogpb.AccessLog{
		Name: "envoy.access_loggers.open_telemetry",
		ConfigType: &v3accesslogpb.AccessLog_TypedConfig{TypedConfig: testutils.MarshalAny(t, &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{
			GrpcService: &v3corepb.GrpcService{
				TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
					TargetUri: "passthrough:///" + addr,
					ChannelCredentialsPlugin: []*anypb.Any{{
						TypeUrl: "type.googleapis.com/envoy.extensions.grpc_service.channel_credentials.insecure.v3.InsecureCredentials",
					}},
				}},
			},
			LogName:             "e2e-log",
			BufferFlushInterval: durationpb.New(10 * time.Millisecond),
			Body:                &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_StringValue{StringValue: accessLogBody}},
			Attributes: &otlpcommonpb.KeyValueList{Values: []*otlpcommonpb.KeyValue{{
				Key:   "duration_ms",
				Value: &otlpcommonpb.AnyValue{Value: &otlpcommonpb.AnyValue_StringValue{StringValue: "%DURATION%"}},
			}}},
		})},
	}
}

// waitForAccessLog returns the next exported record and its resource
// attributes.
func waitForAccessLog(ctx context.Context, t *testing.T, c *otlplogs.Collector) (*otlplogspb.LogRecord, map[string]string) {
	t.Helper()
	for {
		select {
		case req := <-c.Requests:
			records := otlplogs.LogRecords(req.Data)
			if len(records) == 0 {
				continue
			}
			resource := make(map[string]string)
			for _, kv := range req.Data.GetResourceLogs()[0].GetResource().GetAttributes() {
				resource[kv.GetKey()] = kv.GetValue().GetStringValue()
			}
			return records[0], resource
		case <-ctx.Done():
			t.Fatal("Timeout waiting for an access log record")
		}
	}
}

func accessLogTestBackend(t *testing.T, opts ...grpc.ServerOption) *stubserver.StubServer {
	t.Helper()
	return &stubserver.StubServer{
		UnaryCallF: func(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			grpc.SetHeader(ctx, metadata.Pairs("x-resp", "resp-value"))
			grpc.SetTrailer(ctx, metadata.Pairs("x-trailer", "trailer-value"))
			if in.GetFillUsername() {
				return nil, status.Error(codes.NotFound, "no user")
			}
			return &testpb.SimpleResponse{}, nil
		},
	}
}

// Tests that an xDS client exports one access log record per RPC to the
// collector configured in the client Listener.
func (s) TestOTelAccessLog_Client(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, true)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	collector := otlplogs.StartCollector(t)
	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	backend := accessLogTestBackend(t)
	if err := backend.StartServer(); err != nil {
		t.Fatalf("Failed to start backend: %v", err)
	}
	defer backend.Stop()

	const serviceName = "my-service-access-log"
	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, backend.Address),
		SecLevel:   e2e.SecurityLevelNone,
	})
	// Add the access log to the client Listener.
	resources.Listeners[0].ApiListener.ApiListener = testutils.MarshalAny(t, &v3httppb.HttpConnectionManager{
		RouteSpecifier: &v3httppb.HttpConnectionManager_Rds{Rds: &v3httppb.Rds{
			ConfigSource:    &v3corepb.ConfigSource{ConfigSourceSpecifier: &v3corepb.ConfigSource_Ads{Ads: &v3corepb.AggregatedConfigSource{}}},
			RouteConfigName: resources.Routes[0].GetName(),
		}},
		HttpFilters: []*v3httppb.HttpFilter{e2e.RouterHTTPFilter},
		AccessLog:   []*v3accesslogpb.AccessLog{otelAccessLogConfig(t, collector.Address)},
	})
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatalf("managementServer.Update() failed: %v", err)
	}

	cc, err := grpc.NewClient("xds:///"+serviceName, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	defer cc.Close()
	client := testgrpc.NewTestServiceClient(cc)

	rpcCtx := metadata.AppendToOutgoingContext(ctx, "x-user", "alice")
	if _, err := client.UnaryCall(rpcCtx, &testpb.SimpleRequest{}); err != nil {
		t.Fatalf("UnaryCall() failed: %v", err)
	}
	rec, resource := waitForAccessLog(ctx, t, collector)
	if want := "/grpc.testing.TestService/UnaryCall alice OK resp-value trailer-value"; rec.GetBody().GetStringValue() != want {
		t.Errorf("Client access log body = %q, want %q", rec.GetBody().GetStringValue(), want)
	}
	if resource["node_name"] != nodeID || resource["log_name"] != "e2e-log" {
		t.Errorf("Client access log resource = %v, want node_name %q and log_name %q", resource, nodeID, "e2e-log")
	}
	if len(rec.GetAttributes()) != 1 || rec.GetAttributes()[0].GetKey() != "duration_ms" {
		t.Errorf("Client access log attributes = %v, want one duration_ms attribute", rec.GetAttributes())
	}

	if _, err := client.UnaryCall(rpcCtx, &testpb.SimpleRequest{FillUsername: true}); status.Code(err) != codes.NotFound {
		t.Fatalf("UnaryCall() returned error %v, want NotFound", err)
	}
	rec, _ = waitForAccessLog(ctx, t, collector)
	if want := "/grpc.testing.TestService/UnaryCall alice NotFound resp-value trailer-value"; rec.GetBody().GetStringValue() != want {
		t.Errorf("Client access log body = %q, want %q", rec.GetBody().GetStringValue(), want)
	}
}

// startAccessLogXDSServer starts an xDS-enabled server whose inbound Listener
// has the given HTTP filters (before the router) and access logs. It returns a
// client for the server.
func startAccessLogXDSServer(t *testing.T, ctx context.Context, httpFilters []*v3httppb.HttpFilter, logs []*v3accesslogpb.AccessLog) testgrpc.TestServiceClient {
	t.Helper()
	managementServer, nodeID, bootstrapContents, _ := setup.ManagementServerAndResolver(t)

	servingCh := make(chan struct{})
	modeOpt := xds.ServingModeCallback(func(_ net.Addr, args xds.ServingModeChangeArgs) {
		if args.Mode == connectivity.ServingModeServing {
			close(servingCh)
		}
	})
	server, err := xds.NewGRPCServer(grpc.Creds(insecure.NewCredentials()), modeOpt, xds.BootstrapContentsForTesting(bootstrapContents))
	if err != nil {
		t.Fatalf("xds.NewGRPCServer() failed: %v", err)
	}
	backend := accessLogTestBackend(t)
	backend.S = server
	lis, err := testutils.LocalTCPListener()
	if err != nil {
		t.Fatalf("testutils.LocalTCPListener() failed: %v", err)
	}
	backend.Listener = lis
	stubserver.StartTestService(t, backend)
	t.Cleanup(server.Stop)

	host, port, err := hostPortFromListener(lis)
	if err != nil {
		t.Fatalf("Failed to retrieve host and port of server: %v", err)
	}
	filters := append(append([]*v3httppb.HttpFilter{}, httpFilters...), e2e.RouterHTTPFilter)
	hcm := &v3httppb.HttpConnectionManager{
		HttpFilters: filters,
		AccessLog:   logs,
		RouteSpecifier: &v3httppb.HttpConnectionManager_RouteConfig{RouteConfig: &v3routepb.RouteConfiguration{
			Name: "routeName",
			VirtualHosts: []*v3routepb.VirtualHost{{
				Domains: []string{"*"},
				Routes: []*v3routepb.Route{{
					Match:  &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: "/"}},
					Action: &v3routepb.Route_NonForwardingAction{},
				}},
			}},
		}},
	}
	inbound := &v3listenerpb.Listener{
		Name: fmt.Sprintf(e2e.ServerListenerResourceNameTemplate, net.JoinHostPort(host, strconv.Itoa(int(port)))),
		Address: &v3corepb.Address{Address: &v3corepb.Address_SocketAddress{SocketAddress: &v3corepb.SocketAddress{
			Address:       host,
			PortSpecifier: &v3corepb.SocketAddress_PortValue{PortValue: port},
		}}},
		DefaultFilterChain: &v3listenerpb.FilterChain{Filters: []*v3listenerpb.Filter{{
			Name:       "hcm",
			ConfigType: &v3listenerpb.Filter_TypedConfig{TypedConfig: testutils.MarshalAny(t, hcm)},
		}}},
	}
	if err := managementServer.Update(ctx, e2e.UpdateOptions{
		NodeID:         nodeID,
		Listeners:      []*v3listenerpb.Listener{inbound},
		SkipValidation: true,
	}); err != nil {
		t.Fatalf("managementServer.Update() failed: %v", err)
	}
	select {
	case <-servingCh:
	case <-ctx.Done():
		t.Fatal("Timeout waiting for the server to enter SERVING mode")
	}

	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	t.Cleanup(func() { cc.Close() })
	return testgrpc.NewTestServiceClient(cc)
}

// Tests that an xDS-enabled server exports one access log record per RPC.
func (s) TestOTelAccessLog_Server(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, true)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	collector := otlplogs.StartCollector(t)
	client := startAccessLogXDSServer(t, ctx, nil, []*v3accesslogpb.AccessLog{otelAccessLogConfig(t, collector.Address)})

	rpcCtx := metadata.AppendToOutgoingContext(ctx, "x-user", "bob")
	if _, err := client.UnaryCall(rpcCtx, &testpb.SimpleRequest{}); err != nil {
		t.Fatalf("UnaryCall() failed: %v", err)
	}
	rec, _ := waitForAccessLog(ctx, t, collector)
	if want := "/grpc.testing.TestService/UnaryCall bob OK resp-value trailer-value"; rec.GetBody().GetStringValue() != want {
		t.Errorf("Server access log body = %q, want %q", rec.GetBody().GetStringValue(), want)
	}

	if _, err := client.UnaryCall(rpcCtx, &testpb.SimpleRequest{FillUsername: true}); status.Code(err) != codes.NotFound {
		t.Fatalf("UnaryCall() returned error %v, want NotFound", err)
	}
	rec, _ = waitForAccessLog(ctx, t, collector)
	if want := "/grpc.testing.TestService/UnaryCall bob NotFound resp-value trailer-value"; rec.GetBody().GetStringValue() != want {
		t.Errorf("Server access log body = %q, want %q", rec.GetBody().GetStringValue(), want)
	}
}

// Tests that the server access log sees RPCs that a later HTTP filter (RBAC)
// rejects, because the access log filter is the first filter.
func (s) TestOTelAccessLog_ServerLogsRBACDenial(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSOTelAccessLogEnabled, true)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	collector := otlplogs.StartCollector(t)
	denyAll := e2e.HTTPFilter("rbac", &v3rbacpb.RBAC{Rules: &v3rbacconfigpb.RBAC{
		Action: v3rbacconfigpb.RBAC_DENY,
		Policies: map[string]*v3rbacconfigpb.Policy{"deny-all": {
			Permissions: []*v3rbacconfigpb.Permission{{Rule: &v3rbacconfigpb.Permission_Any{Any: true}}},
			Principals:  []*v3rbacconfigpb.Principal{{Identifier: &v3rbacconfigpb.Principal_Any{Any: true}}},
		}},
	}})
	client := startAccessLogXDSServer(t, ctx, []*v3httppb.HttpFilter{denyAll}, []*v3accesslogpb.AccessLog{otelAccessLogConfig(t, collector.Address)})

	rpcCtx := metadata.AppendToOutgoingContext(ctx, "x-user", "mallory")
	if _, err := client.UnaryCall(rpcCtx, &testpb.SimpleRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("UnaryCall() returned error %v, want PermissionDenied", err)
	}
	rec, _ := waitForAccessLog(ctx, t, collector)
	if got := rec.GetBody().GetStringValue(); !strings.HasPrefix(got, "/grpc.testing.TestService/UnaryCall mallory PermissionDenied") {
		t.Errorf("Server access log body = %q, want prefix %q", got, "/grpc.testing.TestService/UnaryCall mallory PermissionDenied")
	}
}
