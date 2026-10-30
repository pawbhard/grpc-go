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
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/grpctest"
	iresolver "google.golang.org/grpc/internal/resolver"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/xds/httpfilter"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	v3accesslogpb "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	v3otelaccesslogpb "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/open_telemetry/v3"
	otlpcommonpb "go.opentelemetry.io/proto/otlp/common/v1"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

const (
	defaultTestTimeout      = 10 * time.Second
	defaultTestShortTimeout = 50 * time.Millisecond
)

// fakeSink records the records that it receives.
type fakeSink struct {
	opts    SinkOptions
	records chan Record
	closed  chan struct{}
}

func (s *fakeSink) Log(r Record) { s.records <- r }
func (s *fakeSink) Close()       { close(s.closed) }

// fakeSinkBuilder sets a sink builder that creates fakeSinks and sends them on
// the returned channel.
func fakeSinkBuilder(t *testing.T) chan *fakeSink {
	sinks := make(chan *fakeSink, 10)
	restore := SetSinkBuilderForTesting(func(opts SinkOptions) (Sink, error) {
		s := &fakeSink{opts: opts, records: make(chan Record, 10), closed: make(chan struct{})}
		sinks <- s
		return s, nil
	})
	t.Cleanup(restore)
	return sinks
}

func receive[T any](ctx context.Context, t *testing.T, ch chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		t.Fatalf("Timeout waiting for %s", what)
	}
	var zero T
	return zero
}

// testConfig parses an access log config with the given body template and
// attributes.
func testConfig(t *testing.T, body string, attrs map[string]string) *Config {
	t.Helper()
	c := &v3otelaccesslogpb.OpenTelemetryAccessLogConfig{
		GrpcService: grpcService(collectorTarget),
		LogName:     "test-log",
		Body:        stringValue(body),
	}
	if len(attrs) > 0 {
		c.Attributes = &otlpcommonpb.KeyValueList{}
		for k, v := range attrs {
			c.Attributes.Values = append(c.Attributes.Values, &otlpcommonpb.KeyValue{Key: k, Value: stringValue(v)})
		}
	}
	cfg, err := ParseAccessLogs([]*v3accesslogpb.AccessLog{otelAccessLog(t, c)}, testParseOptions(t, true))
	if err != nil {
		t.Fatalf("ParseAccessLogs() failed: %v", err)
	}
	t.Cleanup(func() { closeConfig(cfg) })
	return cfg
}

func attributeMap(r Record) map[string]string {
	m := make(map[string]string)
	for _, kv := range r.Attributes {
		m[kv.Key] = kv.Value.String
	}
	return m
}

const clientBody = "%REQ(:PATH)% %REQ(:AUTHORITY)% %REQ(X-REQ)% %RESP(X-RESP)% %TRAILER(X-TRAILER)% %GRPC_STATUS% %BYTES_SENT% %BYTES_RECEIVED% %DOWNSTREAM_REMOTE_ADDRESS% %PROTOCOL%"

func (s) TestClientInterceptor(t *testing.T) {
	sinks := fakeSinkBuilder(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	ss := &stubserver.StubServer{
		UnaryCallF: func(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			grpc.SetHeader(ctx, metadata.Pairs("x-resp", "resp-value"))
			grpc.SetTrailer(ctx, metadata.Pairs("x-trailer", "trailer-value"))
			if in.GetResponseSize() < 0 {
				return nil, status.Error(codes.NotFound, "not found")
			}
			return &testpb.SimpleResponse{Payload: &testpb.Payload{Body: make([]byte, in.GetResponseSize())}}, nil
		},
	}
	if err := ss.StartServer(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer ss.Stop()
	cc, err := grpc.NewClient(ss.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	defer cc.Close()

	f := Builder().(httpfilter.ClientFilterBuilder).BuildClientFilter(httpfilter.ClientFilterOptions{})
	defer f.Close()
	ci, err := f.BuildClientInterceptor(testConfig(t, clientBody, nil), nil)
	if err != nil || ci == nil {
		t.Fatalf("BuildClientInterceptor() = (%v, %v), want non-nil interceptor", ci, err)
	}
	sink := receive(ctx, t, sinks, "sink creation")
	if sink.opts.LogName != "test-log" || sink.opts.Collector.TargetURI != collectorTarget {
		t.Errorf("Sink options = %+v, want log name %q and collector %q", sink.opts, "test-log", collectorTarget)
	}

	const method = "/grpc.testing.TestService/UnaryCall"
	ri := iresolver.RPCInfo{Method: method, Authority: "test.authority"}
	newStream := func(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return cc.NewStream(ctx, &grpc.StreamDesc{}, method, opts...)
	}
	req := &testpb.SimpleRequest{ResponseSize: 5}

	tests := []struct {
		name     string
		req      *testpb.SimpleRequest
		wantCode codes.Code
		wantBody string
	}{
		{
			name:     "ok",
			req:      req,
			wantCode: codes.OK,
			wantBody: method + " test.authority req-value resp-value trailer-value OK 2 9 - HTTP/2",
		},
		{
			name:     "error",
			req:      &testpb.SimpleRequest{ResponseSize: -1},
			wantCode: codes.NotFound,
			wantBody: method + " test.authority req-value resp-value trailer-value NotFound 11 0 - HTTP/2",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rpcCtx := metadata.AppendToOutgoingContext(ctx, "x-req", "req-value")
			cs, err := ci.NewStream(rpcCtx, ri, newStream)
			if err != nil {
				t.Fatalf("NewStream() failed: %v", err)
			}
			if err := cs.SendMsg(test.req); err != nil {
				t.Fatalf("SendMsg() failed: %v", err)
			}
			if err := cs.CloseSend(); err != nil {
				t.Fatalf("CloseSend() failed: %v", err)
			}
			resp := &testpb.SimpleResponse{}
			if err := cs.RecvMsg(resp); status.Code(err) != test.wantCode {
				t.Fatalf("RecvMsg() returned error %v, want code %v", err, test.wantCode)
			}
			if test.wantCode == codes.OK {
				// Read the end of the stream so that the RPC finishes.
				if err := cs.RecvMsg(resp); err == nil {
					t.Fatal("Second RecvMsg() succeeded, want io.EOF")
				}
			}
			rec := receive(ctx, t, sink.records, "access log record")
			if rec.Body == nil || rec.Body.String != test.wantBody {
				t.Errorf("Record body = %+v, want %q", rec.Body, test.wantBody)
			}
			if rec.Timestamp.IsZero() {
				t.Error("Record timestamp is zero")
			}
		})
	}

	// A failure before the stream starts is logged once.
	wantErr := status.Error(codes.PermissionDenied, "denied by a later filter")
	if _, err := ci.NewStream(ctx, ri, func(context.Context, ...grpc.CallOption) (grpc.ClientStream, error) { return nil, wantErr }); err != wantErr {
		t.Fatalf("NewStream() returned error %v, want %v", err, wantErr)
	}
	rec := receive(ctx, t, sink.records, "access log record")
	if want := method + " test.authority - - - PermissionDenied 0 0 - HTTP/2"; rec.Body.String != want {
		t.Errorf("Record body = %q, want %q", rec.Body.String, want)
	}
	sCtx, sCancel := context.WithTimeout(ctx, defaultTestShortTimeout)
	defer sCancel()
	select {
	case rec := <-sink.records:
		t.Fatalf("Got an extra record %+v", rec)
	case <-sCtx.Done():
	}

	// The sink closes when the interceptor closes.
	ci.Close()
	receive(ctx, t, sink.closed, "sink close")
}

func (s) TestServerInterceptor(t *testing.T) {
	sinks := fakeSinkBuilder(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	f := Builder().(httpfilter.ServerFilterBuilder).BuildServerFilter()
	defer f.Close()
	cfg := testConfig(t, "%REQ(:PATH)% %REQ(X-REQ)% %RESP(X-RESP)% %TRAILER(X-TRAILER)% %GRPC_STATUS% %BYTES_RECEIVED% %BYTES_SENT%", map[string]string{"peer": "%DOWNSTREAM_REMOTE_ADDRESS%"})
	si, err := f.BuildServerInterceptor(cfg, nil)
	if err != nil || si == nil {
		t.Fatalf("BuildServerInterceptor() = (%v, %v), want non-nil interceptor", si, err)
	}
	defer si.Close()
	sink := receive(ctx, t, sinks, "sink creation")

	// rejectErr, if set, makes a later filter reject the RPC.
	var rejectErr error
	wrapper := func(ss grpc.ServerStream) (grpc.ServerStream, error) {
		ss, err := si.InterceptRPC(ss)
		if err != nil {
			return nil, err
		}
		if rejectErr != nil {
			return nil, rejectErr
		}
		return ss, nil
	}
	opt := internal.XDSFilterWrapperOption.(func(func(grpc.ServerStream) (grpc.ServerStream, error)) grpc.ServerOption)(wrapper)
	ss := &stubserver.StubServer{
		UnaryCallF: func(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			grpc.SetHeader(ctx, metadata.Pairs("x-resp", "resp-value"))
			grpc.SetTrailer(ctx, metadata.Pairs("x-trailer", "trailer-value"))
			return &testpb.SimpleResponse{Payload: &testpb.Payload{Body: make([]byte, in.GetResponseSize())}}, nil
		},
	}
	if err := ss.Start([]grpc.ServerOption{opt}); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer ss.Stop()

	rpcCtx := metadata.AppendToOutgoingContext(ctx, "x-req", "req-value")
	if _, err := ss.Client.UnaryCall(rpcCtx, &testpb.SimpleRequest{ResponseSize: 5}); err != nil {
		t.Fatalf("UnaryCall() failed: %v", err)
	}
	rec := receive(ctx, t, sink.records, "access log record")
	if want := "/grpc.testing.TestService/UnaryCall req-value resp-value trailer-value OK 2 9"; rec.Body.String != want {
		t.Errorf("Record body = %q, want %q", rec.Body.String, want)
	}
	if got := attributeMap(rec)["peer"]; got == "" || got == "-" {
		t.Errorf("Record attribute peer = %q, want the client address", got)
	}

	// A rejection by a later filter is logged with its status.
	rejectErr = status.Error(codes.PermissionDenied, "denied")
	if _, err := ss.Client.UnaryCall(rpcCtx, &testpb.SimpleRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("UnaryCall() returned error %v, want PermissionDenied", err)
	}
	rec = receive(ctx, t, sink.records, "access log record")
	if want := "/grpc.testing.TestService/UnaryCall req-value - - PermissionDenied 0 0"; rec.Body.String != want {
		t.Errorf("Record body = %q, want %q", rec.Body.String, want)
	}
}

func (s) TestSinkSharingAndLifetime(t *testing.T) {
	sinks := fakeSinkBuilder(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	f := Builder().(httpfilter.ClientFilterBuilder).BuildClientFilter(httpfilter.ClientFilterOptions{})
	defer f.Close()

	// Two interceptors with the same sink settings and different templates
	// share one sink.
	i1, err := f.BuildClientInterceptor(testConfig(t, "a", nil), nil)
	if err != nil {
		t.Fatalf("BuildClientInterceptor() failed: %v", err)
	}
	sink := receive(ctx, t, sinks, "sink creation")
	i2, err := f.BuildClientInterceptor(testConfig(t, "b", nil), nil)
	if err != nil {
		t.Fatalf("BuildClientInterceptor() failed: %v", err)
	}
	sCtx, sCancel := context.WithTimeout(ctx, defaultTestShortTimeout)
	defer sCancel()
	select {
	case s := <-sinks:
		t.Fatalf("A second sink was created with options %+v", s.opts)
	case <-sCtx.Done():
	}

	// An RPC that is in progress keeps the sink open after both interceptors
	// close.
	finish := make(chan struct{})
	started := make(chan struct{})
	go func() {
		i2.NewStream(ctx, iresolver.RPCInfo{Method: "/s/m"}, func(_ context.Context, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			close(started)
			<-finish
			return nil, errors.New("stream failed")
		})
	}()
	<-started
	i1.Close()
	i2.Close()
	select {
	case <-sink.closed:
		t.Fatal("Sink closed while an RPC is in progress")
	case <-time.After(defaultTestShortTimeout):
	}
	close(finish)
	if rec := receive(ctx, t, sink.records, "access log record"); rec.Body.String != "b" {
		t.Errorf("Record body = %q, want %q", rec.Body.String, "b")
	}
	receive(ctx, t, sink.closed, "sink close")

	// A new interceptor creates a new sink.
	i3, err := f.BuildClientInterceptor(testConfig(t, "c", nil), nil)
	if err != nil {
		t.Fatalf("BuildClientInterceptor() failed: %v", err)
	}
	defer i3.Close()
	receive(ctx, t, sinks, "sink creation")
}

// Tests that an RPC that starts after its interceptor closed does not use the
// closed sink. This can happen when an xDS update closes the interceptor that
// a new RPC already selected.
func (s) TestRPCAfterInterceptorClose(t *testing.T) {
	sinks := fakeSinkBuilder(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	f := Builder().(httpfilter.ClientFilterBuilder).BuildClientFilter(httpfilter.ClientFilterOptions{})
	defer f.Close()
	ci, err := f.BuildClientInterceptor(testConfig(t, "a", nil), nil)
	if err != nil || ci == nil {
		t.Fatalf("BuildClientInterceptor() = (%v, %v), want non-nil interceptor", ci, err)
	}
	sink := receive(ctx, t, sinks, "sink creation")
	ci.Close()
	receive(ctx, t, sink.closed, "sink close")

	wantErr := errors.New("stream failed")
	if _, err := ci.NewStream(ctx, iresolver.RPCInfo{Method: "/s/m"}, func(context.Context, ...grpc.CallOption) (grpc.ClientStream, error) {
		return nil, wantErr
	}); err != wantErr {
		t.Fatalf("NewStream() returned error %v, want %v", err, wantErr)
	}
	select {
	case rec := <-sink.records:
		t.Fatalf("Got record %+v on a closed sink", rec)
	case <-time.After(defaultTestShortTimeout):
	}
}

func (s) TestNoSinkBuilder(t *testing.T) {
	t.Cleanup(SetSinkBuilderForTesting(nil))
	f := Builder().(httpfilter.ServerFilterBuilder).BuildServerFilter()
	defer f.Close()
	if i, err := f.BuildServerInterceptor(testConfig(t, "a", nil), nil); i != nil || err != nil {
		t.Fatalf("BuildServerInterceptor() = (%v, %v), want (nil, nil)", i, err)
	}
}

func (s) TestSinkBuilderError(t *testing.T) {
	t.Cleanup(SetSinkBuilderForTesting(func(SinkOptions) (Sink, error) { return nil, errors.New("no sink") }))
	grpctest.ExpectError("Failed to create the access log sink")
	f := Builder().(httpfilter.ClientFilterBuilder).BuildClientFilter(httpfilter.ClientFilterOptions{})
	defer f.Close()
	if i, err := f.BuildClientInterceptor(testConfig(t, "a", nil), nil); i != nil || err != nil {
		t.Fatalf("BuildClientInterceptor() = (%v, %v), want (nil, nil)", i, err)
	}
}

func (s) TestCaptureHeaders(t *testing.T) {
	md := metadata.MD{"a": {"1", "2"}, "b-bin": {"\x00\x01"}, "c": {"3"}}
	got := captureHeaders(md, map[string]bool{"a": true, "b-bin": true, "missing": true})
	if len(got) != 2 || got["a"] != "1,2" || got["b-bin"] != "AAE" {
		t.Errorf("captureHeaders() = %v, want map[a:1,2 b-bin:AAE]", got)
	}
}
