/*
 *
 * Copyright 2020 gRPC authors.
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

package test

import (
	"context"
	"io"
	"runtime/pprof"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/serverfinish"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

type ctxKey string

// TestServerReturningContextError verifies that if a context error is returned
// by the service handler, the status will have the correct status code, not
// Unknown.
func (s) TestServerReturningContextError(t *testing.T) {
	ss := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			return nil, context.DeadlineExceeded
		},
		FullDuplexCallF: func(testgrpc.TestService_FullDuplexCallServer) error {
			return context.DeadlineExceeded
		},
	}
	if err := ss.Start(nil); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	_, err := ss.Client.EmptyCall(ctx, &testpb.Empty{})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.DeadlineExceeded {
		t.Fatalf("ss.Client.EmptyCall() got error %v; want <status with Code()=DeadlineExceeded>", err)
	}

	stream, err := ss.Client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatalf("unexpected error starting the stream: %v", err)
	}
	_, err = stream.Recv()
	if s, ok := status.FromError(err); !ok || s.Code() != codes.DeadlineExceeded {
		t.Fatalf("ss.Client.FullDuplexCall().Recv() got error %v; want <status with Code()=DeadlineExceeded>", err)
	}

}

func pprofCtxCollectLabels(ctx context.Context) map[string]string {
	seenLabels := map[string]string{}
	pprof.ForLabels(ctx, func(k, val string) bool {
		seenLabels[k] = val
		return true
	})
	return seenLabels
}

// TestServerSetGoroutineLabelsInContext verifies that when enabled, the
// grpc.method runtime/pprof goroutine label gets set in the context that's
// passed to the handlers.
func (s) TestServerSetGoroutineLabelsInContext(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.LabelServerGoroutines, envconfig.GoroutineLabelServerMethod)
	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			ctxLabels := pprofCtxCollectLabels(ctx)
			if val, ok := ctxLabels["grpc.method"]; !ok {
				t.Errorf("missing \"grpc.method\" label; found labels: %v", ctxLabels)
			} else if wantVal := "/grpc.testing.TestService/EmptyCall"; val != wantVal {
				t.Errorf("unexpected value for \"grpc.method\" label %q; want %q", ctxLabels["grpc.method"], wantVal)
			}
			return &testpb.Empty{}, nil
		},
		FullDuplexCallF: func(stream testgrpc.TestService_FullDuplexCallServer) error {
			ctxLabels := pprofCtxCollectLabels(stream.Context())
			if val, ok := ctxLabels["grpc.method"]; !ok {
				t.Errorf("missing \"grpc.method\" label; found labels: %v", ctxLabels)
			} else if wantVal := "/grpc.testing.TestService/FullDuplexCall"; val != wantVal {
				t.Errorf("unexpected value for \"grpc.method\" label %q; want %q", ctxLabels["grpc.method"], wantVal)
			}
			return nil
		},
	}
	if err := ss.Start(nil); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("ss.Client.EmptyCall() got error %v; want OK", err)
	}

	stream, err := ss.Client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatalf("unexpected error starting the stream: %v", err)
	}
	if _, err = stream.Recv(); err != io.EOF {
		t.Fatalf("ss.Client.FullDuplexCall().Recv() got error %v; want io.EOF", err)
	}
}

// TestServerSetGoroutineLabelsInContextEnvVarDisabled verifies that when
// disable, the grpc.method runtime/pprof goroutine label does _not_ get set in
// the context that's passed to the handlers.
func (s) TestServerSetGoroutineLabelsInContextEnvVarDisabled(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.LabelServerGoroutines, 0)
	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			ctxLabels := pprofCtxCollectLabels(ctx)
			if val, ok := ctxLabels["grpc.method"]; ok {
				t.Errorf("\"grpc.method\" label set with value %q; found labels: %v", val, ctxLabels)
			}
			return &testpb.Empty{}, nil
		},
		FullDuplexCallF: func(stream testgrpc.TestService_FullDuplexCallServer) error {
			ctxLabels := pprofCtxCollectLabels(stream.Context())
			if val, ok := ctxLabels["grpc.method"]; ok {
				t.Errorf("\"grpc.method\" label set with value %q; found labels: %v", val, ctxLabels)
			}
			return nil
		},
	}
	if err := ss.Start(nil); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("ss.Client.EmptyCall() got error %v; want OK", err)
	}

	stream, err := ss.Client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatalf("unexpected error starting the stream: %v", err)
	}
	if _, err = stream.Recv(); err != io.EOF {
		t.Fatalf("ss.Client.FullDuplexCall().Recv() got error %v; want io.EOF", err)
	}
}

func (s) TestChainUnaryServerInterceptor(t *testing.T) {
	var (
		firstIntKey  = ctxKey("firstIntKey")
		secondIntKey = ctxKey("secondIntKey")
	)

	firstInt := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if ctx.Value(firstIntKey) != nil {
			return nil, status.Errorf(codes.Internal, "first interceptor should not have %v in context", firstIntKey)
		}
		if ctx.Value(secondIntKey) != nil {
			return nil, status.Errorf(codes.Internal, "first interceptor should not have %v in context", secondIntKey)
		}

		firstCtx := context.WithValue(ctx, firstIntKey, 0)
		resp, err := handler(firstCtx, req)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to handle request at firstInt")
		}

		simpleResp, ok := resp.(*testpb.SimpleResponse)
		if !ok {
			return nil, status.Errorf(codes.Internal, "failed to get *testpb.SimpleResponse at firstInt")
		}
		return &testpb.SimpleResponse{
			Payload: &testpb.Payload{
				Type: simpleResp.GetPayload().GetType(),
				Body: append(simpleResp.GetPayload().GetBody(), '1'),
			},
		}, nil
	}

	secondInt := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if ctx.Value(firstIntKey) == nil {
			return nil, status.Errorf(codes.Internal, "second interceptor should have %v in context", firstIntKey)
		}
		if ctx.Value(secondIntKey) != nil {
			return nil, status.Errorf(codes.Internal, "second interceptor should not have %v in context", secondIntKey)
		}

		secondCtx := context.WithValue(ctx, secondIntKey, 1)
		resp, err := handler(secondCtx, req)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to handle request at secondInt")
		}

		simpleResp, ok := resp.(*testpb.SimpleResponse)
		if !ok {
			return nil, status.Errorf(codes.Internal, "failed to get *testpb.SimpleResponse at secondInt")
		}
		return &testpb.SimpleResponse{
			Payload: &testpb.Payload{
				Type: simpleResp.GetPayload().GetType(),
				Body: append(simpleResp.GetPayload().GetBody(), '2'),
			},
		}, nil
	}

	lastInt := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if ctx.Value(firstIntKey) == nil {
			return nil, status.Errorf(codes.Internal, "last interceptor should have %v in context", firstIntKey)
		}
		if ctx.Value(secondIntKey) == nil {
			return nil, status.Errorf(codes.Internal, "last interceptor should not have %v in context", secondIntKey)
		}

		resp, err := handler(ctx, req)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to handle request at lastInt at lastInt")
		}

		simpleResp, ok := resp.(*testpb.SimpleResponse)
		if !ok {
			return nil, status.Errorf(codes.Internal, "failed to get *testpb.SimpleResponse at lastInt")
		}
		return &testpb.SimpleResponse{
			Payload: &testpb.Payload{
				Type: simpleResp.GetPayload().GetType(),
				Body: append(simpleResp.GetPayload().GetBody(), '3'),
			},
		}, nil
	}

	sopts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(firstInt, secondInt, lastInt),
	}

	ss := &stubserver.StubServer{
		UnaryCallF: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			payload, err := newPayload(testpb.PayloadType_COMPRESSABLE, 0)
			if err != nil {
				return nil, status.Errorf(codes.Aborted, "failed to make payload: %v", err)
			}

			return &testpb.SimpleResponse{
				Payload: payload,
			}, nil
		},
	}
	if err := ss.Start(sopts); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	resp, err := ss.Client.UnaryCall(ctx, &testpb.SimpleRequest{})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.OK {
		t.Fatalf("ss.Client.UnaryCall(ctx, _) = %v, %v; want nil, <status with Code()=OK>", resp, err)
	}

	respBytes := resp.Payload.GetBody()
	if string(respBytes) != "321" {
		t.Fatalf("invalid response: want=%s, but got=%s", "321", resp)
	}
}

func (s) TestChainOnBaseUnaryServerInterceptor(t *testing.T) {
	baseIntKey := ctxKey("baseIntKey")

	baseInt := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if ctx.Value(baseIntKey) != nil {
			return nil, status.Errorf(codes.Internal, "base interceptor should not have %v in context", baseIntKey)
		}

		baseCtx := context.WithValue(ctx, baseIntKey, 1)
		return handler(baseCtx, req)
	}

	chainInt := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if ctx.Value(baseIntKey) == nil {
			return nil, status.Errorf(codes.Internal, "chain interceptor should have %v in context", baseIntKey)
		}

		return handler(ctx, req)
	}

	sopts := []grpc.ServerOption{
		grpc.UnaryInterceptor(baseInt),
		grpc.ChainUnaryInterceptor(chainInt),
	}

	ss := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			return &testpb.Empty{}, nil
		},
	}
	if err := ss.Start(sopts); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	resp, err := ss.Client.EmptyCall(ctx, &testpb.Empty{})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.OK {
		t.Fatalf("ss.Client.EmptyCall(ctx, _) = %v, %v; want nil, <status with Code()=OK>", resp, err)
	}
}

func (s) TestChainStreamServerInterceptor(t *testing.T) {
	callCounts := make([]atomic.Int32, 4)

	firstInt := func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if callCounts[0].Load() != 0 {
			return status.Errorf(codes.Internal, "callCounts[0] should be 0, but got=%d", callCounts[0].Load())
		}
		if callCounts[1].Load() != 0 {
			return status.Errorf(codes.Internal, "callCounts[1] should be 0, but got=%d", callCounts[1].Load())
		}
		if callCounts[2].Load() != 0 {
			return status.Errorf(codes.Internal, "callCounts[2] should be 0, but got=%d", callCounts[2].Load())
		}
		if callCounts[3].Load() != 0 {
			return status.Errorf(codes.Internal, "callCounts[3] should be 0, but got=%d", callCounts[3].Load())
		}
		callCounts[0].Add(1)
		return handler(srv, stream)
	}

	secondInt := func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if callCounts[0].Load() != 1 {
			return status.Errorf(codes.Internal, "callCounts[0] should be 1, but got=%d", callCounts[0].Load())
		}
		if callCounts[1].Load() != 0 {
			return status.Errorf(codes.Internal, "callCounts[1] should be 0, but got=%d", callCounts[1].Load())
		}
		if callCounts[2].Load() != 0 {
			return status.Errorf(codes.Internal, "callCounts[2] should be 0, but got=%d", callCounts[2].Load())
		}
		if callCounts[3].Load() != 0 {
			return status.Errorf(codes.Internal, "callCounts[3] should be 0, but got=%d", callCounts[3].Load())
		}
		callCounts[1].Add(1)
		return handler(srv, stream)
	}

	lastInt := func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if callCounts[0].Load() != 1 {
			return status.Errorf(codes.Internal, "callCounts[0] should be 1, but got=%d", callCounts[0].Load())
		}
		if callCounts[1].Load() != 1 {
			return status.Errorf(codes.Internal, "callCounts[1] should be 1, but got=%d", callCounts[1].Load())
		}
		if callCounts[2].Load() != 0 {
			return status.Errorf(codes.Internal, "callCounts[2] should be 0, but got=%d", callCounts[2].Load())
		}
		if callCounts[3].Load() != 0 {
			return status.Errorf(codes.Internal, "callCounts[3] should be 0, but got=%d", callCounts[3].Load())
		}
		callCounts[2].Add(1)
		return handler(srv, stream)
	}

	sopts := []grpc.ServerOption{
		grpc.ChainStreamInterceptor(firstInt, secondInt, lastInt),
	}

	ss := &stubserver.StubServer{
		FullDuplexCallF: func(testgrpc.TestService_FullDuplexCallServer) error {
			if callCounts[0].Load() != 1 {
				return status.Errorf(codes.Internal, "callCounts[0] should be 1, but got=%d", callCounts[0].Load())
			}
			if callCounts[1].Load() != 1 {
				return status.Errorf(codes.Internal, "callCounts[1] should be 1, but got=%d", callCounts[1].Load())
			}
			if callCounts[2].Load() != 1 {
				return status.Errorf(codes.Internal, "callCounts[2] should be 0, but got=%d", callCounts[2].Load())
			}
			if callCounts[3].Load() != 0 {
				return status.Errorf(codes.Internal, "callCounts[3] should be 0, but got=%d", callCounts[3].Load())
			}
			callCounts[3].Add(1)
			return nil
		},
	}
	if err := ss.Start(sopts); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	stream, err := ss.Client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatalf("failed to FullDuplexCall: %v", err)
	}

	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("failed to recv from stream: %v", err)
	}

	if callCounts[3].Load() != 1 {
		t.Fatalf("callCounts[3] should be 1, but got=%d", callCounts[3].Load())
	}
}

// Test verifies that only unary interceptors are invoked for unary RPCs and
// only streaming interceptors are invoked for streaming RPCs.
func (s) TestInterceptorSegregation(t *testing.T) {
	var unaryCalled, streamCalled atomic.Bool

	unaryInt := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		unaryCalled.Store(true)
		return handler(ctx, req)
	}
	streamInt := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		streamCalled.Store(true)
		return handler(srv, ss)
	}
	sopts := []grpc.ServerOption{grpc.UnaryInterceptor(unaryInt), grpc.StreamInterceptor(streamInt)}
	ss := &stubserver.StubServer{
		UnaryCallF: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			return &testpb.SimpleResponse{}, nil
		},
		FullDuplexCallF: func(testgrpc.TestService_FullDuplexCallServer) error {
			return nil
		},
	}
	if err := ss.Start(sopts); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	// Make a unary RPC and ensure that only the unary interceptor was invoked.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := ss.Client.UnaryCall(ctx, &testpb.SimpleRequest{}); err != nil {
		t.Fatalf("ss.Client.UnaryCall failed: %v", err)
	}
	if !unaryCalled.Load() {
		t.Error("Unary interceptor was not called for Unary RPC")
	}
	if streamCalled.Load() {
		t.Error("Stream interceptor was called for Unary RPC")
	}

	// Make a streaming RPC and ensure that only the streaming interceptor was
	// invoked.
	unaryCalled.Store(false)
	streamCalled.Store(false)
	stream, err := ss.Client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatalf("ss.Client.FullDuplexCall failed: %v", err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("stream.Recv() returned %v, want io.EOF", err)
	}
	if unaryCalled.Load() {
		t.Error("Unary interceptor was called for Streaming RPC")
	}
	if !streamCalled.Load() {
		t.Error("Stream interceptor was not called for Streaming RPC")
	}
}

type wrappedTestStreamKey struct{}

type wrappedTestStream struct {
	grpc.ServerStream
}

func (w *wrappedTestStream) Context() context.Context {
	return context.WithValue(w.ServerStream.Context(), wrappedTestStreamKey{}, w)
}

// Test verifies that an internal xDS filter wrapper option configured on the
// server gets invoked and can wrap the ServerStream for both Unary and
// Streaming RPCs.
func (s) TestXDSFilterWrapperOption(t *testing.T) {
	var wrapperCallCount atomic.Int32
	wrapper := func(ss grpc.ServerStream) (grpc.ServerStream, error) {
		wrapperCallCount.Add(1)
		return &wrappedTestStream{ServerStream: ss}, nil
	}

	opt := internal.XDSFilterWrapperOption.(func(func(grpc.ServerStream) (grpc.ServerStream, error)) grpc.ServerOption)(wrapper)

	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			if _, ok := ctx.Value(wrappedTestStreamKey{}).(*wrappedTestStream); !ok {
				return nil, status.Errorf(codes.Internal, "context value is %T, want *wrappedTestStream", ctx.Value(wrappedTestStreamKey{}))
			}
			return &testpb.Empty{}, nil
		},
		FullDuplexCallF: func(stream testgrpc.TestService_FullDuplexCallServer) error {
			if _, ok := stream.Context().Value(wrappedTestStreamKey{}).(*wrappedTestStream); !ok {
				return status.Errorf(codes.Internal, "context value is %T, want *wrappedTestStream", stream.Context().Value(wrappedTestStreamKey{}))
			}
			return nil
		},
	}
	if err := ss.Start([]grpc.ServerOption{opt}); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall failed: %v", err)
	}
	if got := wrapperCallCount.Load(); got != 1 {
		t.Fatalf("XDSFilterWrapperOption callback call count for Unary RPC got %d, want 1", got)
	}

	stream, err := ss.Client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatalf("FullDuplexCall failed: %v", err)
	}
	if _, err = stream.Recv(); err != io.EOF {
		t.Fatalf("Recv failed: %v", err)
	}
	if got := wrapperCallCount.Load(); got != 2 {
		t.Fatalf("XDSFilterWrapperOption callback call count for Streaming RPC got %d, want 2", got)
	}
}

// Test verifies that if an internal xDS filter wrapper returns an error,
// the RPC is rejected early with that status error before executing
// handlers.
func (s) TestXDSFilterWrapperOption_EarlyRejection(t *testing.T) {
	wrapper := func(grpc.ServerStream) (grpc.ServerStream, error) {
		return nil, status.Error(codes.PermissionDenied, "early rejection by internal wrapper")
	}

	opt := internal.XDSFilterWrapperOption.(func(func(grpc.ServerStream) (grpc.ServerStream, error)) grpc.ServerOption)(wrapper)

	ss := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			return &testpb.Empty{}, nil
		},
		FullDuplexCallF: func(testgrpc.TestService_FullDuplexCallServer) error {
			return nil
		},
	}
	if err := ss.Start([]grpc.ServerOption{opt}); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("EmptyCall failed with error %v; want PermissionDenied", err)
	}

	stream, err := ss.Client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatalf("FullDuplexCall failed: %v", err)
	}
	if _, err = stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stream.Recv() got error %v; want PermissionDenied", err)
	}
}

// Test verifies that callbacks registered with serverfinish.Register by an
// internal xDS filter wrapper run once per RPC, after the server writes the
// final status, and see the final status, headers, and trailers.
func (s) TestXDSFilterWrapperOption_FinishHook(t *testing.T) {
	tests := []struct {
		name        string
		wrapperErr  error
		handlerErr  error
		wantCode    codes.Code
		wantHeader  bool
		wantTrailer bool
	}{
		{
			name:        "ok",
			wantCode:    codes.OK,
			wantHeader:  true,
			wantTrailer: true,
		},
		{
			name:        "handler_error",
			handlerErr:  status.Error(codes.NotFound, "not found"),
			wantCode:    codes.NotFound,
			wantHeader:  true,
			wantTrailer: true,
		},
		{
			name:       "wrapper_error",
			wrapperErr: status.Error(codes.PermissionDenied, "denied"),
			wantCode:   codes.PermissionDenied,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			infoCh := make(chan *serverfinish.Info, 2)
			wrapper := func(ss grpc.ServerStream) (grpc.ServerStream, error) {
				if !serverfinish.Register(ss.Context(), func(info *serverfinish.Info) { infoCh <- info }) {
					t.Errorf("serverfinish.Register() = false, want true")
				}
				if test.wrapperErr != nil {
					return nil, test.wrapperErr
				}
				return ss, nil
			}
			opt := internal.XDSFilterWrapperOption.(func(func(grpc.ServerStream) (grpc.ServerStream, error)) grpc.ServerOption)(wrapper)

			ss := &stubserver.StubServer{
				EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
					grpc.SetHeader(ctx, metadata.Pairs("test-header", "h"))
					grpc.SetTrailer(ctx, metadata.Pairs("test-trailer", "t"))
					if test.handlerErr != nil {
						return nil, test.handlerErr
					}
					return &testpb.Empty{}, nil
				},
			}
			if err := ss.Start([]grpc.ServerOption{opt}); err != nil {
				t.Fatalf("Error starting endpoint server: %v", err)
			}
			defer ss.Stop()

			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()
			if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}); status.Code(err) != test.wantCode {
				t.Fatalf("EmptyCall() returned error %v; want code %v", err, test.wantCode)
			}

			var info *serverfinish.Info
			select {
			case info = <-infoCh:
			case <-ctx.Done():
				t.Fatal("Timeout waiting for the finish callback")
			}
			if got := status.Code(info.Err); got != test.wantCode {
				t.Errorf("Info.Err code = %v, want %v", got, test.wantCode)
			}
			if got := len(info.Header.Get("test-header")) == 1; got != test.wantHeader {
				t.Errorf("Info.Header = %v, want test-header present: %v", info.Header, test.wantHeader)
			}
			if got := len(info.Trailer.Get("test-trailer")) == 1; got != test.wantTrailer {
				t.Errorf("Info.Trailer = %v, want test-trailer present: %v", info.Trailer, test.wantTrailer)
			}

			// The callback must run only once.
			sCtx, sCancel := context.WithTimeout(ctx, defaultTestShortTimeout)
			defer sCancel()
			select {
			case info := <-infoCh:
				t.Fatalf("Finish callback ran a second time with %+v", info)
			case <-sCtx.Done():
			}
		})
	}
}

// Test verifies that servers without an internal xDS filter wrapper do not
// attach a finish callback registry to the RPC context.
func (s) TestServerFinishHook_NotAttachedWithoutWrapper(t *testing.T) {
	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			if serverfinish.Register(ctx, func(*serverfinish.Info) {}) {
				return nil, status.Error(codes.Internal, "serverfinish.Register() = true, want false")
			}
			return &testpb.Empty{}, nil
		},
	}
	if err := ss.Start(nil); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}
}
