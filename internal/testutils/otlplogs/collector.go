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

// Package otlplogs provides a fake OTLP logs collector for tests.
package otlplogs

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	otlplogspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

// Request is one export request that the collector received.
type Request struct {
	// Data has the same wire format as ExportLogsServiceRequest.
	Data     *otlplogspb.LogsData
	Metadata metadata.MD
}

// Collector is a fake OTLP logs collector. It implements the
// opentelemetry.proto.collector.logs.v1.LogsService/Export method.
type Collector struct {
	// Address is the address that the collector listens on.
	Address string
	// Requests receives each export request.
	Requests chan *Request
	// Response, if set, returns the response or error for each request. The
	// default response is an empty ExportLogsServiceResponse.
	Response func(*Request) (*emptypb.Empty, error)
}

// StartCollector starts a collector on a local port. It stops when the test
// ends.
func StartCollector(t *testing.T) *Collector {
	t.Helper()
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("net.Listen() failed: %v", err)
	}
	c := &Collector{Address: lis.Addr().String(), Requests: make(chan *Request, 100)}
	s := grpc.NewServer()
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "opentelemetry.proto.collector.logs.v1.LogsService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Export",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				data := &otlplogspb.LogsData{}
				if err := dec(data); err != nil {
					return nil, err
				}
				md, _ := metadata.FromIncomingContext(ctx)
				req := &Request{Data: data, Metadata: md}
				c.Requests <- req
				if c.Response != nil {
					return c.Response(req)
				}
				return &emptypb.Empty{}, nil
			},
		}},
	}, struct{}{})
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return c
}

// LogRecords returns all log records in data.
func LogRecords(data *otlplogspb.LogsData) []*otlplogspb.LogRecord {
	var out []*otlplogspb.LogRecord
	for _, rl := range data.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			out = append(out, sl.GetLogRecords()...)
		}
	}
	return out
}
