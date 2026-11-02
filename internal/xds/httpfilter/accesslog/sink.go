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
	"sync/atomic"
	"time"

	"google.golang.org/grpc/internal/xds/grpcservice"
)

// Record is one access log record.
type Record struct {
	// Timestamp is the start time of the RPC.
	Timestamp time.Time
	// Body is the evaluated body. It is nil if the logger has no body.
	Body *Value
	// Attributes are the evaluated attributes, in config order.
	Attributes []KeyValue
}

// Sink receives the access log records of one or more loggers that share the
// same sink settings.
type Sink interface {
	// Log enqueues r for export. It is called on the RPC path, so it must not
	// block.
	Log(r Record)
	// Close flushes buffered records and releases the resources of the
	// sink. It may block. Log is not called after Close.
	Close()
}

// SinkOptions are the settings of a sink.
type SinkOptions struct {
	// Collector is the OTLP collector to export to. The sink must not close
	// it; the filter releases its credentials after Sink.Close returns.
	Collector *grpcservice.Config
	// LogName is the log name. It may be empty.
	LogName string
	// ResourceAttributes are the static resource attributes.
	ResourceAttributes []KeyValue
	// FlushInterval is the maximum time a record waits before export.
	FlushInterval time.Duration
	// BufferSizeBytes is the requested buffer size.
	BufferSizeBytes uint32
}

// SinkBuilder creates a Sink.
type SinkBuilder func(SinkOptions) (Sink, error)

var sinkBuilder atomic.Pointer[SinkBuilder]

// SetSinkBuilder sets the builder for access log sinks. If no builder is set,
// access logs are dropped.
//
// It must be called only at initialization time (in an init function).
func SetSinkBuilder(b SinkBuilder) {
	sinkBuilder.Store(&b)
}

// SetSinkBuilderForTesting sets the sink builder, and returns a function that
// restores the previous builder.
func SetSinkBuilderForTesting(b SinkBuilder) (restore func()) {
	old := sinkBuilder.Load()
	if b == nil {
		sinkBuilder.Store(nil)
	} else {
		sinkBuilder.Store(&b)
	}
	return func() { sinkBuilder.Store(old) }
}

func getSinkBuilder() SinkBuilder {
	if b := sinkBuilder.Load(); b != nil {
		return *b
	}
	return nil
}
