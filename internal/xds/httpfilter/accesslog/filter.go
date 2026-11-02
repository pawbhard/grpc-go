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
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/grpcsync"
	iresolver "google.golang.org/grpc/internal/resolver"
	"google.golang.org/grpc/internal/serverfinish"
	"google.golang.org/grpc/internal/xds/httpfilter"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// noSinkBuilderWarningInterval is the minimum time between two warnings about
// a missing sink builder.
const noSinkBuilderWarningInterval = time.Minute

var (
	lastNoSinkBuilderWarning atomic.Int64 // Unix nanoseconds.
	timeNow                  = time.Now
)

// warnNoSinkBuilder logs a warning at most once per
// noSinkBuilderWarningInterval.
func warnNoSinkBuilder() {
	now := timeNow().UnixNano()
	last := lastNoSinkBuilderWarning.Load()
	if now-last < int64(noSinkBuilderWarningInterval) || !lastNoSinkBuilderWarning.CompareAndSwap(last, now) {
		return
	}
	logger.Warningf("OpenTelemetry access logging is configured by xDS, but no sink is registered; access logs are dropped. Import google.golang.org/grpc/stats/opentelemetry/xdsaccesslog to export them.")
}

// filter is the access log filter instance of one channel or one server
// listener. It shares sinks across interceptors, so that a sink survives xDS
// updates that keep its settings.
type filter struct {
	mu    sync.Mutex
	sinks map[string]*grpcsync.RefCounted[Sink] // Keyed by LoggerConfig.sinkKey.
}

func newFilter() *filter {
	return &filter{sinks: make(map[string]*grpcsync.RefCounted[Sink])}
}

// Close does nothing: each sink is closed when its last user releases it.
func (*filter) Close() {}

func (f *filter) BuildClientInterceptor(config, _ httpfilter.FilterConfig) (httpfilter.ClientInterceptor, error) {
	i, err := f.buildInterceptor(config)
	if i == nil {
		// Do not return a nil *interceptor in a non-nil interface.
		return nil, err
	}
	return i, nil
}

func (f *filter) BuildServerInterceptor(config, _ httpfilter.FilterConfig) (httpfilter.ServerInterceptor, error) {
	i, err := f.buildInterceptor(config)
	if i == nil {
		return nil, err
	}
	return i, nil
}

// buildInterceptor returns nil and no error if no logger has a sink. Override
// configs are ignored: the filter name is reserved, so overrides are never
// meant for this filter.
func (f *filter) buildInterceptor(config httpfilter.FilterConfig) (*interceptor, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, fmt.Errorf("accesslog: unexpected config type %T", config)
	}
	sb := getSinkBuilder()
	if sb == nil {
		warnNoSinkBuilder()
		return nil, nil
	}
	i := &interceptor{usage: cfg.usage}
	for _, lc := range cfg.Loggers {
		sink, err := f.getOrCreateSink(lc, sb)
		if err != nil {
			// The config is valid, so the other loggers still work.
			logger.Errorf("Failed to create the access log sink for collector %q: %v", lc.Collector.TargetURI, err)
			continue
		}
		i.loggers = append(i.loggers, activeLogger{config: lc, sink: sink})
	}
	if len(i.loggers) == 0 {
		return nil, nil
	}
	return i, nil
}

// getOrCreateSink returns a reference to the sink for the settings of lc. The
// caller releases the reference with Decrement.
func (f *filter) getOrCreateSink(lc *LoggerConfig, sb SinkBuilder) (*grpcsync.RefCounted[Sink], error) {
	key := lc.sinkKey()
	f.mu.Lock()
	defer f.mu.Unlock()
	if rc, ok := f.sinks[key]; ok && rc.TryIncrement() {
		return rc, nil
	}
	sink, err := sb(SinkOptions{
		Collector:          lc.Collector,
		LogName:            lc.LogName,
		ResourceAttributes: lc.ResourceAttributes,
		FlushInterval:      lc.FlushInterval,
		BufferSizeBytes:    lc.BufferSizeBytes,
	})
	if err != nil {
		return nil, err
	}
	var rc *grpcsync.RefCounted[Sink]
	rc = grpcsync.NewRefCounted(sink, func() {
		f.mu.Lock()
		if f.sinks[key] == rc {
			delete(f.sinks, key)
		}
		f.mu.Unlock()
		// Close flushes buffered records and can block for the export
		// timeout. It must not run on the xDS serializer or on an RPC
		// goroutine. The collector credentials are released after the sink
		// closes its channel.
		go func() {
			sink.Close()
			lc.Collector.Close()
		}()
	})
	f.sinks[key] = rc
	return rc, nil
}

type activeLogger struct {
	config *LoggerConfig
	sink   *grpcsync.RefCounted[Sink]
}

// interceptor is the client and server interceptor of the access log filter.
type interceptor struct {
	loggers []activeLogger
	usage   fieldUsage
}

func (i *interceptor) Close() {
	for _, l := range i.loggers {
		l.sink.Decrement()
	}
}

// newRPC returns the state of a new RPC. It holds a reference to each sink
// until the RPC is logged, so that the sinks survive the interceptor.
//
// An RPC can select an interceptor just before an xDS update closes it. So
// newRPC uses TryIncrement, and the RPC skips the sinks that are already
// closed.
func (i *interceptor) newRPC() *rpcState {
	// Use i.loggers without a copy in the common case where all sinks are
	// open.
	loggers := i.loggers
	copied := false
	for n, l := range i.loggers {
		ok := l.sink.TryIncrement()
		switch {
		case ok && copied:
			loggers = append(loggers, l)
		case !ok && !copied:
			loggers = append([]activeLogger(nil), i.loggers[:n]...)
			copied = true
		}
	}
	r := &rpcState{interceptor: i, loggers: loggers, start: timeNow()}
	if !i.usage.bytes {
		r.bytesUnknown.Store(true)
	}
	return r
}

// NewStream implements httpfilter.ClientInterceptor.
func (i *interceptor) NewStream(ctx context.Context, ri iresolver.RPCInfo, newStream func(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStream, error), opts ...grpc.CallOption) (grpc.ClientStream, error) {
	r := i.newRPC()
	if len(i.usage.headers.request) > 0 {
		md, _ := metadata.FromOutgoingContext(ctx)
		r.requestHeaders = captureHeaders(md, i.usage.headers.request)
		addPseudoHeader(r.requestHeaders, i.usage.headers.request, ":path", ri.Method)
		addPseudoHeader(r.requestHeaders, i.usage.headers.request, ":authority", ri.Authority)
		addPseudoHeader(r.requestHeaders, i.usage.headers.request, ":method", "POST")
	}

	// The Header and Trailer call options run before OnFinish, on the same
	// goroutine, so the callback reads the final metadata.
	var header, trailer metadata.MD
	newOpts := make([]grpc.CallOption, 0, len(opts)+3)
	newOpts = append(newOpts, opts...)
	newOpts = append(newOpts, grpc.Header(&header), grpc.Trailer(&trailer), grpc.OnFinish(func(err error) {
		r.onClientFinish(err, header, trailer)
	}))
	cs, err := newStream(ctx, newOpts...)
	if err != nil {
		// OnFinish does not run if a later filter fails before the stream
		// is created. finish runs only once.
		r.finish(err, nil, nil)
		return nil, err
	}
	if !i.usage.bytes {
		return cs, nil
	}
	return &clientStream{ClientStream: cs, rpc: r}, nil
}

// InterceptRPC implements httpfilter.ServerInterceptor.
func (i *interceptor) InterceptRPC(ss grpc.ServerStream) (grpc.ServerStream, error) {
	ctx := ss.Context()
	r := i.newRPC()
	if len(i.usage.headers.request) > 0 {
		md, _ := metadata.FromIncomingContext(ctx)
		r.requestHeaders = captureHeaders(md, i.usage.headers.request)
		if method, ok := grpc.MethodFromServerStream(ss); ok {
			addPseudoHeader(r.requestHeaders, i.usage.headers.request, ":path", method)
		}
		addPseudoHeader(r.requestHeaders, i.usage.headers.request, ":method", "POST")
	}
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		r.remoteAddress = p.Addr.String()
	}
	if !serverfinish.Register(ctx, func(info *serverfinish.Info) {
		r.finish(info.Err, info.Header, info.Trailer)
	}) {
		// The server did not attach a finish registry. This does not happen
		// for xDS servers.
		r.release()
		return ss, nil
	}
	if !i.usage.bytes {
		return ss, nil
	}
	return &serverStream{ServerStream: ss, rpc: r}, nil
}

// rpcState is the per-RPC state of the access log filter.
type rpcState struct {
	interceptor *interceptor
	// loggers are the loggers of interceptor whose sinks the RPC holds a
	// reference to.
	loggers        []activeLogger
	start          time.Time
	requestHeaders map[string]string
	remoteAddress  string

	bytesSent     atomic.Int64
	bytesReceived atomic.Int64
	// bytesUnknown is set if a message is not a proto message, or if no
	// template reads the byte counts.
	bytesUnknown atomic.Bool

	once sync.Once

	// mu guards recvActive and pendingFinish. They are used only on the
	// client, when the byte counts are tracked.
	mu sync.Mutex
	// recvActive is true while RecvMsg runs.
	recvActive bool
	// pendingFinish is the finish call deferred until RecvMsg returns.
	pendingFinish func()
}

// onClientFinish is the OnFinish callback of a client RPC. The RPC can finish
// inside RecvMsg, before the wrapper counts the received message. In that
// case, the record is written when RecvMsg returns.
func (r *rpcState) onClientFinish(err error, header, trailer metadata.MD) {
	r.mu.Lock()
	if r.recvActive {
		r.pendingFinish = func() { r.finish(err, header, trailer) }
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	r.finish(err, header, trailer)
}

func (r *rpcState) startRecv() {
	r.mu.Lock()
	r.recvActive = true
	r.mu.Unlock()
}

func (r *rpcState) endRecv() {
	r.mu.Lock()
	r.recvActive = false
	pending := r.pendingFinish
	r.pendingFinish = nil
	r.mu.Unlock()
	if pending != nil {
		pending()
	}
}

func (r *rpcState) countSent(m any) {
	if pm, ok := m.(proto.Message); ok {
		r.bytesSent.Add(int64(proto.Size(pm)))
		return
	}
	r.bytesUnknown.Store(true)
}

func (r *rpcState) countReceived(m any) {
	if pm, ok := m.(proto.Message); ok {
		r.bytesReceived.Add(int64(proto.Size(pm)))
		return
	}
	r.bytesUnknown.Store(true)
}

// finish writes one record to each sink and releases the sinks. Only the
// first call has an effect.
func (r *rpcState) finish(err error, header, trailer metadata.MD) {
	r.once.Do(func() {
		defer r.releaseSinks()
		i := r.interceptor
		d := &CallData{
			StartTime:               r.start,
			Duration:                timeNow().Sub(r.start),
			RequestHeaders:          r.requestHeaders,
			ResponseHeaders:         captureHeaders(header, i.usage.headers.response),
			ResponseTrailers:        captureHeaders(trailer, i.usage.headers.trailer),
			StatusCode:              statusCode(err),
			HasStatus:               true,
			BytesSent:               -1,
			BytesReceived:           -1,
			DownstreamRemoteAddress: r.remoteAddress,
		}
		if !r.bytesUnknown.Load() {
			d.BytesSent = r.bytesSent.Load()
			d.BytesReceived = r.bytesReceived.Load()
		}
		for _, l := range r.loggers {
			rec := Record{Timestamp: r.start}
			if l.config.body != nil {
				body := l.config.body.evaluate(d)
				rec.Body = &body
			}
			if len(l.config.attributes) > 0 {
				rec.Attributes = evaluateKeyValues(l.config.attributes, d)
			}
			l.sink.Value().Log(rec)
		}
	})
}

// release releases the sinks without writing a record.
func (r *rpcState) release() {
	r.once.Do(r.releaseSinks)
}

func (r *rpcState) releaseSinks() {
	for _, l := range r.loggers {
		l.sink.Decrement()
	}
}

// statusCode returns the status code of an RPC that ended with err.
func statusCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	if st, ok := status.FromError(err); ok {
		return st.Code()
	}
	return status.FromContextError(err).Code()
}

// captureHeaders returns the values of the headers in names. Multiple values
// are joined with ",". Binary header values are base64 encoded, as on the
// wire.
func captureHeaders(md metadata.MD, names map[string]bool) map[string]string {
	if len(names) == 0 {
		return nil
	}
	out := make(map[string]string, len(names))
	for name := range names {
		vs := md[name]
		if len(vs) == 0 {
			continue
		}
		if strings.HasSuffix(name, "-bin") {
			encoded := make([]string, len(vs))
			for i, v := range vs {
				encoded[i] = base64.RawStdEncoding.EncodeToString([]byte(v))
			}
			vs = encoded
		}
		out[name] = strings.Join(vs, ",")
	}
	return out
}

// addPseudoHeader adds a header that is not in the metadata, if a template
// reads it and the value is known.
func addPseudoHeader(headers map[string]string, names map[string]bool, name, value string) {
	if names[name] && value != "" {
		headers[name] = value
	}
}

// clientStream counts the message payload bytes of a client RPC.
type clientStream struct {
	grpc.ClientStream
	rpc *rpcState
}

func (s *clientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err == nil {
		s.rpc.countSent(m)
	}
	return err
}

func (s *clientStream) RecvMsg(m any) error {
	s.rpc.startRecv()
	err := s.ClientStream.RecvMsg(m)
	if err == nil {
		s.rpc.countReceived(m)
	}
	s.rpc.endRecv()
	return err
}

// serverStream counts the message payload bytes of a server RPC.
type serverStream struct {
	grpc.ServerStream
	rpc *rpcState
}

func (s *serverStream) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil {
		s.rpc.countSent(m)
	}
	return err
}

func (s *serverStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.rpc.countReceived(m)
	}
	return err
}
