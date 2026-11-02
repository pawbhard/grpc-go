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
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
)

// defaultStartTimeFormat is the strftime format of %START_TIME% when no
// format is given. It is RFC 3339 with milliseconds, in UTC.
const defaultStartTimeFormat = "%Y-%m-%dT%H:%M:%E3SZ"

// unavailableValue is written for an operator whose value is not available.
const unavailableValue = "-"

// CallData is the per-RPC data that substitution operators read.
type CallData struct {
	// StartTime is the time at which the RPC started. The zero value means
	// unknown.
	StartTime time.Time
	// Duration is the time from the start of the RPC to the time of the log.
	Duration time.Duration
	// RequestHeaders, ResponseHeaders, and ResponseTrailers contain only the
	// headers that some template references. Keys are lower case.
	RequestHeaders   map[string]string
	ResponseHeaders  map[string]string
	ResponseTrailers map[string]string
	// StatusCode is the final status of the RPC. It is valid only if
	// HasStatus is true.
	StatusCode codes.Code
	HasStatus  bool
	// BytesReceived and BytesSent are the message payload sizes. A negative
	// value means unknown.
	BytesReceived int64
	BytesSent     int64
	// DownstreamRemoteAddress is the address of the peer on the server side.
	// The empty string means unknown.
	DownstreamRemoteAddress string
}

type operator int

const (
	opLiteral operator = iota
	opStartTime
	opDuration
	opRequestHeader
	opResponseHeader
	opResponseTrailer
	opGRPCStatus
	opBytesReceived
	opBytesSent
	opDownstreamRemoteAddress
	opProtocol
)

type statusFormat int

const (
	statusCamelString statusFormat = iota
	statusSnakeString
	statusNumber
)

type segment struct {
	op operator
	// text is the literal text or the primary header name.
	text string
	// alternative is the alternative header name. Empty if not set.
	alternative string
	// timeFormat is the compiled format of %START_TIME%.
	timeFormat   *strftimeFormat
	statusFormat statusFormat
	// maxLength truncates the value to at most this many bytes. A negative
	// value means no limit.
	maxLength int
}

// Formatter is a compiled Envoy-style substitution format string, such as
// "%REQ(:PATH)% %GRPC_STATUS% %DURATION%".
//
// Supported syntax:
//   - "%%" is a literal "%".
//   - "%OP%", "%OP(arg)%", and "%OP(arg):N%". N truncates the value to at most
//     N bytes.
//
// Supported operators:
//   - START_TIME[(fmt)]: start time of the RPC, in UTC. The default format is
//     "%Y-%m-%dT%H:%M:%E3SZ". "%f" and "%<N>f" give fractional seconds.
//   - DURATION: duration of the RPC in milliseconds.
//   - REQ(h[?alt]): request header value.
//   - RESP(h[?alt]): response header value.
//   - TRAILER(h[?alt]): response trailer value.
//   - GRPC_STATUS[(CAMEL_STRING|SNAKE_STRING|NUMBER)]: final gRPC status.
//   - GRPC_STATUS_NUMBER: final gRPC status as a number.
//   - BYTES_RECEIVED: received message payload bytes.
//   - BYTES_SENT: sent message payload bytes.
//   - DOWNSTREAM_REMOTE_ADDRESS: the peer address (server side only).
//   - PROTOCOL: always "HTTP/2".
//
// A value that is not available is written as "-".
type Formatter struct {
	format   string
	segments []segment
}

// ParseFormat compiles format. It returns an error if format is invalid or
// uses an unsupported operator.
func ParseFormat(format string) (*Formatter, error) {
	f := &Formatter{format: format}
	var literal strings.Builder
	flushLiteral := func() {
		if literal.Len() == 0 {
			return
		}
		f.segments = append(f.segments, segment{op: opLiteral, text: literal.String(), maxLength: -1})
		literal.Reset()
	}
	pos := 0
	for pos < len(format) {
		c := format[pos]
		if c != '%' {
			literal.WriteByte(c)
			pos++
			continue
		}
		// "%%" is an escaped "%".
		if pos+1 < len(format) && format[pos+1] == '%' {
			literal.WriteByte('%')
			pos += 2
			continue
		}
		opStart := pos
		pos++
		nameStart := pos
		for pos < len(format) && isOperatorNameChar(format[pos]) {
			pos++
		}
		name := format[nameStart:pos]
		if name == "" {
			return nil, fmt.Errorf("invalid access log format %q: expected operator name at position %d", format, opStart)
		}
		// Optional "(arg)".
		var arg string
		hasArg := false
		if pos < len(format) && format[pos] == '(' {
			end := strings.IndexByte(format[pos+1:], ')')
			if end < 0 {
				return nil, fmt.Errorf("invalid access log format %q: missing ')' for operator %s", format, name)
			}
			arg = format[pos+1 : pos+1+end]
			hasArg = true
			pos += end + 2
		}
		// Optional ":N".
		maxLength := -1
		if pos < len(format) && format[pos] == ':' {
			pos++
			digitsStart := pos
			for pos < len(format) && format[pos] >= '0' && format[pos] <= '9' {
				pos++
			}
			n, err := strconv.Atoi(format[digitsStart:pos])
			if err != nil {
				return nil, fmt.Errorf("invalid access log format %q: invalid length for operator %s", format, name)
			}
			maxLength = n
		}
		// Closing "%".
		if pos >= len(format) || format[pos] != '%' {
			return nil, fmt.Errorf("invalid access log format %q: missing closing '%%' for operator %s", format, name)
		}
		pos++
		seg, err := parseOperator(name, arg, hasArg)
		if err != nil {
			return nil, fmt.Errorf("invalid access log format %q: %v", format, err)
		}
		seg.maxLength = maxLength
		flushLiteral()
		f.segments = append(f.segments, seg)
	}
	flushLiteral()
	return f, nil
}

func isOperatorNameChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func parseOperator(name, arg string, hasArg bool) (segment, error) {
	var seg segment
	switch name {
	case "START_TIME":
		seg.op = opStartTime
		format := defaultStartTimeFormat
		if arg != "" {
			format = arg
		}
		tf, err := parseStrftime(format)
		if err != nil {
			return segment{}, fmt.Errorf("operator START_TIME: %v", err)
		}
		seg.timeFormat = tf
		return seg, nil
	case "REQ", "RESP", "TRAILER":
		switch name {
		case "REQ":
			seg.op = opRequestHeader
		case "RESP":
			seg.op = opResponseHeader
		default:
			seg.op = opResponseTrailer
		}
		if arg == "" {
			return segment{}, fmt.Errorf("operator %s requires a header", name)
		}
		parts := strings.Split(arg, "?")
		if len(parts) > 2 || parts[0] == "" || (len(parts) == 2 && parts[1] == "") {
			return segment{}, fmt.Errorf("operator %s has invalid header: %q", name, arg)
		}
		seg.text = strings.ToLower(parts[0])
		if len(parts) == 2 {
			seg.alternative = strings.ToLower(parts[1])
		}
		return seg, nil
	case "GRPC_STATUS":
		seg.op = opGRPCStatus
		switch arg {
		case "", "CAMEL_STRING":
			seg.statusFormat = statusCamelString
		case "SNAKE_STRING":
			seg.statusFormat = statusSnakeString
		case "NUMBER":
			seg.statusFormat = statusNumber
		default:
			return segment{}, fmt.Errorf("operator GRPC_STATUS supports only CAMEL_STRING, SNAKE_STRING or NUMBER; got %q", arg)
		}
		return seg, nil
	case "DURATION":
		seg.op = opDuration
	case "GRPC_STATUS_NUMBER":
		seg.op = opGRPCStatus
		seg.statusFormat = statusNumber
	case "BYTES_RECEIVED":
		seg.op = opBytesReceived
	case "BYTES_SENT":
		seg.op = opBytesSent
	case "DOWNSTREAM_REMOTE_ADDRESS":
		seg.op = opDownstreamRemoteAddress
	case "PROTOCOL":
		seg.op = opProtocol
	default:
		return segment{}, fmt.Errorf("unsupported operator %s", name)
	}
	if hasArg {
		return segment{}, fmt.Errorf("operator %s takes no argument", name)
	}
	return seg, nil
}

// String returns the original format string.
func (f *Formatter) String() string {
	return f.format
}

// Format evaluates the template against d.
func (f *Formatter) Format(d *CallData) string {
	var b strings.Builder
	for i := range f.segments {
		seg := &f.segments[i]
		if seg.op == opLiteral {
			b.WriteString(seg.text)
			continue
		}
		v, ok := seg.evaluate(d)
		if !ok {
			b.WriteString(unavailableValue)
			continue
		}
		if seg.maxLength >= 0 && len(v) > seg.maxLength {
			v = v[:seg.maxLength]
		}
		b.WriteString(v)
	}
	return b.String()
}

func (seg *segment) evaluate(d *CallData) (string, bool) {
	switch seg.op {
	case opStartTime:
		if d.StartTime.IsZero() {
			return "", false
		}
		return seg.timeFormat.format(d.StartTime.UTC()), true
	case opDuration:
		return strconv.FormatInt(d.Duration.Milliseconds(), 10), true
	case opRequestHeader:
		return lookupHeader(d.RequestHeaders, seg.text, seg.alternative)
	case opResponseHeader:
		return lookupHeader(d.ResponseHeaders, seg.text, seg.alternative)
	case opResponseTrailer:
		return lookupHeader(d.ResponseTrailers, seg.text, seg.alternative)
	case opGRPCStatus:
		if !d.HasStatus {
			return "", false
		}
		return formatStatus(d.StatusCode, seg.statusFormat), true
	case opBytesReceived:
		if d.BytesReceived < 0 {
			return "", false
		}
		return strconv.FormatInt(d.BytesReceived, 10), true
	case opBytesSent:
		if d.BytesSent < 0 {
			return "", false
		}
		return strconv.FormatInt(d.BytesSent, 10), true
	case opDownstreamRemoteAddress:
		if d.DownstreamRemoteAddress == "" {
			return "", false
		}
		return d.DownstreamRemoteAddress, true
	case opProtocol:
		return "HTTP/2", true
	}
	return "", false
}

func lookupHeader(headers map[string]string, name, alternative string) (string, bool) {
	if v, ok := headers[name]; ok {
		return v, true
	}
	if alternative != "" {
		if v, ok := headers[alternative]; ok {
			return v, true
		}
	}
	return "", false
}

// headerNames is the set of header names that one or more formatters read.
// It lets the filter copy only the headers that it needs.
type headerNames struct {
	request  map[string]bool
	response map[string]bool
	trailer  map[string]bool
}

func newHeaderNames() *headerNames {
	return &headerNames{request: map[string]bool{}, response: map[string]bool{}, trailer: map[string]bool{}}
}

// fieldUsage records which parts of CallData the templates read.
type fieldUsage struct {
	headers *headerNames
	bytes   bool
}

// addUsage adds the header names and fields that f reads to u.
func (f *Formatter) addUsage(u *fieldUsage) {
	for _, seg := range f.segments {
		var target map[string]bool
		switch seg.op {
		case opRequestHeader:
			target = u.headers.request
		case opResponseHeader:
			target = u.headers.response
		case opResponseTrailer:
			target = u.headers.trailer
		case opBytesReceived, opBytesSent:
			u.bytes = true
			continue
		default:
			continue
		}
		target[seg.text] = true
		if seg.alternative != "" {
			target[seg.alternative] = true
		}
	}
}

// statusCamelNames are the names that Envoy's Grpc::Utility::grpcStatusToString
// uses.
var statusCamelNames = map[codes.Code]string{
	codes.OK:                 "OK",
	codes.Canceled:           "Canceled",
	codes.Unknown:            "Unknown",
	codes.InvalidArgument:    "InvalidArgument",
	codes.DeadlineExceeded:   "DeadlineExceeded",
	codes.NotFound:           "NotFound",
	codes.AlreadyExists:      "AlreadyExists",
	codes.PermissionDenied:   "PermissionDenied",
	codes.ResourceExhausted:  "ResourceExhausted",
	codes.FailedPrecondition: "FailedPrecondition",
	codes.Aborted:            "Aborted",
	codes.OutOfRange:         "OutOfRange",
	codes.Unimplemented:      "Unimplemented",
	codes.Internal:           "Internal",
	codes.Unavailable:        "Unavailable",
	codes.DataLoss:           "DataLoss",
	codes.Unauthenticated:    "Unauthenticated",
}

// statusSnakeNames are the canonical upper snake case names of the codes.
var statusSnakeNames = map[codes.Code]string{
	codes.OK:                 "OK",
	codes.Canceled:           "CANCELLED",
	codes.Unknown:            "UNKNOWN",
	codes.InvalidArgument:    "INVALID_ARGUMENT",
	codes.DeadlineExceeded:   "DEADLINE_EXCEEDED",
	codes.NotFound:           "NOT_FOUND",
	codes.AlreadyExists:      "ALREADY_EXISTS",
	codes.PermissionDenied:   "PERMISSION_DENIED",
	codes.ResourceExhausted:  "RESOURCE_EXHAUSTED",
	codes.FailedPrecondition: "FAILED_PRECONDITION",
	codes.Aborted:            "ABORTED",
	codes.OutOfRange:         "OUT_OF_RANGE",
	codes.Unimplemented:      "UNIMPLEMENTED",
	codes.Internal:           "INTERNAL",
	codes.Unavailable:        "UNAVAILABLE",
	codes.DataLoss:           "DATA_LOSS",
	codes.Unauthenticated:    "UNAUTHENTICATED",
}

func formatStatus(c codes.Code, f statusFormat) string {
	var names map[codes.Code]string
	switch f {
	case statusCamelString:
		names = statusCamelNames
	case statusSnakeString:
		names = statusSnakeNames
	}
	if name, ok := names[c]; ok {
		return name
	}
	return strconv.FormatUint(uint64(c), 10)
}
