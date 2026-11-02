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
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/grpctest"
)

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

func testCallData() *CallData {
	return &CallData{
		StartTime:               time.Date(2026, time.March, 4, 5, 6, 7, 123456789, time.UTC),
		Duration:                1500 * time.Millisecond,
		RequestHeaders:          map[string]string{":path": "/pkg.Svc/Method", "x-alt": "alt-value"},
		ResponseHeaders:         map[string]string{"x-resp": "resp-value"},
		ResponseTrailers:        map[string]string{"x-trailer": "trailer-value"},
		StatusCode:              codes.PermissionDenied,
		HasStatus:               true,
		BytesReceived:           12,
		BytesSent:               34,
		DownstreamRemoteAddress: "10.0.0.1:443",
	}
}

func (s) TestFormatterFormat(t *testing.T) {
	tests := []struct {
		format string
		data   *CallData
		want   string
	}{
		{format: "plain text", want: "plain text"},
		{format: "100%% sure", want: "100% sure"},
		{format: "%START_TIME%", want: "2026-03-04T05:06:07.123Z"},
		{format: "%START_TIME()%", want: "2026-03-04T05:06:07.123Z"},
		{format: "%START_TIME(%Y/%m/%d %H:%M:%S)%", want: "2026/03/04 05:06:07"},
		{format: "%START_TIME(%s.%f)%", want: "1772600767.123456789"},
		{format: "%START_TIME(%s.%3f)%", want: "1772600767.123"},
		{format: "%START_TIME(%E6S|%E*S|%E0S)%", want: "07.123456|07.123456789|07"},
		{format: "%START_TIME(%F %T %z %Ez)%", want: "2026-03-04 05:06:07 +0000 +00:00"},
		{format: "%START_TIME(%a %b %e %j %p %I)%", want: "Wed Mar  4 063 AM 05"},
		{format: "%START_TIME%", data: &CallData{}, want: "-"},
		{format: "%DURATION%", want: "1500"},
		{format: "%REQ(:PATH)%", want: "/pkg.Svc/Method"},
		{format: "%REQ(:path)%", want: "/pkg.Svc/Method"},
		{format: "%REQ(X-MISSING?X-ALT)%", want: "alt-value"},
		{format: "%REQ(X-MISSING)%", want: "-"},
		{format: "%RESP(X-RESP)%", want: "resp-value"},
		{format: "%TRAILER(X-TRAILER)%", want: "trailer-value"},
		{format: "%TRAILER(X-RESP)%", want: "-"},
		{format: "%GRPC_STATUS%", want: "PermissionDenied"},
		{format: "%GRPC_STATUS(CAMEL_STRING)%", want: "PermissionDenied"},
		{format: "%GRPC_STATUS(SNAKE_STRING)%", want: "PERMISSION_DENIED"},
		{format: "%GRPC_STATUS(NUMBER)%", want: "7"},
		{format: "%GRPC_STATUS_NUMBER%", want: "7"},
		{format: "%GRPC_STATUS%", data: &CallData{StatusCode: codes.Canceled, HasStatus: true}, want: "Canceled"},
		{format: "%GRPC_STATUS(SNAKE_STRING)%", data: &CallData{StatusCode: codes.Canceled, HasStatus: true}, want: "CANCELLED"},
		{format: "%GRPC_STATUS%", data: &CallData{StatusCode: 42, HasStatus: true}, want: "42"},
		{format: "%GRPC_STATUS%", data: &CallData{}, want: "-"},
		{format: "%BYTES_RECEIVED% %BYTES_SENT%", want: "12 34"},
		{format: "%BYTES_RECEIVED% %BYTES_SENT%", data: &CallData{BytesReceived: -1, BytesSent: -1}, want: "- -"},
		{format: "%DOWNSTREAM_REMOTE_ADDRESS%", want: "10.0.0.1:443"},
		{format: "%DOWNSTREAM_REMOTE_ADDRESS%", data: &CallData{}, want: "-"},
		{format: "%PROTOCOL%", want: "HTTP/2"},
		{format: "%REQ(:PATH):4%", want: "/pkg"},
		{format: "%REQ(:PATH):0%", want: ""},
		{format: "%PROTOCOL:100%", want: "HTTP/2"},
		{
			format: "[%START_TIME%] \"%REQ(:PATH)% %PROTOCOL%\" %GRPC_STATUS% %DURATION%ms",
			want:   "[2026-03-04T05:06:07.123Z] \"/pkg.Svc/Method HTTP/2\" PermissionDenied 1500ms",
		},
	}
	for _, test := range tests {
		t.Run(test.format, func(t *testing.T) {
			f, err := ParseFormat(test.format)
			if err != nil {
				t.Fatalf("ParseFormat(%q) failed: %v", test.format, err)
			}
			data := test.data
			if data == nil {
				data = testCallData()
			}
			if got := f.Format(data); got != test.want {
				t.Errorf("Format() = %q, want %q", got, test.want)
			}
			if got := f.String(); got != test.format {
				t.Errorf("String() = %q, want %q", got, test.format)
			}
		})
	}
}

func (s) TestParseFormatErrors(t *testing.T) {
	tests := []struct {
		format  string
		wantErr string
	}{
		{format: "%", wantErr: "expected operator name"},
		{format: "%lower%", wantErr: "expected operator name"},
		{format: "%REQ(:PATH%", wantErr: "missing ')'"},
		{format: "%DURATION", wantErr: "missing closing '%'"},
		{format: "%DURATION:%", wantErr: "invalid length"},
		{format: "%DURATION:x%", wantErr: "invalid length"},
		{format: "%UNKNOWN_OP%", wantErr: "unsupported operator UNKNOWN_OP"},
		{format: "%UPSTREAM_HOST%", wantErr: "unsupported operator"},
		{format: "%DURATION(x)%", wantErr: "takes no argument"},
		{format: "%PROTOCOL()%", wantErr: "takes no argument"},
		{format: "%REQ%", wantErr: "requires a header"},
		{format: "%REQ()%", wantErr: "requires a header"},
		{format: "%REQ(a?b?c)%", wantErr: "invalid header"},
		{format: "%REQ(?b)%", wantErr: "invalid header"},
		{format: "%REQ(a?)%", wantErr: "invalid header"},
		{format: "%GRPC_STATUS(LOWER)%", wantErr: "supports only"},
		{format: "%START_TIME(%Q)%", wantErr: "unsupported specifier %Q"},
		{format: "%START_TIME(%)%", wantErr: "ends with"},
		{format: "%START_TIME(%E)%", wantErr: "incomplete specifier"},
		{format: "%START_TIME(%EQS)%", wantErr: "unsupported specifier %EQ"},
		{format: "%START_TIME(%E3Q)%", wantErr: "unsupported specifier %E3Q"},
	}
	for _, test := range tests {
		t.Run(test.format, func(t *testing.T) {
			_, err := ParseFormat(test.format)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ParseFormat(%q) returned error %v, want error containing %q", test.format, err, test.wantErr)
			}
		})
	}
}

func (s) TestFormatterUsage(t *testing.T) {
	u := &fieldUsage{headers: newHeaderNames()}
	for _, format := range []string{"%REQ(:PATH)% %RESP(X-A?X-B)%", "%TRAILER(X-T)% %REQ(Y)%"} {
		f, err := ParseFormat(format)
		if err != nil {
			t.Fatalf("ParseFormat(%q) failed: %v", format, err)
		}
		f.addUsage(u)
	}
	if u.bytes {
		t.Errorf("fieldUsage.bytes = true, want false")
	}
	want := &headerNames{
		request:  map[string]bool{":path": true, "y": true},
		response: map[string]bool{"x-a": true, "x-b": true},
		trailer:  map[string]bool{"x-t": true},
	}
	if diff := cmp.Diff(want, u.headers, cmp.AllowUnexported(headerNames{})); diff != "" {
		t.Errorf("Header names mismatch (-want +got):\n%s", diff)
	}

	f, err := ParseFormat("%BYTES_SENT%")
	if err != nil {
		t.Fatalf("ParseFormat() failed: %v", err)
	}
	f.addUsage(u)
	if !u.bytes {
		t.Errorf("fieldUsage.bytes = false after %%BYTES_SENT%%, want true")
	}
}
