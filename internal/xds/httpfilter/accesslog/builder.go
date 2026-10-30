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

	"google.golang.org/grpc/internal/xds/httpfilter"
	"google.golang.org/protobuf/proto"
)

// Builder returns the HTTP filter builder of the access log filter. The
// builder is not registered in the HTTP filter registry, so an http_filters
// entry cannot select it. The xDS resource parser adds it as the first filter,
// so that it sees the statuses of all later filters.
func Builder() httpfilter.Builder {
	return builder{}
}

type builder struct{}

var (
	_ httpfilter.ClientFilterBuilder = builder{}
	_ httpfilter.ServerFilterBuilder = builder{}
)

func (builder) TypeURLs() []string {
	return []string{TypeURL}
}

func (builder) ParseFilterConfig(proto.Message, httpfilter.ParseOptions) (httpfilter.FilterConfig, error) {
	return nil, errors.New("accesslog: access loggers are not HTTP filters")
}

func (builder) ParseFilterConfigOverride(proto.Message, httpfilter.ParseOptions) (httpfilter.FilterConfig, error) {
	return nil, errors.New("accesslog: access loggers do not support config overrides")
}

func (builder) IsTerminal() bool {
	return false
}

func (builder) BuildClientFilter(httpfilter.ClientFilterOptions) httpfilter.ClientFilter {
	return filter{}
}

func (builder) BuildServerFilter() httpfilter.ServerFilter {
	return filter{}
}

// filter builds no interceptors yet.
type filter struct{}

func (filter) BuildClientInterceptor(_, _ httpfilter.FilterConfig) (httpfilter.ClientInterceptor, error) {
	return nil, nil
}

func (filter) BuildServerInterceptor(_, _ httpfilter.FilterConfig) (httpfilter.ServerInterceptor, error) {
	return nil, nil
}

func (filter) Close() {}
