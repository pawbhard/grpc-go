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

// Package accesslog implements xDS access logging for gRPC clients and
// servers.
//
// The package parses the OpenTelemetryAccessLogConfig entries of the
// HttpConnectionManager access_log field, and provides an HTTP filter that
// writes one record per RPC to a Sink. The filter is not registered in the
// HTTP filter registry: the xDS resource parser adds it as the first filter of
// the chain.
//
// This package has no OpenTelemetry dependency. An OpenTelemetry Logs SDK
// sink is registered by the
// google.golang.org/grpc/stats/opentelemetry/xdsaccesslog package.
package accesslog
