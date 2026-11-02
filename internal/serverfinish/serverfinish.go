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

// Package serverfinish lets server-side components observe the end of an RPC.
//
// It is the server-side counterpart of grpc.OnFinish. The gRPC server attaches
// a callback registry to the context of each RPC when xDS HTTP filters are in
// use. Components that run before the method handler (such as xDS HTTP filter
// interceptors) can then register callbacks that run once, after the server
// writes the final status of the RPC.
package serverfinish

import (
	"context"
	"sync"

	"google.golang.org/grpc/metadata"
)

// Info describes how an RPC ended.
type Info struct {
	// Err is the final status of the RPC as an error created by the status
	// package. It is nil if the RPC completed with an OK status.
	Err error
	// Header is the response header metadata that the server sent or would
	// have sent.
	Header metadata.MD
	// Trailer is the response trailer metadata that the server sent with the
	// status.
	Trailer metadata.MD
}

// Registry holds the callbacks for one RPC. The zero value is not usable;
// create one with NewContext.
type Registry struct {
	mu   sync.Mutex
	done bool
	fns  []func(*Info)
}

type registryKey struct{}

// NewContext returns a child of ctx that carries a new Registry, and the
// Registry itself. The caller runs the callbacks with Registry.Run.
func NewContext(ctx context.Context) (context.Context, *Registry) {
	r := &Registry{}
	return context.WithValue(ctx, registryKey{}, r), r
}

// Register adds f to the Registry in ctx. f is called exactly once, after the
// server writes the final status of the RPC. f must not block.
//
// Register returns false and does not keep f if ctx has no Registry, or if the
// callbacks already ran.
func Register(ctx context.Context, f func(*Info)) bool {
	r, ok := ctx.Value(registryKey{}).(*Registry)
	if !ok {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return false
	}
	r.fns = append(r.fns, f)
	return true
}

// Run calls the registered callbacks in the order of registration. Only the
// first call to Run has an effect. info must not be modified by callbacks.
func (r *Registry) Run(info *Info) {
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return
	}
	r.done = true
	fns := r.fns
	r.fns = nil
	r.mu.Unlock()
	for _, f := range fns {
		f(info)
	}
}
