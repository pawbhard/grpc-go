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

package serverfinish

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/grpctest"
	"google.golang.org/grpc/status"
)

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

const defaultTestTimeout = 10 * time.Second

func (s) TestRegisterWithoutRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if Register(ctx, func(*Info) {}) {
		t.Fatal("Register() = true for a context without a Registry, want false")
	}
}

func (s) TestRunOrderAndOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	ctx, r := NewContext(ctx)
	var got []int
	for i := range 3 {
		if !Register(ctx, func(*Info) { got = append(got, i) }) {
			t.Fatalf("Register(%d) = false, want true", i)
		}
	}
	wantErr := status.Error(codes.Aborted, "aborted")
	var gotErr error
	Register(ctx, func(info *Info) { gotErr = info.Err })

	r.Run(&Info{Err: wantErr})
	r.Run(&Info{})

	if len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Errorf("Callbacks ran in order %v, want [0 1 2]", got)
	}
	if gotErr != wantErr {
		t.Errorf("Info.Err = %v, want %v", gotErr, wantErr)
	}
	if Register(ctx, func(*Info) {}) {
		t.Error("Register() after Run() = true, want false")
	}
}
