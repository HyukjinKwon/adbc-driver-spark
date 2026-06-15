// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sparkconnect

import (
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestWrapGRPCNil verifies a nil error passes through unchanged.
func TestWrapGRPCNil(t *testing.T) {
	if got := wrapGRPC(nil); got != nil {
		t.Errorf("wrapGRPC(nil) = %v, want nil", got)
	}
}

// TestWrapGRPCNonStatusError verifies a plain (non-gRPC-status) error is mapped
// to an Internal adbc.Error preserving the message.
func TestWrapGRPCNonStatusError(t *testing.T) {
	err := wrapGRPC(errors.New("boom"))
	var ae adbc.Error
	if !errors.As(err, &ae) {
		t.Fatalf("expected adbc.Error, got %T", err)
	}
	if ae.Code != adbc.StatusInternal {
		t.Errorf("Code = %v, want StatusInternal", ae.Code)
	}
	if ae.Msg != "boom" {
		t.Errorf("Msg = %q, want %q", ae.Msg, "boom")
	}
}

// TestWrapGRPCStatusMapping verifies each gRPC status code maps to the expected
// adbc.Status, and that the message and vendor code are carried through.
func TestWrapGRPCStatusMapping(t *testing.T) {
	tests := []struct {
		name     string
		code     codes.Code
		wantADBC adbc.Status
	}{
		{"canceled", codes.Canceled, adbc.StatusCancelled},
		{"invalid argument", codes.InvalidArgument, adbc.StatusInvalidArgument},
		{"deadline exceeded", codes.DeadlineExceeded, adbc.StatusTimeout},
		{"not found", codes.NotFound, adbc.StatusNotFound},
		{"already exists", codes.AlreadyExists, adbc.StatusAlreadyExists},
		{"permission denied", codes.PermissionDenied, adbc.StatusUnauthorized},
		{"unauthenticated", codes.Unauthenticated, adbc.StatusUnauthorized},
		{"unimplemented", codes.Unimplemented, adbc.StatusNotImplemented},
		{"unavailable", codes.Unavailable, adbc.StatusIO},
		{"internal", codes.Internal, adbc.StatusInternal},
		{"unknown maps to internal", codes.Unknown, adbc.StatusInternal},
		{"resource exhausted maps to internal", codes.ResourceExhausted, adbc.StatusInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := "grpc failure: " + tt.name
			in := status.Error(tt.code, msg)
			out := wrapGRPC(in)

			var ae adbc.Error
			if !errors.As(out, &ae) {
				t.Fatalf("expected adbc.Error, got %T", out)
			}
			if ae.Code != tt.wantADBC {
				t.Errorf("code %v: got adbc status %v, want %v", tt.code, ae.Code, tt.wantADBC)
			}
			if ae.Msg != msg {
				t.Errorf("Msg = %q, want %q", ae.Msg, msg)
			}
			if ae.VendorCode != int32(tt.code) {
				t.Errorf("VendorCode = %d, want %d", ae.VendorCode, int32(tt.code))
			}
		})
	}
}

// TestGrpcToADBCDirect exercises the grpcToADBC mapping function directly,
// including the default branch for an out-of-range code.
func TestGrpcToADBCDirect(t *testing.T) {
	if got := grpcToADBC(codes.OK); got != adbc.StatusOK {
		t.Errorf("OK -> %v, want StatusOK", got)
	}
	if got := grpcToADBC(codes.Aborted); got != adbc.StatusInternal {
		t.Errorf("Aborted -> %v, want StatusInternal", got)
	}
	if got := grpcToADBC(codes.Code(9999)); got != adbc.StatusInternal {
		t.Errorf("unknown code -> %v, want StatusInternal", got)
	}
}
