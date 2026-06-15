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

package spark

// This file provides an in-process fake Spark Connect gRPC server used by the
// hermetic tests in this package. It lets the real driver and the real Spark
// Connect transport run end to end over a loopback gRPC connection with no live
// Spark cluster, JVM, or Docker required, so the full request/response and Arrow
// decoding paths are exercised in a plain `go test`.

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	connect "github.com/HyukjinKwon/adbc-driver-spark/internal/sparkconnect/proto/spark/connect"
)

// queryResult is what the fake server returns for a single ExecutePlan request.
// When grpcErr is set it is returned as the RPC error; otherwise the schema and
// records are streamed back as a single Arrow IPC batch.
type queryResult struct {
	schema  *arrow.Schema
	records []arrow.Record
	grpcErr error
}

// capturedCall records an ExecutePlan request for later assertions.
type capturedCall struct {
	query   string
	posArgs []*connect.Expression_Literal
}

// fakeServer is a minimal Spark Connect service backed by a routing function.
type fakeServer struct {
	connect.UnimplementedSparkConnectServiceServer

	alloc memory.Allocator
	route func(query string) queryResult

	// sparkVersion is returned from AnalyzePlan(SparkVersion); when empty the
	// fake answers with an empty version string.
	sparkVersion string

	mu       sync.Mutex
	calls    []capturedCall
	conf     map[string]string
	lastMeta metadata.MD
}

func newFakeServer(alloc memory.Allocator, route func(query string) queryResult) *fakeServer {
	return &fakeServer{alloc: alloc, route: route, conf: map[string]string{}}
}

func (f *fakeServer) recordCall(c capturedCall) {
	f.mu.Lock()
	f.calls = append(f.calls, c)
	f.mu.Unlock()
}

// lastCall returns the most recent ExecutePlan request seen by the server.
func (f *fakeServer) lastCall() (capturedCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return capturedCall{}, false
	}
	return f.calls[len(f.calls)-1], true
}

// headerValue returns the first value of an incoming gRPC metadata header seen
// on the most recent ExecutePlan request, or "" if absent.
func (f *fakeServer) headerValue(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	vals := f.lastMeta.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func (f *fakeServer) ExecutePlan(req *connect.ExecutePlanRequest, stream connect.SparkConnectService_ExecutePlanServer) error {
	sql := req.GetPlan().GetRoot().GetSql()
	query := sql.GetQuery()
	f.recordCall(capturedCall{query: query, posArgs: sql.GetPosArgs()})
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		f.mu.Lock()
		f.lastMeta = md.Copy()
		f.mu.Unlock()
	}

	res := queryResult{}
	if f.route != nil {
		res = f.route(query)
	}
	if res.grpcErr != nil {
		return res.grpcErr
	}

	sessionID := req.GetSessionId()
	if res.schema != nil {
		// Emit each record as its own Arrow IPC batch (separate ArrowBatch
		// responses) so the transport's cross-batch concatenation is exercised.
		// With no records, emit a single schema-only batch (as Spark does for
		// empty / LIMIT 0 results).
		batches := make([][]arrow.Record, 0, len(res.records))
		if len(res.records) == 0 {
			batches = append(batches, nil)
		}
		for _, rec := range res.records {
			batches = append(batches, []arrow.Record{rec})
		}
		for _, recs := range batches {
			data, err := encodeArrowIPC(f.alloc, res.schema, recs)
			if err != nil {
				return status.Errorf(codes.Internal, "fake server: encode arrow: %v", err)
			}
			var rows int64
			for _, r := range recs {
				rows += r.NumRows()
			}
			if err := stream.Send(&connect.ExecutePlanResponse{
				SessionId:           sessionID,
				ServerSideSessionId: "fake-server-session",
				ResponseType: &connect.ExecutePlanResponse_ArrowBatch_{
					ArrowBatch: &connect.ExecutePlanResponse_ArrowBatch{RowCount: rows, Data: data},
				},
			}); err != nil {
				return err
			}
		}
	}

	return stream.Send(&connect.ExecutePlanResponse{
		SessionId:           sessionID,
		ServerSideSessionId: "fake-server-session",
		ResponseType: &connect.ExecutePlanResponse_ResultComplete_{
			ResultComplete: &connect.ExecutePlanResponse_ResultComplete{},
		},
	})
}

func (f *fakeServer) Config(_ context.Context, req *connect.ConfigRequest) (*connect.ConfigResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	resp := &connect.ConfigResponse{
		SessionId:           req.GetSessionId(),
		ServerSideSessionId: "fake-server-session",
	}
	switch op := req.GetOperation().GetOpType().(type) {
	case *connect.ConfigRequest_Operation_Set:
		for _, kv := range op.Set.GetPairs() {
			f.conf[kv.GetKey()] = kv.GetValue()
		}
	case *connect.ConfigRequest_Operation_Get:
		for _, k := range op.Get.GetKeys() {
			v := f.conf[k]
			val := v
			resp.Pairs = append(resp.Pairs, &connect.KeyValue{Key: k, Value: &val})
		}
	case *connect.ConfigRequest_Operation_GetOption:
		for _, k := range op.GetOption.GetKeys() {
			if v, ok := f.conf[k]; ok {
				val := v
				resp.Pairs = append(resp.Pairs, &connect.KeyValue{Key: k, Value: &val})
			}
		}
	}
	return resp, nil
}

// AnalyzePlan answers the SparkVersion analyze request used by GetInfo to
// report the vendor (Spark server) version. Only the SparkVersion analysis is
// implemented; that is the single analyze the driver issues.
func (f *fakeServer) AnalyzePlan(_ context.Context, req *connect.AnalyzePlanRequest) (*connect.AnalyzePlanResponse, error) {
	resp := &connect.AnalyzePlanResponse{
		SessionId:           req.GetSessionId(),
		ServerSideSessionId: "fake-server-session",
	}
	if req.GetSparkVersion() != nil {
		resp.Result = &connect.AnalyzePlanResponse_SparkVersion_{
			SparkVersion: &connect.AnalyzePlanResponse_SparkVersion{Version: f.sparkVersion},
		}
	}
	return resp, nil
}

func (f *fakeServer) ReleaseSession(_ context.Context, req *connect.ReleaseSessionRequest) (*connect.ReleaseSessionResponse, error) {
	return &connect.ReleaseSessionResponse{SessionId: req.GetSessionId()}, nil
}

// startFakeServer starts srv on a loopback port and returns the sc:// URI that
// addresses it. The server is stopped automatically when the test finishes.
func startFakeServer(t *testing.T, srv *fakeServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	connect.RegisterSparkConnectServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	return "sc://" + lis.Addr().String()
}

// encodeArrowIPC serializes schema and records to an Arrow IPC stream. When no
// records are supplied a single zero-row record is written so that the stream
// still carries the schema (as Spark does for empty / LIMIT 0 results).
func encodeArrowIPC(alloc memory.Allocator, schema *arrow.Schema, records []arrow.Record) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if len(records) == 0 {
		b := array.NewRecordBuilder(alloc, schema)
		empty := b.NewRecord()
		err := w.Write(empty)
		empty.Release()
		b.Release()
		if err != nil {
			return nil, err
		}
	}
	for _, rec := range records {
		if err := w.Write(rec); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// --- small builders used by the hermetic tests ---

// stringColumnResult builds a one-column string result with the given column
// name and values, suitable for emulating SHOW CATALOGS / NAMESPACES / TABLES.
func stringColumnResult(alloc memory.Allocator, column string, values ...string) queryResult {
	schema := arrow.NewSchema([]arrow.Field{{Name: column, Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	sb := b.Field(0).(*array.StringBuilder)
	for _, v := range values {
		sb.Append(v)
	}
	rec := b.NewRecord()
	return queryResult{schema: schema, records: []arrow.Record{rec}}
}

// schemaOnlyResult returns a result that carries schema but no rows. Used to
// emulate DESCRIBE / "SELECT * ... LIMIT 0" schema probes.
func schemaOnlyResult(schema *arrow.Schema) queryResult {
	return queryResult{schema: schema}
}

func containsFold(haystack, needle string) bool {
	return strings.Contains(strings.ToUpper(haystack), strings.ToUpper(needle))
}
