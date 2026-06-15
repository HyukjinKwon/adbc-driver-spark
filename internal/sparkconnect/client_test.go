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

// This file drives the real Spark Connect client against an in-process insecure
// gRPC server implementing connect.SparkConnectServiceServer. It exercises the
// gRPC request/response plumbing and the Arrow IPC result reader end to end with
// no live Spark cluster, JVM, or Docker required, so the full client path runs
// in a plain `go test`.

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
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

// clientResult is what the test server returns for an ExecutePlan request. When
// grpcErr is set it is returned as the RPC error; otherwise the schema and the
// records (one Arrow IPC stream each) are streamed back.
type clientResult struct {
	schema  *arrow.Schema
	records []arrow.Record
	grpcErr error
	// trailingErr, when set, is returned after the records have been streamed
	// (before ResultComplete), simulating a server failure mid-stream.
	trailingErr error
}

// testServer is a Spark Connect service backed by a routing function for
// ExecutePlan plus canned responses for the unary RPCs.
type testServer struct {
	connect.UnimplementedSparkConnectServiceServer

	alloc memory.Allocator
	route func(query string) clientResult

	sparkVersion string
	confErr      error
	interruptErr error

	mu             sync.Mutex
	execQueries    []string
	execPosArgs    [][]*connect.Expression_Literal
	conf           map[string]string
	lastExecMeta   metadata.MD
	lastUserCtx    *connect.UserContext
	lastClientType string
	interrupted    bool
	released       bool
}

func newTestServer(alloc memory.Allocator, route func(query string) clientResult) *testServer {
	return &testServer{alloc: alloc, route: route, conf: map[string]string{}}
}

func (s *testServer) ExecutePlan(req *connect.ExecutePlanRequest, stream connect.SparkConnectService_ExecutePlanServer) error {
	sql := req.GetPlan().GetRoot().GetSql()
	query := sql.GetQuery()

	s.mu.Lock()
	s.execQueries = append(s.execQueries, query)
	s.execPosArgs = append(s.execPosArgs, sql.GetPosArgs())
	s.lastUserCtx = req.GetUserContext()
	s.lastClientType = req.GetClientType()
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.lastExecMeta = md.Copy()
	}
	s.mu.Unlock()

	res := clientResult{}
	if s.route != nil {
		res = s.route(query)
	}
	if res.grpcErr != nil {
		return res.grpcErr
	}

	sessionID := req.GetSessionId()
	if res.schema != nil {
		batches := make([][]arrow.Record, 0, len(res.records))
		if len(res.records) == 0 {
			batches = append(batches, nil)
		}
		for _, rec := range res.records {
			batches = append(batches, []arrow.Record{rec})
		}
		for _, recs := range batches {
			data, err := encodeStream(s.alloc, res.schema, recs)
			if err != nil {
				return status.Errorf(codes.Internal, "test server: encode arrow: %v", err)
			}
			var rows int64
			for _, r := range recs {
				rows += r.NumRows()
			}
			if err := stream.Send(&connect.ExecutePlanResponse{
				SessionId:           sessionID,
				ServerSideSessionId: "test-server-session",
				ResponseType: &connect.ExecutePlanResponse_ArrowBatch_{
					ArrowBatch: &connect.ExecutePlanResponse_ArrowBatch{RowCount: rows, Data: data},
				},
			}); err != nil {
				return err
			}
		}
	}

	if res.trailingErr != nil {
		return res.trailingErr
	}

	return stream.Send(&connect.ExecutePlanResponse{
		SessionId:           sessionID,
		ServerSideSessionId: "test-server-session",
		ResponseType: &connect.ExecutePlanResponse_ResultComplete_{
			ResultComplete: &connect.ExecutePlanResponse_ResultComplete{},
		},
	})
}

func (s *testServer) Config(_ context.Context, req *connect.ConfigRequest) (*connect.ConfigResponse, error) {
	if s.confErr != nil {
		return nil, s.confErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	resp := &connect.ConfigResponse{
		SessionId:           req.GetSessionId(),
		ServerSideSessionId: "test-server-session",
	}
	switch op := req.GetOperation().GetOpType().(type) {
	case *connect.ConfigRequest_Operation_Set:
		for _, kv := range op.Set.GetPairs() {
			s.conf[kv.GetKey()] = kv.GetValue()
		}
	case *connect.ConfigRequest_Operation_Get:
		for _, k := range op.Get.GetKeys() {
			if v, ok := s.conf[k]; ok {
				val := v
				resp.Pairs = append(resp.Pairs, &connect.KeyValue{Key: k, Value: &val})
			}
		}
	}
	return resp, nil
}

func (s *testServer) AnalyzePlan(_ context.Context, req *connect.AnalyzePlanRequest) (*connect.AnalyzePlanResponse, error) {
	return &connect.AnalyzePlanResponse{
		SessionId:           req.GetSessionId(),
		ServerSideSessionId: "test-server-session",
		Result: &connect.AnalyzePlanResponse_SparkVersion_{
			SparkVersion: &connect.AnalyzePlanResponse_SparkVersion{Version: s.sparkVersion},
		},
	}, nil
}

func (s *testServer) Interrupt(_ context.Context, req *connect.InterruptRequest) (*connect.InterruptResponse, error) {
	if s.interruptErr != nil {
		return nil, s.interruptErr
	}
	s.mu.Lock()
	s.interrupted = true
	s.mu.Unlock()
	return &connect.InterruptResponse{
		SessionId:           req.GetSessionId(),
		ServerSideSessionId: "test-server-session",
	}, nil
}

func (s *testServer) ReleaseSession(_ context.Context, req *connect.ReleaseSessionRequest) (*connect.ReleaseSessionResponse, error) {
	s.mu.Lock()
	s.released = true
	s.mu.Unlock()
	return &connect.ReleaseSessionResponse{SessionId: req.GetSessionId()}, nil
}

func (s *testServer) execHeader(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	vals := s.lastExecMeta.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// encodeStream serializes schema and records to a complete Arrow IPC stream
// (schema + batch). With no records it writes a single zero-row record so the
// stream still carries the schema, matching Spark's empty-result behavior.
func encodeStream(alloc memory.Allocator, schema *arrow.Schema, records []arrow.Record) ([]byte, error) {
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

// startTestServer starts srv on a loopback port and returns the sc:// URI that
// addresses it. The server is stopped when the test finishes.
func startTestServer(t *testing.T, srv *testServer) string {
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

// dialTest dials the given URI with the real client and registers cleanup.
func dialTest(t *testing.T, uri string) *Client {
	t.Helper()
	cfg, err := ParseConnectionString(uri)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	c, err := Dial(context.Background(), cfg, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// intColumnResult builds a one-column int64 result with the given values.
func intColumnResult(alloc memory.Allocator, column string, values ...int64) clientResult {
	schema := arrow.NewSchema([]arrow.Field{{Name: column, Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	ib := b.Field(0).(*array.Int64Builder)
	for _, v := range values {
		ib.Append(v)
	}
	rec := b.NewRecord()
	return clientResult{schema: schema, records: []arrow.Record{rec}}
}

func readAllRows(t *testing.T, rdr array.RecordReader) [][]int64 {
	t.Helper()
	var out [][]int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		col := rec.Column(0).(*array.Int64)
		var row []int64
		for i := 0; i < col.Len(); i++ {
			row = append(row, col.Value(i))
		}
		out = append(out, row)
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reader err: %v", err)
	}
	return out
}

func TestExecuteSQLSingleBatch(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(string) clientResult {
		return intColumnResult(alloc, "n", 1, 2, 3)
	})
	c := dialTest(t, startTestServer(t, srv))

	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	defer rdr.Release()

	if got := rdr.Schema().Field(0).Name; got != "n" {
		t.Fatalf("schema field = %q, want n", got)
	}
	rows := readAllRows(t, rdr)
	if len(rows) != 1 || len(rows[0]) != 3 || rows[0][0] != 1 || rows[0][2] != 3 {
		t.Fatalf("rows = %v, want [[1 2 3]]", rows)
	}
}

func TestExecuteSQLMultiBatch(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(string) clientResult {
		schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
		mk := func(vals ...int64) arrow.Record {
			b := array.NewRecordBuilder(alloc, schema)
			defer b.Release()
			ib := b.Field(0).(*array.Int64Builder)
			for _, v := range vals {
				ib.Append(v)
			}
			return b.NewRecord()
		}
		return clientResult{schema: schema, records: []arrow.Record{mk(1, 2), mk(3), mk(4, 5)}}
	})
	c := dialTest(t, startTestServer(t, srv))

	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	defer rdr.Release()

	rows := readAllRows(t, rdr)
	// Three separate ArrowBatch messages -> three records concatenated.
	if len(rows) != 3 {
		t.Fatalf("expected 3 records, got %d (%v)", len(rows), rows)
	}
	var total int
	for _, r := range rows {
		total += len(r)
	}
	if total != 5 {
		t.Fatalf("expected 5 rows total, got %d", total)
	}
}

func TestExecuteSQLEmptyResult(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(string) clientResult {
		// No ArrowBatch at all -> client synthesizes empty no-column schema.
		return clientResult{}
	})
	c := dialTest(t, startTestServer(t, srv))

	rdr, err := c.ExecuteSQL(context.Background(), "CREATE TABLE t", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	defer rdr.Release()

	if rdr.Schema().NumFields() != 0 {
		t.Fatalf("expected empty schema, got %d fields", rdr.Schema().NumFields())
	}
	if rdr.Next() {
		t.Fatalf("expected no rows")
	}
}

func TestExecuteSQLServerError(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(string) clientResult {
		return clientResult{grpcErr: status.Error(codes.InvalidArgument, "bad sql")}
	})
	c := dialTest(t, startTestServer(t, srv))

	_, err := c.ExecuteSQL(context.Background(), "SELECT bad", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var ae adbc.Error
	if !asADBC(err, &ae) {
		t.Fatalf("expected adbc.Error, got %T: %v", err, err)
	}
	if ae.Code != adbc.StatusInvalidArgument {
		t.Fatalf("code = %v, want StatusInvalidArgument", ae.Code)
	}
}

func asADBC(err error, target *adbc.Error) bool {
	if ae, ok := err.(adbc.Error); ok {
		*target = ae
		return true
	}
	return false
}

func TestExecuteSQLPosArgs(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(string) clientResult {
		return intColumnResult(alloc, "n", 7)
	})
	c := dialTest(t, startTestServer(t, srv))

	lit := &connect.Expression_Literal{
		LiteralType: &connect.Expression_Literal_Integer{Integer: 42},
	}
	rdr, err := c.ExecuteSQL(context.Background(), "SELECT ?", []*connect.Expression_Literal{lit})
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	rdr.Release()

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.execPosArgs) == 0 || len(srv.execPosArgs[len(srv.execPosArgs)-1]) != 1 {
		t.Fatalf("server did not receive pos args: %v", srv.execPosArgs)
	}
	got := srv.execPosArgs[len(srv.execPosArgs)-1][0].GetInteger()
	if got != 42 {
		t.Fatalf("pos arg = %d, want 42", got)
	}
}

func TestSchemaOf(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(query string) clientResult {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			{Name: "b", Type: arrow.BinaryTypes.String},
		}, nil)
		return clientResult{schema: schema}
	})
	c := dialTest(t, startTestServer(t, srv))

	schema, err := c.SchemaOf(context.Background(), "SELECT a, b FROM t")
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}
	if schema.NumFields() != 2 || schema.Field(0).Name != "a" || schema.Field(1).Name != "b" {
		t.Fatalf("unexpected schema: %v", schema)
	}
	// Verify the query was wrapped with LIMIT 0.
	srv.mu.Lock()
	last := srv.execQueries[len(srv.execQueries)-1]
	srv.mu.Unlock()
	if !bytes.Contains([]byte(last), []byte("LIMIT 0")) {
		t.Fatalf("query not wrapped with LIMIT 0: %q", last)
	}
}

func TestSetConfAndGetConf(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, nil)
	c := dialTest(t, startTestServer(t, srv))
	ctx := context.Background()

	if err := c.SetConf(ctx, "spark.foo", "bar"); err != nil {
		t.Fatalf("SetConf: %v", err)
	}
	// Server session id captured from the Config response.
	if c.ServerSideSessionID() != "test-server-session" {
		t.Fatalf("server session = %q", c.ServerSideSessionID())
	}

	v, ok, err := c.GetConf(ctx, "spark.foo")
	if err != nil {
		t.Fatalf("GetConf: %v", err)
	}
	if !ok || v != "bar" {
		t.Fatalf("GetConf = %q,%v want bar,true", v, ok)
	}

	// Absent key -> ok=false.
	_, ok, err = c.GetConf(ctx, "spark.missing")
	if err != nil {
		t.Fatalf("GetConf missing: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
}

func TestConfErrorPaths(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, nil)
	srv.confErr = status.Error(codes.PermissionDenied, "nope")
	c := dialTest(t, startTestServer(t, srv))
	ctx := context.Background()

	if err := c.SetConf(ctx, "k", "v"); err == nil {
		t.Fatal("expected SetConf error")
	} else {
		var ae adbc.Error
		if !asADBC(err, &ae) || ae.Code != adbc.StatusUnauthorized {
			t.Fatalf("SetConf err = %v", err)
		}
	}
	if _, _, err := c.GetConf(ctx, "k"); err == nil {
		t.Fatal("expected GetConf error")
	}
}

func TestServerSparkVersion(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, nil)
	srv.sparkVersion = "4.0.1"
	c := dialTest(t, startTestServer(t, srv))

	ver, err := c.ServerSparkVersion(context.Background())
	if err != nil {
		t.Fatalf("ServerSparkVersion: %v", err)
	}
	if ver != "4.0.1" {
		t.Fatalf("version = %q, want 4.0.1", ver)
	}
	if c.ServerSideSessionID() != "test-server-session" {
		t.Fatalf("server session not captured: %q", c.ServerSideSessionID())
	}
}

func TestInterrupt(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, nil)
	c := dialTest(t, startTestServer(t, srv))

	if err := c.Interrupt(context.Background()); err != nil {
		t.Fatalf("Interrupt: %v", err)
	}
	srv.mu.Lock()
	got := srv.interrupted
	srv.mu.Unlock()
	if !got {
		t.Fatal("server did not see interrupt")
	}

	// Error path.
	srv.interruptErr = status.Error(codes.Unavailable, "down")
	if err := c.Interrupt(context.Background()); err == nil {
		t.Fatal("expected interrupt error")
	}
}

func TestServerSideSessionIDFromExecute(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(string) clientResult {
		return intColumnResult(alloc, "n", 1)
	})
	c := dialTest(t, startTestServer(t, srv))

	if c.ServerSideSessionID() != "" {
		t.Fatalf("expected empty server session before any call, got %q", c.ServerSideSessionID())
	}
	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	rdr.Release()
	if c.ServerSideSessionID() != "test-server-session" {
		t.Fatalf("server session = %q, want test-server-session", c.ServerSideSessionID())
	}
}

func TestSessionIDStable(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, nil)
	cfg, err := ParseConnectionString(startTestServer(t, srv))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cfg.SessionID = "pinned-session"
	c, err := Dial(context.Background(), cfg, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if c.SessionID() != "pinned-session" {
		t.Fatalf("SessionID = %q, want pinned-session", c.SessionID())
	}
}

func TestOutgoingHeaders(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(string) clientResult {
		return intColumnResult(alloc, "n", 1)
	})
	uri := startTestServer(t, srv) + "/;user_agent=my-agent;user_id=alice;x-custom=hello"
	c := dialTest(t, uri)

	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	rdr.Release()

	if got := srv.execHeader("x-custom"); got != "hello" {
		t.Fatalf("x-custom header = %q, want hello", got)
	}
	srv.mu.Lock()
	clientType := srv.lastClientType
	userCtx := srv.lastUserCtx
	srv.mu.Unlock()
	if clientType != "my-agent" {
		t.Fatalf("client_type = %q, want my-agent (user_agent)", clientType)
	}
	if userCtx.GetUserId() != "alice" {
		t.Fatalf("user_id = %q, want alice", userCtx.GetUserId())
	}
}

func TestTokenSetsAuthHeader(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(string) clientResult {
		return intColumnResult(alloc, "n", 1)
	})
	uri := startTestServer(t, srv) + "/;token=sekret"
	c := dialTest(t, uri)

	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	rdr.Release()

	if got := srv.execHeader("authorization"); got != "Bearer sekret" {
		t.Fatalf("authorization header = %q, want Bearer sekret", got)
	}
}

func TestReaderRetainReleaseAndRecord(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, func(string) clientResult {
		return intColumnResult(alloc, "n", 1, 2)
	})
	c := dialTest(t, startTestServer(t, srv))

	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	// Retain bumps the refcount so the first Release does not free.
	rdr.Retain()
	if !rdr.Next() {
		t.Fatal("expected a record")
	}
	// Deprecated Record() should return the same batch as RecordBatch().
	if rdr.Record() != rdr.RecordBatch() {
		t.Fatal("Record and RecordBatch differ")
	}
	rdr.Release() // refcount 2 -> 1, not freed
	rdr.Release() // refcount 1 -> 0, freed
}

func TestServerSparkVersionError(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, nil)
	c := dialTest(t, startTestServer(t, srv))
	// Close the channel so AnalyzePlan fails at transport level.
	_ = c.Close()
	if _, err := c.ServerSparkVersion(context.Background()); err == nil {
		t.Fatal("expected error after close")
	}
}

func TestCloseReleasesSession(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newTestServer(alloc, nil)
	c := dialTest(t, startTestServer(t, srv))

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Second close is a no-op.
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	srv.mu.Lock()
	released := srv.released
	srv.mu.Unlock()
	if !released {
		t.Fatal("server did not see ReleaseSession")
	}
}
