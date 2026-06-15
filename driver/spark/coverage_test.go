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

// Additional hermetic tests covering statement lifecycle, connection metadata
// error paths, GetObjects at every depth, and database option handling. They use
// the in-process fake Spark Connect server from fakespark_test.go.

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- statement lifecycle ---

func TestStatementSetOptionUnknown(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	err = stmt.SetOption("nope", "x")
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusNotImplemented {
		t.Fatalf("SetOption err = %v", err)
	}
}

func TestStatementSetSubstraitPlanUnsupported(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()

	err := stmt.SetSubstraitPlan([]byte{1, 2, 3})
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusNotImplemented {
		t.Fatalf("SetSubstraitPlan err = %v", err)
	}
}

func TestStatementExecuteNoQuery(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()

	_, _, err := stmt.ExecuteQuery(context.Background())
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusInvalidState {
		t.Fatalf("ExecuteQuery err = %v, want InvalidState", err)
	}

	if _, err := stmt.ExecuteUpdate(context.Background()); err == nil {
		t.Fatal("ExecuteUpdate expected error with no query")
	}
}

func TestStatementExecuteAfterClose(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	_ = stmt.SetSqlQuery("SELECT 1")
	if err := stmt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, _, err := stmt.ExecuteQuery(context.Background())
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusInvalidState {
		t.Fatalf("ExecuteQuery after close err = %v, want InvalidState", err)
	}
}

func TestStatementExecuteUpdateRunsStatement(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, func(string) queryResult {
		return schemaOnlyResult(arrow.NewSchema(nil, nil))
	})
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()
	_ = stmt.SetSqlQuery("INSERT INTO t VALUES (1)")

	affected, err := stmt.ExecuteUpdate(context.Background())
	if err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	if affected != -1 {
		t.Fatalf("affected = %d, want -1", affected)
	}
}

func TestStatementExecuteUpdatePropagatesError(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, func(string) queryResult {
		return queryResult{grpcErr: status.Error(codes.InvalidArgument, "boom")}
	})
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()
	_ = stmt.SetSqlQuery("BAD")
	if _, err := stmt.ExecuteUpdate(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

func TestStatementPrepare(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()

	// No query yet -> error.
	if err := stmt.Prepare(context.Background()); err == nil {
		t.Fatal("Prepare expected error with no query")
	}
	_ = stmt.SetSqlQuery("SELECT 1")
	if err := stmt.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
}

func TestStatementBindRejectsMultiRow(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()

	schema := arrow.NewSchema([]arrow.Field{{Name: "p", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	rec := b.NewRecord()
	b.Release()
	defer rec.Release()

	err := stmt.Bind(context.Background(), rec)
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusNotImplemented {
		t.Fatalf("Bind multi-row err = %v, want NotImplemented", err)
	}
}

func TestStatementBindNilAndRebind(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()

	schema := arrow.NewSchema([]arrow.Field{{Name: "p", Type: arrow.PrimitiveTypes.Int64}}, nil)
	mk := func(v int64) arrow.RecordBatch {
		b := array.NewRecordBuilder(alloc, schema)
		b.Field(0).(*array.Int64Builder).Append(v)
		rec := b.NewRecord()
		b.Release()
		return rec
	}

	first := mk(1)
	if err := stmt.Bind(context.Background(), first); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	first.Release()
	// Rebind releases the previous record.
	second := mk(2)
	if err := stmt.Bind(context.Background(), second); err != nil {
		t.Fatalf("Bind 2: %v", err)
	}
	second.Release()
	// Bind nil clears params.
	if err := stmt.Bind(context.Background(), nil); err != nil {
		t.Fatalf("Bind nil: %v", err)
	}
}

func TestStatementBindStreamUnsupported(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()

	err := stmt.BindStream(context.Background(), nil)
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusNotImplemented {
		t.Fatalf("BindStream err = %v", err)
	}
}

func TestStatementGetParameterSchemaUnsupported(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()

	if _, err := stmt.GetParameterSchema(); err == nil {
		t.Fatal("GetParameterSchema expected error")
	}
}

func TestStatementExecutePartitionsUnsupported(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()

	_, _, _, err := stmt.ExecutePartitions(context.Background())
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusNotImplemented {
		t.Fatalf("ExecutePartitions err = %v", err)
	}
}

// --- connection unsupported / error paths ---

func TestConnectionCommitRollbackUnsupported(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))

	for _, fn := range []func(context.Context) error{cnxn.Commit, cnxn.Rollback} {
		err := fn(context.Background())
		var ae adbc.Error
		if !errors.As(err, &ae) || ae.Code != adbc.StatusNotImplemented {
			t.Fatalf("err = %v, want NotImplemented", err)
		}
	}
}

func TestConnectionReadPartitionUnsupported(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	if _, err := cnxn.ReadPartition(context.Background(), []byte("x")); err == nil {
		t.Fatal("ReadPartition expected error")
	}
}

func TestConnectionCloseIdempotent(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	cnxn := openTestConn(t, startFakeServer(t, srv))
	if err := cnxn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Second Close is a no-op (client already nil).
	if err := cnxn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestGetTableSchemaPropagatesError(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, func(string) queryResult {
		return queryResult{grpcErr: status.Error(codes.NotFound, "no table")}
	})
	cnxn := openTestConn(t, startFakeServer(t, srv))
	if _, err := cnxn.GetTableSchema(context.Background(), nil, nil, "missing"); err == nil {
		t.Fatal("expected error")
	}
}

// --- GetObjects depths ---

func newCatalogFakeServer(alloc memory.Allocator, peopleSchema *arrow.Schema) *fakeServer {
	return newFakeServer(alloc, func(q string) queryResult {
		switch {
		case containsFold(q, "SHOW CATALOGS"):
			return stringColumnResult(alloc, "catalog", "spark_catalog")
		case containsFold(q, "SHOW NAMESPACES"):
			return stringColumnResult(alloc, "namespace", "default")
		case containsFold(q, "SHOW TABLES"):
			return stringColumnResult(alloc, "tableName", "people")
		case containsFold(q, "LIMIT 0"):
			return schemaOnlyResult(peopleSchema)
		default:
			return queryResult{grpcErr: status.Errorf(codes.InvalidArgument, "unexpected: %s", q)}
		}
	})
}

func TestGetObjectsDBSchemaDepth(t *testing.T) {
	alloc := memory.DefaultAllocator
	peopleSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	srv := newCatalogFakeServer(alloc, peopleSchema)
	cnxn := openTestConn(t, startFakeServer(t, srv))

	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthDBSchemas, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("no record")
	}
	rec := reader.RecordBatch()
	schemasList := rec.Column(1).(*array.List)
	schemaStruct := schemasList.ListValues().(*array.Struct)
	if schemaStruct.Len() != 1 {
		t.Fatalf("schemas = %d, want 1", schemaStruct.Len())
	}
	// At DBSchemas depth, tables list should be null.
	tablesList := schemaStruct.Field(1).(*array.List)
	if !tablesList.IsNull(0) {
		t.Fatal("tables should be null at ObjectDepthDBSchemas")
	}
}

func TestGetObjectsTablesDepth(t *testing.T) {
	alloc := memory.DefaultAllocator
	peopleSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	srv := newCatalogFakeServer(alloc, peopleSchema)
	cnxn := openTestConn(t, startFakeServer(t, srv))

	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthTables, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("no record")
	}
	rec := reader.RecordBatch()
	schemaStruct := rec.Column(1).(*array.List).ListValues().(*array.Struct)
	tableStruct := schemaStruct.Field(1).(*array.List).ListValues().(*array.Struct)
	if tableStruct.Len() != 1 {
		t.Fatalf("tables = %d, want 1", tableStruct.Len())
	}
	// Columns list should be null at Tables depth.
	columnsList := tableStruct.Field(2).(*array.List)
	if !columnsList.IsNull(0) {
		t.Fatal("columns should be null at ObjectDepthTables")
	}
}

func TestGetObjectsWithCatalogFilter(t *testing.T) {
	alloc := memory.DefaultAllocator
	peopleSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	srv := newCatalogFakeServer(alloc, peopleSchema)
	cnxn := openTestConn(t, startFakeServer(t, srv))

	// A specific, non-pattern catalog name is used directly (no SHOW CATALOGS).
	cat := "spark_catalog"
	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthCatalogs, &cat, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("no record")
	}
	if got := reader.RecordBatch().Column(0).(*array.String).Value(0); got != "spark_catalog" {
		t.Fatalf("catalog = %q", got)
	}
}

func TestGetObjectsSchemaAndTableFilters(t *testing.T) {
	alloc := memory.DefaultAllocator
	peopleSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	var sawSchemaLike, sawTableLike bool
	srv := newFakeServer(alloc, func(q string) queryResult {
		switch {
		case containsFold(q, "SHOW CATALOGS"):
			return stringColumnResult(alloc, "catalog", "spark_catalog")
		case containsFold(q, "SHOW NAMESPACES"):
			if containsFold(q, "LIKE") {
				sawSchemaLike = true
			}
			return stringColumnResult(alloc, "namespace", "default")
		case containsFold(q, "SHOW TABLES"):
			if containsFold(q, "LIKE") {
				sawTableLike = true
			}
			return stringColumnResult(alloc, "tableName", "people")
		case containsFold(q, "LIMIT 0"):
			return schemaOnlyResult(peopleSchema)
		default:
			return queryResult{grpcErr: status.Errorf(codes.InvalidArgument, "unexpected: %s", q)}
		}
	})
	cnxn := openTestConn(t, startFakeServer(t, srv))

	sch := "default"
	tbl := "people"
	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthColumns, nil, &sch, &tbl, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	reader.Release()
	if !sawSchemaLike {
		t.Error("schema LIKE filter not applied")
	}
	if !sawTableLike {
		t.Error("table LIKE filter not applied")
	}
}

func TestGetObjectsCatalogFallback(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, func(q string) queryResult {
		switch {
		case containsFold(q, "SHOW CATALOGS"):
			// Server rejects SHOW CATALOGS -> client falls back to spark_catalog.
			return queryResult{grpcErr: status.Error(codes.Unimplemented, "no")}
		case containsFold(q, "SHOW NAMESPACES"):
			return stringColumnResult(alloc, "namespace", "default")
		default:
			return queryResult{grpcErr: status.Errorf(codes.InvalidArgument, "unexpected: %s", q)}
		}
	})
	cnxn := openTestConn(t, startFakeServer(t, srv))

	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthDBSchemas, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("no record")
	}
	if got := reader.RecordBatch().Column(0).(*array.String).Value(0); got != "spark_catalog" {
		t.Fatalf("fallback catalog = %q, want spark_catalog", got)
	}
}

func TestGetObjectsSchemaListingDegradesGracefully(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, func(q string) queryResult {
		switch {
		case containsFold(q, "SHOW CATALOGS"):
			return stringColumnResult(alloc, "catalog", "spark_catalog")
		case containsFold(q, "SHOW NAMESPACES"):
			// Both qualified and unqualified namespace listings fail -> no schemas.
			return queryResult{grpcErr: status.Error(codes.InvalidArgument, "nested dbs unsupported")}
		default:
			return queryResult{grpcErr: status.Errorf(codes.InvalidArgument, "unexpected: %s", q)}
		}
	})
	cnxn := openTestConn(t, startFakeServer(t, srv))

	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthColumns, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("no record")
	}
	// Catalog present but with an empty schema list.
	rec := reader.RecordBatch()
	schemaStruct := rec.Column(1).(*array.List).ListValues().(*array.Struct)
	if schemaStruct.Len() != 0 {
		t.Fatalf("expected 0 schemas, got %d", schemaStruct.Len())
	}
}

func TestGetObjectsColumnDescribeFailureSkips(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, func(q string) queryResult {
		switch {
		case containsFold(q, "SHOW CATALOGS"):
			return stringColumnResult(alloc, "catalog", "spark_catalog")
		case containsFold(q, "SHOW NAMESPACES"):
			return stringColumnResult(alloc, "namespace", "default")
		case containsFold(q, "SHOW TABLES"):
			return stringColumnResult(alloc, "tableName", "people")
		case containsFold(q, "LIMIT 0"):
			// Describe fails -> appendColumns errors and the table is kept
			// without columns rather than failing the whole call.
			return queryResult{grpcErr: status.Error(codes.PermissionDenied, "denied")}
		default:
			return queryResult{grpcErr: status.Errorf(codes.InvalidArgument, "unexpected: %s", q)}
		}
	})
	cnxn := openTestConn(t, startFakeServer(t, srv))

	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthColumns, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	reader.Release()
}

func TestGetObjectsCatalogListError(t *testing.T) {
	// listCatalogs falls back, but a hard error when filtering a specific
	// catalog with a pattern still flows through SHOW CATALOGS. Use a pattern
	// catalog so SHOW CATALOGS runs, and have schema listing fail at columns.
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, func(q string) queryResult {
		if containsFold(q, "SHOW CATALOGS") {
			return stringColumnResult(alloc, "catalog", "spark_catalog", "other")
		}
		return stringColumnResult(alloc, "namespace", "default")
	})
	cnxn := openTestConn(t, startFakeServer(t, srv))

	pat := "spark%"
	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthCatalogs, &pat, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("no record")
	}
	// Pattern "spark%" should match only spark_catalog.
	rec := reader.RecordBatch()
	if rec.NumRows() != 1 {
		t.Fatalf("catalogs = %d, want 1", rec.NumRows())
	}
}

// --- database options ---

func TestDatabaseSetOptionsRequiresURI(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	_, err := drv.NewDatabase(map[string]string{})
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusInvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestDatabaseSetOptionsInvalidURI(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	_, err := drv.NewDatabase(map[string]string{OptionKeyURI: "ftp://bad"})
	if err == nil {
		t.Fatal("expected error for invalid URI")
	}
}

func TestDatabaseSetOptionsAllKeys(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		OptionKeyURI:        "sc://localhost:15002",
		OptionKeyToken:      "tok",
		OptionKeyUserID:     "alice",
		OptionKeyUserAgent:  "agent",
		OptionKeySessionID:  "sess",
		OptionKeyTLSEnabled: "true",
	})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDatabaseSetOptionsInvalidTLS(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	_, err := drv.NewDatabase(map[string]string{
		OptionKeyURI:        "sc://localhost:15002",
		OptionKeyTLSEnabled: "notabool",
	})
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusInvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestDatabaseSetOptionsOverlayWithoutURI(t *testing.T) {
	// First configure with a URI, then a second SetOptions without a URI should
	// reuse the existing config and apply overlay options.
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionKeyURI: "sc://localhost:15002"})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	d := db.(*database)
	if err := d.SetOptions(map[string]string{OptionKeyToken: "later-token"}); err != nil {
		t.Fatalf("SetOptions overlay: %v", err)
	}
	d.mu.Lock()
	tok := d.cfg.Token
	d.mu.Unlock()
	if tok != "later-token" {
		t.Fatalf("token = %q, want later-token", tok)
	}
}

func TestDatabaseOpenUnconfigured(t *testing.T) {
	d := &database{alloc: memory.DefaultAllocator}
	_, err := d.Open(context.Background())
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusInvalidState {
		t.Fatalf("Open err = %v, want InvalidState", err)
	}
}

func TestDriverNewDatabaseWithContext(t *testing.T) {
	drv := NewDriver(nil).(*driver)
	db, err := drv.NewDatabaseWithContext(context.Background(), map[string]string{OptionKeyURI: "sc://localhost:15002"})
	if err != nil {
		t.Fatalf("NewDatabaseWithContext: %v", err)
	}
	_ = db.Close()
}

func TestServerSideSessionIDCaptured(t *testing.T) {
	// Drive a query through the driver and confirm the underlying client
	// captured the server-side session id reported by the fake server.
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, func(string) queryResult {
		return stringColumnResult(alloc, "x", "v")
	})
	cnxn := openTestConn(t, startFakeServer(t, srv))
	c := cnxn.(*connection)

	stmt, _ := c.NewStatement()
	defer stmt.Close()
	_ = stmt.SetSqlQuery("SELECT 'v' AS x")
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	for reader.Next() {
	}
	reader.Release()

	if got := c.client.ServerSideSessionID(); got != "fake-server-session" {
		t.Fatalf("server session = %q, want fake-server-session", got)
	}
}
