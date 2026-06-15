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

// Hermetic end-to-end tests: they run the real ADBC driver and the real Spark
// Connect transport against an in-process fake Spark Connect server (see
// fakespark_test.go) over a loopback gRPC connection. No live Spark, JVM, or
// Docker is required, so they execute in a normal `go test ./...`.

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

func openTestConn(t *testing.T, uri string) adbc.Connection {
	t.Helper()
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{adbc.OptionKeyURI: uri})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cnxn, err := db.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = cnxn.Close() })
	return cnxn
}

func TestExecuteQueryReturnsRows(t *testing.T) {
	alloc := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "msg", Type: arrow.BinaryTypes.String},
	}, nil)

	srv := newFakeServer(alloc, func(string) queryResult {
		b := array.NewRecordBuilder(alloc, schema)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
		return queryResult{schema: schema, records: []arrow.Record{b.NewRecord()}}
	})
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery("SELECT id, msg FROM t"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}

	reader, affected, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	defer reader.Release()
	if affected != -1 {
		t.Errorf("affected = %d, want -1 (unknown)", affected)
	}
	if got, want := reader.Schema().NumFields(), 2; got != want {
		t.Fatalf("schema fields = %d, want %d", got, want)
	}

	var ids []int64
	var msgs []string
	for reader.Next() {
		rec := reader.RecordBatch()
		idCol := rec.Column(0).(*array.Int64)
		msgCol := rec.Column(1).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			ids = append(ids, idCol.Value(i))
			msgs = append(msgs, msgCol.Value(i))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader.Err: %v", err)
	}
	if len(ids) != 3 || ids[0] != 1 || ids[2] != 3 {
		t.Errorf("ids = %v, want [1 2 3]", ids)
	}
	if len(msgs) != 3 || msgs[0] != "a" || msgs[2] != "c" {
		t.Errorf("msgs = %v, want [a b c]", msgs)
	}

	if call, ok := srv.lastCall(); !ok || call.query != "SELECT id, msg FROM t" {
		t.Errorf("server saw query %q", call.query)
	}
}

func TestExecuteUpdateNoResultSet(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, func(string) queryResult {
		// A DDL statement: schema with no columns, no rows.
		return schemaOnlyResult(arrow.NewSchema(nil, nil))
	})
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	affected, err := stmt.ExecuteUpdate(context.Background())
	if err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	if affected != -1 {
		t.Errorf("affected = %d, want -1", affected)
	}
}

func TestPositionalParameterBinding(t *testing.T) {
	alloc := memory.DefaultAllocator
	resultSchema := arrow.NewSchema([]arrow.Field{{Name: "ok", Type: arrow.FixedWidthTypes.Boolean}}, nil)
	srv := newFakeServer(alloc, func(string) queryResult {
		b := array.NewRecordBuilder(alloc, resultSchema)
		defer b.Release()
		b.Field(0).(*array.BooleanBuilder).Append(true)
		return queryResult{schema: resultSchema, records: []arrow.Record{b.NewRecord()}}
	})
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery("SELECT ? = ?"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}

	// Bind a single row with an int64 and a string parameter.
	paramSchema := arrow.NewSchema([]arrow.Field{
		{Name: "p0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "p1", Type: arrow.BinaryTypes.String},
	}, nil)
	pb := array.NewRecordBuilder(alloc, paramSchema)
	pb.Field(0).(*array.Int64Builder).Append(42)
	pb.Field(1).(*array.StringBuilder).Append("hello")
	params := pb.NewRecord()
	pb.Release()
	defer params.Release()

	if err := stmt.Bind(context.Background(), params); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	for reader.Next() {
	}
	reader.Release()

	call, ok := srv.lastCall()
	if !ok {
		t.Fatal("server saw no call")
	}
	if len(call.posArgs) != 2 {
		t.Fatalf("posArgs = %d, want 2", len(call.posArgs))
	}
	if got := call.posArgs[0].GetLong(); got != 42 {
		t.Errorf("posArgs[0] long = %d, want 42", got)
	}
	if got := call.posArgs[1].GetString_(); got != "hello" {
		t.Errorf("posArgs[1] string = %q, want hello", got)
	}
}

func TestGetTableSchema(t *testing.T) {
	alloc := memory.DefaultAllocator
	tableSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	srv := newFakeServer(alloc, func(q string) queryResult {
		// SchemaOf wraps the query as "SELECT * FROM (...) LIMIT 0".
		if containsFold(q, "LIMIT 0") {
			return schemaOnlyResult(tableSchema)
		}
		return queryResult{grpcErr: status.Error(codes.InvalidArgument, "unexpected query")}
	})
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	cat, sch := "spark_catalog", "default"
	got, err := cnxn.GetTableSchema(context.Background(), &cat, &sch, "people")
	if err != nil {
		t.Fatalf("GetTableSchema: %v", err)
	}
	if got.NumFields() != 2 || got.Field(0).Name != "id" || got.Field(1).Name != "name" {
		t.Errorf("schema = %v, want fields [id name]", got)
	}
	if call, ok := srv.lastCall(); !ok || !containsFold(call.query, "people") {
		t.Errorf("server query %q did not reference the table", call.query)
	}
}

func TestGetTableTypes(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	reader, err := cnxn.GetTableTypes(context.Background())
	if err != nil {
		t.Fatalf("GetTableTypes: %v", err)
	}
	defer reader.Release()

	var types []string
	for reader.Next() {
		col := reader.RecordBatch().Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			types = append(types, col.Value(i))
		}
	}
	want := map[string]bool{"TABLE": true, "VIEW": true, "TEMPORARY": true}
	for _, ty := range types {
		if !want[ty] {
			t.Errorf("unexpected table type %q", ty)
		}
		delete(want, ty)
	}
	if len(want) != 0 {
		t.Errorf("missing table types: %v", want)
	}
}

func TestGetInfoReportsDriverIdentity(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, nil)
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	reader, err := cnxn.GetInfo(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	defer reader.Release()

	found := map[adbc.InfoCode]string{}
	for reader.Next() {
		rec := reader.RecordBatch()
		names := rec.Column(0).(*array.Uint32)
		values := rec.Column(1).(*array.DenseUnion)
		strChild := values.Field(0).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			if values.TypeCode(i) == 0 {
				off := values.ValueOffset(i)
				found[adbc.InfoCode(names.Value(i))] = strChild.Value(int(off))
			}
		}
	}
	if found[adbc.InfoDriverName] != DriverName {
		t.Errorf("driver name = %q, want %q", found[adbc.InfoDriverName], DriverName)
	}
	if found[adbc.InfoVendorName] != VendorName {
		t.Errorf("vendor name = %q, want %q", found[adbc.InfoVendorName], VendorName)
	}
}

func TestGetObjectsFullDepth(t *testing.T) {
	alloc := memory.DefaultAllocator
	peopleSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	srv := newFakeServer(alloc, func(q string) queryResult {
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
			return queryResult{grpcErr: status.Errorf(codes.InvalidArgument, "unexpected query: %s", q)}
		}
	})
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthColumns, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("GetObjects returned no record")
	}
	rec := reader.RecordBatch()
	if rec.NumRows() != 1 {
		t.Fatalf("catalogs = %d, want 1", rec.NumRows())
	}
	catalogNames := rec.Column(0).(*array.String)
	if catalogNames.Value(0) != "spark_catalog" {
		t.Errorf("catalog = %q, want spark_catalog", catalogNames.Value(0))
	}

	// Drill: catalog_db_schemas -> db_schema_name.
	schemasList := rec.Column(1).(*array.List)
	schemaStruct := schemasList.ListValues().(*array.Struct)
	schemaName := schemaStruct.Field(0).(*array.String)
	if schemaStruct.Len() != 1 || schemaName.Value(0) != "default" {
		t.Fatalf("schema name = %q (len %d), want default", schemaName.Value(0), schemaStruct.Len())
	}

	// Drill: db_schema_tables -> table_name + table_type.
	tablesList := schemaStruct.Field(1).(*array.List)
	tableStruct := tablesList.ListValues().(*array.Struct)
	tableName := tableStruct.Field(0).(*array.String)
	tableType := tableStruct.Field(1).(*array.String)
	if tableStruct.Len() != 1 || tableName.Value(0) != "people" {
		t.Fatalf("table name = %q (len %d), want people", tableName.Value(0), tableStruct.Len())
	}
	if tableType.Value(0) != "TABLE" {
		t.Errorf("table type = %q, want TABLE", tableType.Value(0))
	}

	// Drill: table_columns -> column_name (id, name).
	columnsList := tableStruct.Field(2).(*array.List)
	columnStruct := columnsList.ListValues().(*array.Struct)
	columnName := columnStruct.Field(0).(*array.String)
	if columnStruct.Len() != 2 || columnName.Value(0) != "id" || columnName.Value(1) != "name" {
		t.Errorf("columns len %d, values [%q %q], want [id name]", columnStruct.Len(), columnName.Value(0), columnName.Value(1))
	}
}

func TestMultiBatchStreaming(t *testing.T) {
	alloc := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	mkRec := func(v int64) arrow.Record {
		b := array.NewRecordBuilder(alloc, schema)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).Append(v)
		return b.NewRecord()
	}
	srv := newFakeServer(alloc, func(string) queryResult {
		// Two records -> two separate Arrow IPC batches over the gRPC stream.
		return queryResult{schema: schema, records: []arrow.Record{mkRec(10), mkRec(20)}}
	})
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	_ = stmt.SetSqlQuery("SELECT n FROM t")
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	defer reader.Release()

	var got []int64
	for reader.Next() {
		col := reader.RecordBatch().Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			got = append(got, col.Value(i))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader.Err: %v", err)
	}
	if len(got) != 2 || got[0] != 10 || got[1] != 20 {
		t.Errorf("rows across batches = %v, want [10 20]", got)
	}
}

func TestGetObjectsCatalogDepthDoesNotDescend(t *testing.T) {
	alloc := memory.DefaultAllocator
	srv := newFakeServer(alloc, func(q string) queryResult {
		if containsFold(q, "SHOW CATALOGS") {
			return stringColumnResult(alloc, "catalog", "cat_a", "cat_b")
		}
		// Any descent (SHOW NAMESPACES/TABLES) at catalog depth is a bug.
		return queryResult{grpcErr: status.Errorf(codes.Internal, "must not descend: %s", q)}
	})
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	reader, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("no record")
	}
	rec := reader.RecordBatch()
	if rec.NumRows() != 2 {
		t.Errorf("catalogs = %d, want 2", rec.NumRows())
	}
	// catalog_db_schemas must be null at catalog depth.
	schemas := rec.Column(1).(*array.List)
	if !schemas.IsNull(0) || !schemas.IsNull(1) {
		t.Errorf("catalog_db_schemas should be null at ObjectDepthCatalogs")
	}
}

func TestAuthTokenPropagatedAsHeader(t *testing.T) {
	alloc := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{{Name: "ok", Type: arrow.FixedWidthTypes.Boolean}}, nil)
	srv := newFakeServer(alloc, func(string) queryResult {
		b := array.NewRecordBuilder(alloc, schema)
		defer b.Release()
		b.Field(0).(*array.BooleanBuilder).Append(true)
		return queryResult{schema: schema, records: []arrow.Record{b.NewRecord()}}
	})
	uri := startFakeServer(t, srv) + "/;token=s3cr3t-token"
	cnxn := openTestConn(t, uri)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	_ = stmt.SetSqlQuery("SELECT true")
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	for reader.Next() {
	}
	reader.Release()

	if got := srv.headerValue("authorization"); got != "Bearer s3cr3t-token" {
		t.Errorf("authorization header = %q, want %q", got, "Bearer s3cr3t-token")
	}
}

func TestExecuteQueryPropagatesServerError(t *testing.T) {
	srv := newFakeServer(memory.DefaultAllocator, func(string) queryResult {
		return queryResult{grpcErr: status.Error(codes.InvalidArgument, "boom: bad SQL")}
	})
	uri := startFakeServer(t, srv)
	cnxn := openTestConn(t, uri)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery("SELECT bad syntax"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	_, _, err = stmt.ExecuteQuery(context.Background())
	if err == nil {
		t.Fatal("expected an error from ExecuteQuery, got nil")
	}
	var adbcErr adbc.Error
	if !errors.As(err, &adbcErr) {
		t.Fatalf("error is not an adbc.Error: %T %v", err, err)
	}
	if adbcErr.Code != adbc.StatusInvalidArgument {
		t.Errorf("status = %v, want InvalidArgument", adbcErr.Code)
	}
}
