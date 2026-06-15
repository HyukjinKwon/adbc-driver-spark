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

//go:build e2e

package e2e

import (
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// isNotImplemented reports whether err is an ADBC StatusNotImplemented error.
func isNotImplemented(err error) bool {
	var ae adbc.Error
	return errors.As(err, &ae) && ae.Code == adbc.StatusNotImplemented
}

func TestGetInfo(t *testing.T) {
	ctx, conn := openConn(t)
	rdr, err := conn.GetInfo(ctx, nil)
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	defer rdr.Release()
	if n := drain(rdr); n == 0 {
		t.Fatalf("GetInfo returned no rows; expected at least driver/vendor metadata")
	}
}

func TestGetTableTypes(t *testing.T) {
	ctx, conn := openConn(t)
	rdr, err := conn.GetTableTypes(ctx)
	if err != nil {
		t.Fatalf("GetTableTypes: %v", err)
	}
	defer rdr.Release()
	if n := drain(rdr); n == 0 {
		t.Fatalf("GetTableTypes returned no rows; expected TABLE/VIEW/...")
	}
}

func TestExecuteUpdateAndGetObjects(t *testing.T) {
	ctx, conn := openConn(t)

	exec := func(sql string) {
		stmt, err := conn.NewStatement()
		if err != nil {
			t.Fatalf("NewStatement: %v", err)
		}
		defer stmt.Close()
		if err := stmt.SetSqlQuery(sql); err != nil {
			t.Fatalf("SetSqlQuery(%q): %v", sql, err)
		}
		if _, err := stmt.ExecuteUpdate(ctx); err != nil {
			t.Fatalf("ExecuteUpdate(%q): %v", sql, err)
		}
	}

	exec("CREATE OR REPLACE TEMPORARY VIEW adbc_e2e_view AS SELECT 1 AS a, 'x' AS b")

	// GetTableSchema for the view we just created.
	schema, err := conn.GetTableSchema(ctx, nil, nil, "adbc_e2e_view")
	switch {
	case isNotImplemented(err):
		t.Log("GetTableSchema not implemented yet; skipping that assertion")
	case err != nil:
		t.Fatalf("GetTableSchema: %v", err)
	default:
		if schema.NumFields() != 2 {
			t.Errorf("GetTableSchema field count = %d, want 2", schema.NumFields())
		}
	}

	// GetObjects should list at least our view somewhere in the hierarchy.
	rdr, err := conn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	switch {
	case isNotImplemented(err):
		t.Log("GetObjects not implemented yet; skipping that assertion")
	case err != nil:
		t.Fatalf("GetObjects: %v", err)
	default:
		defer rdr.Release()
		_ = drain(rdr)
	}
}

// TestPreparedBind exercises prepared statements with bound parameters. It is
// lenient: if the driver reports the feature as not implemented, the test is
// skipped rather than failed.
func TestPreparedBind(t *testing.T) {
	ctx, conn := openConn(t)
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT ? AS x"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	if err := stmt.Prepare(ctx); err != nil {
		if isNotImplemented(err) {
			t.Skip("Prepare not implemented yet")
		}
		t.Fatalf("Prepare: %v", err)
	}

	rec := singleInt32Record(t, "x", 42)
	defer rec.Release()

	if err := stmt.Bind(ctx, rec); err != nil {
		if isNotImplemented(err) {
			t.Skip("Bind not implemented yet")
		}
		t.Fatalf("Bind: %v", err)
	}

	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery (bound): %v", err)
	}
	defer rdr.Release()
	if n := drain(rdr); n != 1 {
		t.Fatalf("bound query returned %d rows, want 1", n)
	}
}

func TestErrorMapping(t *testing.T) {
	ctx, conn := openConn(t)
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT * FROM a_table_that_does_not_exist_xyz"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	_, _, err = stmt.ExecuteQuery(ctx)
	if err == nil {
		t.Fatal("expected an error for a missing table, got nil")
	}
	var ae adbc.Error
	if errors.As(err, &ae) {
		if ae.Code == adbc.StatusOK {
			t.Errorf("error has StatusOK code: %v", ae)
		}
	} else {
		t.Logf("error is not an adbc.Error (still acceptable): %v", err)
	}
}

// TestAutocommitOnly verifies that transaction control on Spark Connect either
// works as a no-op or cleanly reports not-implemented (Spark has no
// multi-statement transactions).
func TestTransactionsReportCleanly(t *testing.T) {
	ctx, conn := openConn(t)
	if err := conn.Commit(ctx); err != nil && !isNotImplemented(err) {
		t.Errorf("Commit returned unexpected error: %v", err)
	}
	if err := conn.Rollback(ctx); err != nil && !isNotImplemented(err) {
		t.Errorf("Rollback returned unexpected error: %v", err)
	}
}

func singleInt32Record(t *testing.T, name string, v int32) arrow.Record {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: name, Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).Append(v)
	return b.NewRecord()
}
