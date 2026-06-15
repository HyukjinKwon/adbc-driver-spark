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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestSelectLiteral(t *testing.T) {
	ctx, conn := openConn(t)
	schema, recs := query(t, ctx, conn, "SELECT 1 AS id, 'hi' AS msg")
	defer releaseAll(recs)

	if got := totalRows(recs); got != 1 {
		t.Fatalf("row count = %d, want 1", got)
	}
	if schema.NumFields() != 2 {
		t.Fatalf("field count = %d, want 2", schema.NumFields())
	}
	if schema.Field(0).Name != "id" || schema.Field(1).Name != "msg" {
		t.Fatalf("field names = %q, %q; want id, msg", schema.Field(0).Name, schema.Field(1).Name)
	}

	rec := recs[0]
	id, ok := rec.Column(0).(*array.Int32)
	if !ok {
		t.Fatalf("column 0 is %T, want *array.Int32", rec.Column(0))
	}
	if id.Value(0) != 1 {
		t.Fatalf("id = %d, want 1", id.Value(0))
	}
	msg, ok := rec.Column(1).(*array.String)
	if !ok {
		t.Fatalf("column 1 is %T, want *array.String", rec.Column(1))
	}
	if msg.Value(0) != "hi" {
		t.Fatalf("msg = %q, want hi", msg.Value(0))
	}
}

func TestMultiRowAggregation(t *testing.T) {
	ctx, conn := openConn(t)
	schema, recs := query(t, ctx, conn,
		"SELECT id, id * 2 AS doubled FROM range(0, 1000) ORDER BY id")
	defer releaseAll(recs)

	if got := totalRows(recs); got != 1000 {
		t.Fatalf("row count = %d, want 1000", got)
	}
	if schema.NumFields() != 2 {
		t.Fatalf("field count = %d, want 2", schema.NumFields())
	}
}

// TestTypeMapping verifies the documented Spark to Arrow type mapping by
// selecting one column of each supported type.
func TestTypeMapping(t *testing.T) {
	ctx, conn := openConn(t)
	const sql = `SELECT
		CAST(true AS BOOLEAN)              AS c_bool,
		CAST(1 AS TINYINT)                 AS c_byte,
		CAST(2 AS SMALLINT)                AS c_short,
		CAST(3 AS INT)                     AS c_int,
		CAST(4 AS BIGINT)                  AS c_long,
		CAST(1.5 AS FLOAT)                 AS c_float,
		CAST(2.5 AS DOUBLE)                AS c_double,
		CAST(3.14 AS DECIMAL(10,2))        AS c_decimal,
		CAST('s' AS STRING)                AS c_string,
		CAST('b' AS BINARY)                AS c_binary,
		CAST('2024-01-02' AS DATE)         AS c_date,
		CAST('2024-01-02 03:04:05' AS TIMESTAMP) AS c_ts,
		ARRAY(1, 2, 3)                     AS c_array,
		MAP('k', 1)                        AS c_map,
		NAMED_STRUCT('a', 1, 'b', 's')     AS c_struct`
	schema, recs := query(t, ctx, conn, sql)
	defer releaseAll(recs)

	want := map[string]arrow.Type{
		"c_bool":    arrow.BOOL,
		"c_byte":    arrow.INT8,
		"c_short":   arrow.INT16,
		"c_int":     arrow.INT32,
		"c_long":    arrow.INT64,
		"c_float":   arrow.FLOAT32,
		"c_double":  arrow.FLOAT64,
		"c_decimal": arrow.DECIMAL128,
		"c_string":  arrow.STRING,
		"c_binary":  arrow.BINARY,
		"c_date":    arrow.DATE32,
		"c_ts":      arrow.TIMESTAMP,
		"c_array":   arrow.LIST,
		"c_map":     arrow.MAP,
		"c_struct":  arrow.STRUCT,
	}
	for name, typ := range want {
		idx := schema.FieldIndices(name)
		if len(idx) == 0 {
			t.Errorf("missing column %q", name)
			continue
		}
		got := schema.Field(idx[0]).Type.ID()
		if got != typ {
			t.Errorf("column %q has arrow type %s, want %s", name, got, typ)
		}
	}
	if got := totalRows(recs); got != 1 {
		t.Fatalf("row count = %d, want 1", got)
	}
}

func TestNullHandling(t *testing.T) {
	ctx, conn := openConn(t)
	_, recs := query(t, ctx, conn, "SELECT CAST(NULL AS INT) AS n")
	defer releaseAll(recs)

	if len(recs) == 0 || recs[0].NumRows() != 1 {
		t.Fatalf("expected one row")
	}
	if !recs[0].Column(0).IsNull(0) {
		t.Fatalf("expected null value")
	}
}
