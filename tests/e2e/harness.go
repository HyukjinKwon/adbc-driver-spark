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

// Package e2e contains end-to-end tests that run the native Go ADBC driver
// against a live Spark Connect server.
//
// These tests are guarded by the `e2e` build tag so they never affect the
// default `go test ./...` matrix, and they skip at runtime unless the
// SPARK_CONNECT_URI environment variable points at a reachable server, e.g.
//
//	SPARK_CONNECT_URI=sc://localhost:15002 go test -tags e2e ./tests/e2e/...
package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	spark "github.com/HyukjinKwon/adbc-driver-spark/driver/spark"
)

// sparkURI returns the Spark Connect URI from the environment, skipping the
// test when it is not set.
func sparkURI(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("SPARK_CONNECT_URI")
	if uri == "" {
		t.Skip("SPARK_CONNECT_URI not set; skipping e2e test (run via tests/docker or the e2e workflow)")
	}
	return uri
}

// openConn opens a fresh ADBC database + connection against the configured
// Spark Connect server and registers cleanup. It fails the test on any error.
func openConn(t *testing.T) (context.Context, adbc.Connection) {
	t.Helper()
	uri := sparkURI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	drv := spark.NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		adbc.OptionKeyURI: uri,
	})
	if err != nil {
		t.Fatalf("NewDatabase(%q): %v", uri, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	conn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, conn
}

// query runs sql on a new statement and returns a fully drained slice of
// records plus the schema. Records are retained; the caller must Release them.
func query(t *testing.T, ctx context.Context, conn adbc.Connection, sql string) (*arrow.Schema, []arrow.Record) {
	t.Helper()
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		t.Fatalf("SetSqlQuery(%q): %v", sql, err)
	}
	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery(%q): %v", sql, err)
	}
	defer rdr.Release()

	schema := rdr.Schema()
	var recs []arrow.Record
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		recs = append(recs, rec)
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reading result of %q: %v", sql, err)
	}
	return schema, recs
}

// totalRows sums the row counts of a record slice.
func totalRows(recs []arrow.Record) int64 {
	var n int64
	for _, r := range recs {
		n += r.NumRows()
	}
	return n
}

// releaseAll releases every record in the slice.
func releaseAll(recs []arrow.Record) {
	for _, r := range recs {
		r.Release()
	}
}

// drain consumes and releases a RecordReader, returning the number of rows.
func drain(rdr array.RecordReader) int64 {
	var n int64
	for rdr.Next() {
		n += rdr.RecordBatch().NumRows()
	}
	return n
}
