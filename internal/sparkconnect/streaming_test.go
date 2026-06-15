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

// These tests pin the streaming result reader: it must decode one ArrowBatch at
// a time, hold no more than the current batch in memory, free every Arrow buffer
// it allocates on each exit path, and surface a server error that arrives after
// some batches have already been delivered. A CheckedAllocator backs the client
// so any leaked record buffer fails the test.

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// multiBatchResult returns a result whose records are sent as separate ArrowBatch
// messages, so the reader must pull each from the stream. Records are built on
// the supplied (server-side) allocator, kept distinct from the client allocator
// under test.
func multiBatchResult(alloc memory.Allocator, batches ...[]int64) clientResult {
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	recs := make([]arrow.Record, 0, len(batches))
	for _, vals := range batches {
		b := array.NewRecordBuilder(alloc, schema)
		ib := b.Field(0).(*array.Int64Builder)
		for _, v := range vals {
			ib.Append(v)
		}
		recs = append(recs, b.NewRecord())
		b.Release()
	}
	return clientResult{schema: schema, records: recs}
}

// dialChecked dials the test server with a CheckedAllocator so the test can
// assert that the client decode path leaks no Arrow buffers.
func dialChecked(t *testing.T, uri string) (*Client, *memory.CheckedAllocator) {
	t.Helper()
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	cfg, err := ParseConnectionString(uri)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	c, err := Dial(context.Background(), cfg, checked)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, checked
}

// TestStreamingFullReadNoLeak streams every batch to completion, then releases,
// and asserts the client allocator is back to zero outstanding bytes.
func TestStreamingFullReadNoLeak(t *testing.T) {
	srv := newTestServer(memory.DefaultAllocator, func(string) clientResult {
		return multiBatchResult(memory.DefaultAllocator, []int64{1, 2}, []int64{3}, []int64{4, 5, 6})
	})
	c, checked := dialChecked(t, startTestServer(t, srv))

	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	var rows, batches int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
		batches++
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reader err: %v", err)
	}
	if rows != 6 || batches != 3 {
		t.Fatalf("rows=%d batches=%d, want 6 and 3", rows, batches)
	}
	rdr.Release()
	checked.AssertSize(t, 0)
}

// TestStreamingPartialReadNoLeak reads only the first batch and then abandons the
// reader. The unyielded buffered batches must be freed and the stream torn down,
// leaving zero outstanding bytes.
func TestStreamingPartialReadNoLeak(t *testing.T) {
	srv := newTestServer(memory.DefaultAllocator, func(string) clientResult {
		return multiBatchResult(memory.DefaultAllocator, []int64{1, 2}, []int64{3}, []int64{4, 5})
	})
	c, checked := dialChecked(t, startTestServer(t, srv))

	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if !rdr.Next() {
		t.Fatal("expected at least one batch")
	}
	// Abandon without draining the rest.
	rdr.Release()
	checked.AssertSize(t, 0)
}

// TestStreamingSchemaOnlyNoLeak constructs the reader (which primes the first
// batch), reads the schema, and releases without iterating.
func TestStreamingSchemaOnlyNoLeak(t *testing.T) {
	srv := newTestServer(memory.DefaultAllocator, func(string) clientResult {
		return multiBatchResult(memory.DefaultAllocator, []int64{1, 2}, []int64{3})
	})
	c, checked := dialChecked(t, startTestServer(t, srv))

	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if rdr.Schema().NumFields() != 1 {
		t.Fatalf("schema fields = %d, want 1", rdr.Schema().NumFields())
	}
	rdr.Release()
	checked.AssertSize(t, 0)
}

// TestStreamingMidStreamError verifies that a server error arriving after some
// batches were already delivered is surfaced through Err() (not swallowed), and
// that releasing afterward leaks nothing.
func TestStreamingMidStreamError(t *testing.T) {
	srv := newTestServer(memory.DefaultAllocator, func(string) clientResult {
		r := multiBatchResult(memory.DefaultAllocator, []int64{1, 2})
		r.trailingErr = status.Error(codes.Internal, "boom mid-stream")
		return r
	})
	c, checked := dialChecked(t, startTestServer(t, srv))

	rdr, err := c.ExecuteSQL(context.Background(), "SELECT n", nil)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	// The first batch was primed at construction; iterate until the stream
	// yields the trailing error.
	for rdr.Next() {
	}
	rerr := rdr.Err()
	if rerr == nil {
		t.Fatal("expected mid-stream error from Err()")
	}
	var ae adbc.Error
	if !asADBC(rerr, &ae) || ae.Code != adbc.StatusInternal {
		t.Fatalf("err = %v, want adbc.Error StatusInternal", rerr)
	}
	rdr.Release()
	checked.AssertSize(t, 0)
}
