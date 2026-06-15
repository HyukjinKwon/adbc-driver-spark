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
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	connect "github.com/HyukjinKwon/adbc-driver-spark/internal/sparkconnect/proto/spark/connect"
)

// resultReader adapts a Spark Connect ExecutePlan response stream to the Arrow
// array.RecordReader interface. Spark sends results as a sequence of Arrow IPC
// streams (one per ArrowBatch message); this reader concatenates the record
// batches across all of them while preserving a single, stable schema.
//
// Results are materialized eagerly when the reader is constructed. This keeps
// memory ownership simple and correct; streaming decode is tracked as a future
// optimization in the project roadmap.
type resultReader struct {
	refCount int64
	schema   *arrow.Schema
	records  []arrow.Record
	pos      int
	cur      arrow.Record
	err      error
}

var _ array.RecordReader = (*resultReader)(nil)

func newResultReader(c *Client, stream grpc.ServerStreamingClient[connect.ExecutePlanResponse]) (array.RecordReader, error) {
	r := &resultReader{refCount: 1, pos: -1}
	alloc := c.Allocator()

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			r.releaseRecords()
			return nil, wrapGRPC(err)
		}
		c.rememberServerSession(resp.GetServerSideSessionId())

		batch := resp.GetArrowBatch()
		if batch == nil || len(batch.GetData()) == 0 {
			continue
		}
		if err := r.appendIPCStream(alloc, batch.GetData()); err != nil {
			r.releaseRecords()
			return nil, err
		}
	}

	if r.schema == nil {
		// The statement produced no Arrow output (for example a DDL command).
		// Present an empty result with no columns rather than failing.
		r.schema = arrow.NewSchema(nil, nil)
	}
	return r, nil
}

func (r *resultReader) appendIPCStream(alloc memory.Allocator, data []byte) error {
	rdr, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(alloc))
	if err != nil {
		return fmt.Errorf("spark connect: decode arrow batch: %w", err)
	}
	defer rdr.Release()

	if r.schema == nil {
		r.schema = rdr.Schema()
	}
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		r.records = append(r.records, rec)
	}
	if err := rdr.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("spark connect: read arrow batch: %w", err)
	}
	return nil
}

func (r *resultReader) releaseRecords() {
	for _, rec := range r.records {
		rec.Release()
	}
	r.records = nil
}

// Retain increases the reference count by 1.
func (r *resultReader) Retain() { atomic.AddInt64(&r.refCount, 1) }

// Release decreases the reference count by 1, freeing all held records at zero.
func (r *resultReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) != 0 {
		return
	}
	if r.cur != nil {
		r.cur = nil
	}
	r.releaseRecords()
}

// Schema returns the Arrow schema common to every record in the result.
func (r *resultReader) Schema() *arrow.Schema { return r.schema }

// Next advances to the next record, returning false at end of stream or error.
func (r *resultReader) Next() bool {
	if r.err != nil {
		return false
	}
	r.pos++
	if r.pos >= len(r.records) {
		r.cur = nil
		return false
	}
	r.cur = r.records[r.pos]
	return true
}

// RecordBatch returns the record produced by the most recent call to Next.
func (r *resultReader) RecordBatch() arrow.RecordBatch { return r.cur }

// Record returns the record produced by the most recent call to Next.
//
// Deprecated: retained to satisfy the array.RecordReader interface; use
// RecordBatch instead.
func (r *resultReader) Record() arrow.RecordBatch { return r.cur }

// Err returns any error encountered while iterating.
func (r *resultReader) Err() error { return r.err }
