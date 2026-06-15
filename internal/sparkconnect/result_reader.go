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
	"context"
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
// streams (one per ArrowBatch message); this reader decodes them lazily.
//
// Delivery is streaming: only the records of a single ArrowBatch are held in
// memory at a time, and the next batch is pulled from the gRPC stream as the
// consumer advances. The first batch is read eagerly during construction so the
// schema is known before the first Next call. This keeps client memory bounded
// regardless of result size and lets the server pace production to consumption.
//
// The reader follows the standard RecordReader ownership contract: the record
// returned by RecordBatch is valid only until the next call to Next (or until
// Release). Callers that need to retain a record across iterations must Retain
// it themselves.
type resultReader struct {
	refCount int64
	schema   *arrow.Schema

	c      *Client
	alloc  memory.Allocator
	stream grpc.ServerStreamingClient[connect.ExecutePlanResponse]
	// cancel tears down the underlying gRPC stream. It is called from Release so
	// that abandoning a reader before the stream is exhausted still frees the
	// server-side operation promptly.
	cancel context.CancelFunc

	// buf holds the decoded records of the most recently fetched ArrowBatch that
	// have not yet been yielded; bufPos is the index of the next one to yield.
	buf    []arrow.Record
	bufPos int
	cur    arrow.Record
	err    error
	// done is set once the stream has been fully consumed or has errored; no
	// further Recv calls are made after it is true.
	done bool
}

var _ array.RecordReader = (*resultReader)(nil)

func newResultReader(c *Client, stream grpc.ServerStreamingClient[connect.ExecutePlanResponse], cancel context.CancelFunc) (array.RecordReader, error) {
	r := &resultReader{
		refCount: 1,
		c:        c,
		alloc:    c.Allocator(),
		stream:   stream,
		cancel:   cancel,
	}

	// Prime the first batch so Schema is available before the first Next, and so
	// that a query that fails immediately surfaces the error from construction.
	r.fill()
	if r.err != nil {
		err := r.err
		r.Release()
		return nil, err
	}
	if r.schema == nil {
		// The statement produced no Arrow output (for example a DDL command).
		// Present an empty result with no columns rather than failing.
		r.schema = arrow.NewSchema(nil, nil)
	}
	return r, nil
}

// fill pulls response messages from the stream until it decodes a non-empty
// ArrowBatch into buf, or the stream ends (done) or errors (err+done). Non-batch
// messages (such as ResultComplete) and empty batches are skipped.
func (r *resultReader) fill() {
	for {
		resp, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			r.done = true
			return
		}
		if err != nil {
			r.err = wrapGRPC(err)
			r.done = true
			return
		}
		r.c.rememberServerSession(resp.GetServerSideSessionId())

		batch := resp.GetArrowBatch()
		if batch == nil || len(batch.GetData()) == 0 {
			continue
		}
		recs, derr := r.decodeIPC(batch.GetData())
		if derr != nil {
			r.err = derr
			r.done = true
			return
		}
		if len(recs) == 0 {
			continue
		}
		r.buf = recs
		r.bufPos = 0
		return
	}
}

// decodeIPC decodes one ArrowBatch payload (a complete Arrow IPC stream) into
// its constituent records, retaining each so it survives past the reader's
// release. The schema is captured from the first batch decoded.
func (r *resultReader) decodeIPC(data []byte) ([]arrow.Record, error) {
	rdr, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(r.alloc))
	if err != nil {
		return nil, fmt.Errorf("spark connect: decode arrow batch: %w", err)
	}
	defer rdr.Release()

	if r.schema == nil {
		r.schema = rdr.Schema()
	}
	var recs []arrow.Record
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		recs = append(recs, rec)
	}
	if err := rdr.Err(); err != nil && !errors.Is(err, io.EOF) {
		for _, rec := range recs {
			rec.Release()
		}
		return nil, fmt.Errorf("spark connect: read arrow batch: %w", err)
	}
	return recs, nil
}

// Retain increases the reference count by 1.
func (r *resultReader) Retain() { atomic.AddInt64(&r.refCount, 1) }

// Release decreases the reference count by 1, freeing all held records and
// tearing down the gRPC stream at zero.
func (r *resultReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) != 0 {
		return
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	for i := r.bufPos; i < len(r.buf); i++ {
		r.buf[i].Release()
	}
	r.buf = nil
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
}

// Schema returns the Arrow schema common to every record in the result.
func (r *resultReader) Schema() *arrow.Schema { return r.schema }

// Next advances to the next record, returning false at end of stream or error.
// The record from the previous call is released, so callers must Retain any
// record they need to keep across iterations.
func (r *resultReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.err != nil {
		return false
	}
	for r.bufPos >= len(r.buf) {
		if r.done {
			return false
		}
		r.buf = nil
		r.bufPos = 0
		r.fill()
		if r.err != nil {
			return false
		}
	}
	r.cur = r.buf[r.bufPos]
	r.bufPos++
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
