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

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	connect "github.com/HyukjinKwon/adbc-driver-spark/internal/sparkconnect/proto/spark/connect"
)

// statement is the concrete adbc.Statement implementation. It accumulates a SQL
// query and optional bound parameters, then executes them against the Spark
// Connect session that owns it.
type statement struct {
	cnxn  *connection
	alloc memory.Allocator

	query    string
	params   arrow.RecordBatch
	prepared bool
}

var _ adbc.Statement = (*statement)(nil)

// Close releases resources associated with the statement.
func (s *statement) Close() error {
	if s.params != nil {
		s.params.Release()
		s.params = nil
	}
	s.cnxn = nil
	return nil
}

// SetOption sets a string option on the statement. No statement-level options
// are currently recognized; unknown keys are rejected so callers learn quickly.
func (s *statement) SetOption(key, _ string) error {
	return adbc.Error{
		Msg:  "spark: unknown statement option: " + key,
		Code: adbc.StatusNotImplemented,
	}
}

// SetSqlQuery sets the SQL text to execute.
func (s *statement) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

// SetSubstraitPlan is unsupported; this driver speaks SQL only.
func (s *statement) SetSubstraitPlan([]byte) error {
	return adbc.Error{
		Msg:  "spark: Substrait plans are not supported; use SetSqlQuery",
		Code: adbc.StatusNotImplemented,
	}
}

// ExecuteQuery executes the current query and returns a reader over the result
// set. The affected-row count is reported as -1 because Spark Connect does not
// provide it ahead of consuming the stream.
func (s *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.cnxn == nil || s.cnxn.client == nil {
		return nil, -1, errClosed()
	}
	if s.query == "" {
		return nil, -1, errNoQuery()
	}

	posArgs, err := s.positionalLiterals()
	if err != nil {
		return nil, -1, err
	}

	reader, err := s.cnxn.client.ExecuteSQL(ctx, s.query, posArgs)
	if err != nil {
		return nil, -1, err
	}
	return reader, -1, nil
}

// ExecuteUpdate executes the current statement for its side effects, discarding
// any result set. It returns -1 because Spark Connect does not report an
// affected-row count for arbitrary statements.
func (s *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	reader, _, err := s.ExecuteQuery(ctx)
	if err != nil {
		return -1, err
	}
	defer reader.Release()
	for reader.Next() {
	}
	if err := reader.Err(); err != nil {
		return -1, err
	}
	return -1, nil
}

// Prepare marks the statement as prepared. Spark Connect evaluates SQL lazily on
// the server, so preparation is a client-side acknowledgement; the query is
// still validated when first executed.
func (s *statement) Prepare(context.Context) error {
	if s.query == "" {
		return errNoQuery()
	}
	s.prepared = true
	return nil
}

// Bind binds a single row of positional parameters from an Arrow record. The
// record is retained until the next Bind call or until the statement closes.
func (s *statement) Bind(_ context.Context, values arrow.RecordBatch) error {
	if values != nil && values.NumRows() > 1 {
		return adbc.Error{
			Msg:  "spark: parameter binding supports a single row; bulk binding is not implemented",
			Code: adbc.StatusNotImplemented,
		}
	}
	if s.params != nil {
		s.params.Release()
	}
	if values != nil {
		values.Retain()
	}
	s.params = values
	return nil
}

// BindStream is unsupported: streamed/bulk parameter binding is not implemented.
func (s *statement) BindStream(context.Context, array.RecordReader) error {
	return adbc.Error{
		Msg:  "spark: streamed parameter binding is not supported",
		Code: adbc.StatusNotImplemented,
	}
}

// GetParameterSchema is unsupported: Spark Connect does not expose the parameter
// schema of an arbitrary SQL statement.
func (s *statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Msg:  "spark: parameter schema introspection is not supported",
		Code: adbc.StatusNotImplemented,
	}
}

// ExecutePartitions is unsupported: Spark Connect does not expose result
// partitions to clients.
func (s *statement) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Msg:  "spark: partitioned execution is not supported",
		Code: adbc.StatusNotImplemented,
	}
}

// positionalLiterals converts the single bound parameter row, if any, into
// Spark Connect SQL literals in column order.
func (s *statement) positionalLiterals() ([]*connect.Expression_Literal, error) {
	if s.params == nil || s.params.NumRows() == 0 {
		return nil, nil
	}
	literals := make([]*connect.Expression_Literal, 0, s.params.NumCols())
	for col := 0; col < int(s.params.NumCols()); col++ {
		lit, err := columnToLiteral(s.params.Column(col), 0)
		if err != nil {
			return nil, err
		}
		literals = append(literals, lit)
	}
	return literals, nil
}

func errClosed() error {
	return adbc.Error{Msg: "spark: statement is closed", Code: adbc.StatusInvalidState}
}

func errNoQuery() error {
	return adbc.Error{Msg: "spark: no query has been set", Code: adbc.StatusInvalidState}
}
