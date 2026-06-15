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
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/HyukjinKwon/adbc-driver-spark/internal/sparkconnect"
)

// connection is the concrete adbc.Connection implementation. It wraps a live
// Spark Connect client and exposes statement execution and catalog metadata.
type connection struct {
	db     *database
	client *sparkconnect.Client
	alloc  memory.Allocator
}

var _ adbc.Connection = (*connection)(nil)

// NewStatement initializes a new statement bound to this connection.
func (c *connection) NewStatement() (adbc.Statement, error) {
	return &statement{cnxn: c, alloc: c.alloc}, nil
}

// Close releases the Spark Connect session and underlying gRPC channel.
func (c *connection) Close() error {
	if c.client == nil {
		return nil
	}
	err := c.client.Close()
	c.client = nil
	if err != nil {
		return adbc.Error{Msg: err.Error(), Code: adbc.StatusIO}
	}
	return nil
}

// Commit is a no-op: Spark Connect statements run in autocommit mode. ADBC
// requires this method to only be used when autocommit is disabled, which this
// driver does not support, so it reports the condition explicitly.
func (c *connection) Commit(context.Context) error {
	return adbc.Error{
		Msg:  "spark: transactions are not supported; the driver operates in autocommit mode",
		Code: adbc.StatusNotImplemented,
	}
}

// Rollback mirrors Commit: transactions are not supported.
func (c *connection) Rollback(context.Context) error {
	return adbc.Error{
		Msg:  "spark: transactions are not supported; the driver operates in autocommit mode",
		Code: adbc.StatusNotImplemented,
	}
}

// GetTableSchema returns the Arrow schema of the named table. Spark Connect does
// not expose schema metadata as Arrow directly, so the schema is obtained by
// describing the fully-qualified relation with a zero-row projection.
func (c *connection) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	ref := quoteQualifiedName(catalog, dbSchema, tableName)
	schema, err := c.client.SchemaOf(ctx, "SELECT * FROM "+ref)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// GetTableTypes returns the table types Spark exposes through its catalog.
func (c *connection) GetTableTypes(context.Context) (array.RecordReader, error) {
	return buildTableTypes(c.alloc), nil
}

// GetInfo returns driver and vendor metadata for the requested info codes.
func (c *connection) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	return c.buildInfo(ctx, infoCodes)
}

// GetObjects returns a hierarchical view of catalogs, schemas, tables, and
// columns, applying the supplied search patterns.
func (c *connection) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	return c.buildObjects(ctx, depth, catalog, dbSchema, tableName, columnName, tableType)
}

// ReadPartition is unsupported: Spark Connect does not expose independently
// addressable result partitions to clients.
func (c *connection) ReadPartition(context.Context, []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Msg:  "spark: partitioned result reads are not supported",
		Code: adbc.StatusNotImplemented,
	}
}

// quoteQualifiedName assembles a backtick-quoted, dot-separated identifier from
// the optional catalog and schema and the required table name.
func quoteQualifiedName(catalog, dbSchema *string, table string) string {
	parts := make([]string, 0, 3)
	if catalog != nil && *catalog != "" {
		parts = append(parts, quoteIdent(*catalog))
	}
	if dbSchema != nil && *dbSchema != "" {
		parts = append(parts, quoteIdent(*dbSchema))
	}
	parts = append(parts, quoteIdent(table))
	out := parts[0]
	for _, p := range parts[1:] {
		out = fmt.Sprintf("%s.%s", out, p)
	}
	return out
}

// quoteIdent quotes a Spark SQL identifier with backticks, escaping any
// embedded backticks per Spark's lexer rules.
func quoteIdent(ident string) string {
	escaped := ""
	for _, r := range ident {
		if r == '`' {
			escaped += "``"
			continue
		}
		escaped += string(r)
	}
	return "`" + escaped + "`"
}
