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
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// buildObjects implements Connection.GetObjects by walking the Spark catalog at
// the requested depth and assembling the standard ADBC nested Arrow structure.
//
// The traversal issues Spark SQL catalog commands (SHOW CATALOGS / NAMESPACES /
// TABLES) and reuses GetTableSchema for column information. Filters are applied
// as SQL LIKE patterns where Spark supports them and client-side otherwise.
func (c *connection) buildObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableTypes []string) (array.RecordReader, error) {
	catalogs, err := c.listCatalogs(ctx, catalog)
	if err != nil {
		return nil, err
	}

	bldr := array.NewRecordBuilder(c.alloc, getObjectsSchema)
	defer bldr.Release()

	catalogNameBldr := bldr.Field(0).(*array.StringBuilder)
	dbSchemaListBldr := bldr.Field(1).(*array.ListBuilder)

	for _, cat := range catalogs {
		catalogNameBldr.Append(cat)

		if depth == adbc.ObjectDepthCatalogs {
			dbSchemaListBldr.AppendNull()
			continue
		}

		dbSchemaListBldr.Append(true)
		if err := c.appendDBSchemas(ctx, dbSchemaListBldr.ValueBuilder().(*array.StructBuilder), depth, cat, dbSchema, tableName, tableTypes); err != nil {
			return nil, err
		}
	}

	rec := bldr.NewRecord()
	defer rec.Release()
	rr, _ := array.NewRecordReader(getObjectsSchema, []arrow.Record{rec})
	return rr, nil
}

func (c *connection) appendDBSchemas(ctx context.Context, sb *array.StructBuilder, depth adbc.ObjectDepth, catalog string, dbSchema, tableName *string, tableTypes []string) error {
	schemas, err := c.listSchemas(ctx, catalog, dbSchema)
	if err != nil {
		return err
	}

	nameBldr := sb.FieldBuilder(0).(*array.StringBuilder)
	tablesListBldr := sb.FieldBuilder(1).(*array.ListBuilder)

	for _, sch := range schemas {
		sb.Append(true)
		nameBldr.Append(sch)

		if depth == adbc.ObjectDepthDBSchemas {
			tablesListBldr.AppendNull()
			continue
		}

		tablesListBldr.Append(true)
		if err := c.appendTables(ctx, tablesListBldr.ValueBuilder().(*array.StructBuilder), depth, catalog, sch, tableName, tableTypes); err != nil {
			return err
		}
	}
	return nil
}

func (c *connection) appendTables(ctx context.Context, sb *array.StructBuilder, depth adbc.ObjectDepth, catalog, schema string, tableName *string, tableTypes []string) error {
	tables, err := c.listTables(ctx, catalog, schema, tableName)
	if err != nil {
		return err
	}

	nameBldr := sb.FieldBuilder(0).(*array.StringBuilder)
	typeBldr := sb.FieldBuilder(1).(*array.StringBuilder)
	columnsListBldr := sb.FieldBuilder(2).(*array.ListBuilder)
	constraintsListBldr := sb.FieldBuilder(3).(*array.ListBuilder)

	for _, tbl := range tables {
		sb.Append(true)
		nameBldr.Append(tbl)
		typeBldr.Append("TABLE")
		// Spark Connect does not expose constraint metadata; emit an empty list.
		constraintsListBldr.Append(true)

		if depth != adbc.ObjectDepthColumns {
			columnsListBldr.AppendNull()
			continue
		}

		columnsListBldr.Append(true)
		if err := c.appendColumns(ctx, columnsListBldr.ValueBuilder().(*array.StructBuilder), catalog, schema, tbl); err != nil {
			// A table can disappear or be inaccessible between listing and
			// describing; treat columns as unavailable rather than failing the
			// whole call.
			continue
		}
	}
	return nil
}

func (c *connection) appendColumns(ctx context.Context, sb *array.StructBuilder, catalog, schema, table string) error {
	cat, sch := catalog, schema
	tableArrowSchema, err := c.GetTableSchema(ctx, &cat, &sch, table)
	if err != nil {
		return err
	}

	nameBldr := sb.FieldBuilder(0).(*array.StringBuilder)
	ordinalBldr := sb.FieldBuilder(1).(*array.Int32Builder)

	for i, field := range tableArrowSchema.Fields() {
		sb.Append(true)
		nameBldr.Append(field.Name)
		ordinalBldr.Append(int32(i + 1))
		// Remaining xdbc_* fields are optional and reported as null.
		for f := 2; f < sb.NumField(); f++ {
			sb.FieldBuilder(f).AppendNull()
		}
	}
	return nil
}

// listCatalogs returns the catalogs to traverse. When a specific, non-pattern
// catalog name is supplied it is used directly; otherwise SHOW CATALOGS is
// consulted (falling back to the default catalog if unsupported).
func (c *connection) listCatalogs(ctx context.Context, catalog *string) ([]string, error) {
	if catalog != nil && *catalog != "" && !isPattern(*catalog) {
		return []string{*catalog}, nil
	}
	names, err := c.queryStrings(ctx, "SHOW CATALOGS", "catalog")
	if err != nil {
		// Older Spark deployments may not support SHOW CATALOGS.
		return []string{"spark_catalog"}, nil
	}
	return filterByPattern(names, catalog), nil
}

func (c *connection) listSchemas(ctx context.Context, catalog string, dbSchema *string) ([]string, error) {
	like := ""
	if dbSchema != nil && *dbSchema != "" {
		like = " LIKE '" + escapeLike(*dbSchema) + "'"
	}
	// "SHOW NAMESPACES IN <catalog>" is the portable form, but the v1 session
	// catalog on several Spark versions rejects a qualified namespace with
	// "Nested databases are not supported by v1 session catalog". Fall back to
	// listing namespaces in the current catalog, then degrade to no schemas
	// rather than failing the whole GetObjects call.
	names, err := c.queryStrings(ctx, "SHOW NAMESPACES IN "+quoteIdent(catalog)+like, "namespace")
	if err != nil {
		names, err = c.queryStrings(ctx, "SHOW NAMESPACES"+like, "namespace")
	}
	if err != nil {
		return nil, nil
	}
	return names, nil
}

func (c *connection) listTables(ctx context.Context, catalog, schema string, tableName *string) ([]string, error) {
	sql := "SHOW TABLES IN " + quoteIdent(catalog) + "." + quoteIdent(schema)
	if tableName != nil && *tableName != "" {
		sql += " LIKE '" + escapeLike(*tableName) + "'"
	}
	return c.queryStrings(ctx, sql, "tableName")
}

// queryStrings runs a SQL statement and collects the values of a named string
// column across all returned record batches.
func (c *connection) queryStrings(ctx context.Context, sql, column string) ([]string, error) {
	reader, err := c.client.ExecuteSQL(ctx, sql, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	idx := -1
	for i, f := range reader.Schema().Fields() {
		if strings.EqualFold(f.Name, column) {
			idx = i
			break
		}
	}
	if idx < 0 {
		// Fall back to the first column if the expected name is absent.
		idx = 0
	}

	var out []string
	for reader.Next() {
		rec := reader.RecordBatch()
		if idx >= int(rec.NumCols()) {
			continue
		}
		col, ok := rec.Column(idx).(*array.String)
		if !ok {
			continue
		}
		for r := 0; r < col.Len(); r++ {
			if col.IsNull(r) {
				continue
			}
			// array.String.Value aliases the Arrow value buffer via unsafe; that
			// buffer is freed when the reader is released (immediately under the
			// CGO mallocator used by the shared library). Clone so the returned
			// names stay valid after Release, avoiding a use-after-free that
			// surfaced as catalog/schema names of spaces or zero length.
			out = append(out, strings.Clone(col.Value(r)))
		}
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// isPattern reports whether s contains SQL LIKE wildcards.
func isPattern(s string) bool {
	return strings.ContainsAny(s, "%_")
}

// escapeLike escapes single quotes so a value can be embedded in a SQL literal.
func escapeLike(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// filterByPattern keeps names matching the optional LIKE pattern (client-side).
func filterByPattern(names []string, pattern *string) []string {
	if pattern == nil || *pattern == "" || !isPattern(*pattern) {
		if pattern != nil && *pattern != "" {
			// Exact match requested.
			var out []string
			for _, n := range names {
				if n == *pattern {
					out = append(out, n)
				}
			}
			return out
		}
		return names
	}
	matcher := likeToMatcher(*pattern)
	var out []string
	for _, n := range names {
		if matcher(n) {
			out = append(out, n)
		}
	}
	return out
}

// likeToMatcher compiles a minimal SQL LIKE pattern (% and _) into a matcher.
func likeToMatcher(pattern string) func(string) bool {
	return func(s string) bool { return likeMatch(pattern, s) }
}

// likeMatch evaluates a SQL LIKE pattern against s, supporting % and _.
func likeMatch(pattern, s string) bool {
	p := []rune(pattern)
	str := []rune(s)
	var i, j, star, mark int
	star = -1
	for j < len(str) {
		if i < len(p) && (p[i] == '_' || p[i] == str[j]) {
			i++
			j++
		} else if i < len(p) && p[i] == '%' {
			star = i
			mark = j
			i++
		} else if star != -1 {
			i = star + 1
			mark++
			j = mark
		} else {
			return false
		}
	}
	for i < len(p) && p[i] == '%' {
		i++
	}
	return i == len(p)
}
