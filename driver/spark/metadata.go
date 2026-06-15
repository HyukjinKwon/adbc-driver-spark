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
)

// tableTypesSchema is the schema returned by GetTableTypes.
var tableTypesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "table_type", Type: arrow.BinaryTypes.String, Nullable: false},
}, nil)

// sparkTableTypes are the table types Spark exposes through its catalog.
var sparkTableTypes = []string{"TABLE", "VIEW", "TEMPORARY"}

// buildTableTypes returns a single-batch reader listing Spark's table types.
func buildTableTypes(alloc memory.Allocator) array.RecordReader {
	bldr := array.NewRecordBuilder(alloc, tableTypesSchema)
	defer bldr.Release()

	col := bldr.Field(0).(*array.StringBuilder)
	for _, t := range sparkTableTypes {
		col.Append(t)
	}

	rec := bldr.NewRecord()
	defer rec.Release()
	rr, _ := array.NewRecordReader(tableTypesSchema, []arrow.Record{rec})
	return rr
}

// getInfoValueUnion is the dense union used for the info_value column. The type
// codes and member order match the ADBC standard exactly.
var getInfoValueUnion = arrow.DenseUnionOf([]arrow.Field{
	{Name: "string_value", Type: arrow.BinaryTypes.String},
	{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "int64_value", Type: arrow.PrimitiveTypes.Int64},
	{Name: "int32_bitmask", Type: arrow.PrimitiveTypes.Int32},
	{Name: "string_list", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	{Name: "int32_to_int32_list_map", Type: arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.ListOf(arrow.PrimitiveTypes.Int32))},
}, []arrow.UnionTypeCode{0, 1, 2, 3, 4, 5})

// getInfoSchema is the schema returned by GetInfo.
var getInfoSchema = arrow.NewSchema([]arrow.Field{
	{Name: "info_name", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
	{Name: "info_value", Type: getInfoValueUnion, Nullable: true},
}, nil)

// stringInfo holds the string-valued metadata this driver reports.
func (c *connection) stringInfo() map[adbc.InfoCode]string {
	return map[adbc.InfoCode]string{
		adbc.InfoVendorName:         VendorName,
		adbc.InfoVendorArrowVersion: arrow.PkgVersion,
		adbc.InfoDriverName:         DriverName,
		adbc.InfoDriverVersion:      Version,
		adbc.InfoDriverArrowVersion: arrow.PkgVersion,
	}
}

// buildInfo returns driver/vendor metadata for the requested info codes. If
// infoCodes is empty, every known code is returned.
func (c *connection) buildInfo(_ context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	strInfo := c.stringInfo()

	want := infoCodes
	if len(want) == 0 {
		want = make([]adbc.InfoCode, 0, len(strInfo))
		for code := range strInfo {
			want = append(want, code)
		}
	}

	bldr := array.NewRecordBuilder(c.alloc, getInfoSchema)
	defer bldr.Release()

	nameBldr := bldr.Field(0).(*array.Uint32Builder)
	valueBldr := bldr.Field(1).(*array.DenseUnionBuilder)
	strChild := valueBldr.Child(0).(*array.StringBuilder)

	for _, code := range want {
		if v, ok := strInfo[code]; ok {
			nameBldr.Append(uint32(code))
			valueBldr.Append(0)
			strChild.Append(v)
		}
	}

	rec := bldr.NewRecord()
	defer rec.Release()
	rr, _ := array.NewRecordReader(getInfoSchema, []arrow.Record{rec})
	return rr, nil
}

// Nested schemas for GetObjects. These mirror the ADBC standard precisely so
// that consumers reading specific fields by name interoperate correctly.
var (
	usageSchema = arrow.StructOf(
		arrow.Field{Name: "fk_catalog", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "fk_db_schema", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "fk_table", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "fk_column_name", Type: arrow.BinaryTypes.String, Nullable: false},
	)

	constraintSchema = arrow.StructOf(
		arrow.Field{Name: "constraint_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "constraint_type", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "constraint_column_names", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
		arrow.Field{Name: "constraint_column_usage", Type: arrow.ListOf(usageSchema), Nullable: true},
	)

	columnSchema = arrow.StructOf(
		arrow.Field{Name: "column_name", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "ordinal_position", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "remarks", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_data_type", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_type_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_column_size", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "xdbc_decimal_digits", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_num_prec_radix", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_nullable", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_column_def", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_sql_data_type", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_datetime_sub", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_char_octet_length", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "xdbc_is_nullable", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_scope_catalog", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_scope_schema", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_scope_table", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_is_autoincrement", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		arrow.Field{Name: "xdbc_is_generatedcolumn", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	)

	tableSchema = arrow.StructOf(
		arrow.Field{Name: "table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "table_type", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "table_columns", Type: arrow.ListOf(columnSchema), Nullable: true},
		arrow.Field{Name: "table_constraints", Type: arrow.ListOf(constraintSchema), Nullable: true},
	)

	dbSchemaSchema = arrow.StructOf(
		arrow.Field{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "db_schema_tables", Type: arrow.ListOf(tableSchema), Nullable: true},
	)

	getObjectsSchema = arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "catalog_db_schemas", Type: arrow.ListOf(dbSchemaSchema), Nullable: true},
	}, nil)
)
