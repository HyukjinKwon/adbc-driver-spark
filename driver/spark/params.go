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
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	connect "github.com/HyukjinKwon/adbc-driver-spark/internal/sparkconnect/proto/spark/connect"
)

// columnToLiteral converts the value at the given row of an Arrow array into a
// Spark Connect SQL literal. It supports the common scalar types used for
// parameter binding; unsupported types yield a StatusNotImplemented error.
func columnToLiteral(col arrow.Array, row int) (*connect.Expression_Literal, error) {
	if col.IsNull(row) {
		return &connect.Expression_Literal{
			LiteralType: &connect.Expression_Literal_Null{},
		}, nil
	}

	lit := &connect.Expression_Literal{}
	switch arr := col.(type) {
	case *array.Boolean:
		lit.LiteralType = &connect.Expression_Literal_Boolean{Boolean: arr.Value(row)}
	case *array.Int8:
		lit.LiteralType = &connect.Expression_Literal_Byte{Byte: int32(arr.Value(row))}
	case *array.Int16:
		lit.LiteralType = &connect.Expression_Literal_Short{Short: int32(arr.Value(row))}
	case *array.Int32:
		lit.LiteralType = &connect.Expression_Literal_Integer{Integer: arr.Value(row)}
	case *array.Int64:
		lit.LiteralType = &connect.Expression_Literal_Long{Long: arr.Value(row)}
	case *array.Uint8:
		lit.LiteralType = &connect.Expression_Literal_Short{Short: int32(arr.Value(row))}
	case *array.Uint16:
		lit.LiteralType = &connect.Expression_Literal_Integer{Integer: int32(arr.Value(row))}
	case *array.Uint32:
		lit.LiteralType = &connect.Expression_Literal_Long{Long: int64(arr.Value(row))}
	case *array.Uint64:
		v := arr.Value(row)
		if v > math.MaxInt64 {
			return nil, adbc.Error{
				Msg:  fmt.Sprintf("spark: uint64 parameter %d exceeds the maximum bind value (Spark long is signed)", v),
				Code: adbc.StatusInvalidArgument,
			}
		}
		lit.LiteralType = &connect.Expression_Literal_Long{Long: int64(v)}
	case *array.Float32:
		lit.LiteralType = &connect.Expression_Literal_Float{Float: arr.Value(row)}
	case *array.Float64:
		lit.LiteralType = &connect.Expression_Literal_Double{Double: arr.Value(row)}
	// String and Binary values from arrow-go alias the underlying buffer (via
	// unsafe), which is freed when the bound record is released, immediately so
	// under the CGO mallocator. Clone so the literal survives until the plan is
	// marshaled and sent.
	case *array.String:
		lit.LiteralType = &connect.Expression_Literal_String_{String_: strings.Clone(arr.Value(row))}
	case *array.LargeString:
		lit.LiteralType = &connect.Expression_Literal_String_{String_: strings.Clone(arr.Value(row))}
	case *array.Binary:
		lit.LiteralType = &connect.Expression_Literal_Binary{Binary: bytes.Clone(arr.Value(row))}
	case *array.LargeBinary:
		lit.LiteralType = &connect.Expression_Literal_Binary{Binary: bytes.Clone(arr.Value(row))}
	case *array.Date32:
		lit.LiteralType = &connect.Expression_Literal_Date{Date: int32(arr.Value(row))}
	case *array.Date64:
		// Date64 stores milliseconds since the epoch; Spark's date literal is in
		// days. Floor-divide so any intra-day component is dropped.
		const msPerDay = 24 * 60 * 60 * 1000
		lit.LiteralType = &connect.Expression_Literal_Date{Date: int32(int64(arr.Value(row)) / msPerDay)}
	case *array.Timestamp:
		tt, ok := arr.DataType().(*arrow.TimestampType)
		if !ok {
			return nil, adbc.Error{Msg: "spark: timestamp column has no TimestampType", Code: adbc.StatusInternal}
		}
		micros, err := timestampToMicros(int64(arr.Value(row)), tt.Unit)
		if err != nil {
			return nil, err
		}
		// A timestamp carrying a time zone denotes an absolute instant, which maps
		// to Spark's TIMESTAMP; a zone-less timestamp is wall-clock, which maps to
		// TIMESTAMP_NTZ. The stored value is micros-since-epoch in both cases.
		if tt.TimeZone != "" {
			lit.LiteralType = &connect.Expression_Literal_Timestamp{Timestamp: micros}
		} else {
			lit.LiteralType = &connect.Expression_Literal_TimestampNtz{TimestampNtz: micros}
		}
	case *array.Decimal128:
		dt := arr.DataType().(*arrow.Decimal128Type)
		prec, scale := dt.Precision, dt.Scale
		lit.LiteralType = &connect.Expression_Literal_Decimal_{Decimal: &connect.Expression_Literal_Decimal{
			Value:     arr.Value(row).ToString(scale),
			Precision: &prec,
			Scale:     &scale,
		}}
	case *array.Decimal256:
		dt := arr.DataType().(*arrow.Decimal256Type)
		prec, scale := dt.Precision, dt.Scale
		lit.LiteralType = &connect.Expression_Literal_Decimal_{Decimal: &connect.Expression_Literal_Decimal{
			Value:     arr.Value(row).ToString(scale),
			Precision: &prec,
			Scale:     &scale,
		}}
	default:
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("spark: cannot bind parameter of Arrow type %s", col.DataType()),
			Code: adbc.StatusNotImplemented,
		}
	}
	return lit, nil
}

// timestampToMicros converts an Arrow timestamp value to microseconds since the
// UNIX epoch, the unit Spark Connect timestamp literals use. Nanosecond inputs
// are truncated to microsecond resolution, matching Spark's precision.
func timestampToMicros(v int64, unit arrow.TimeUnit) (int64, error) {
	switch unit {
	case arrow.Second:
		return v * 1_000_000, nil
	case arrow.Millisecond:
		return v * 1_000, nil
	case arrow.Microsecond:
		return v, nil
	case arrow.Nanosecond:
		return v / 1_000, nil
	default:
		return 0, adbc.Error{
			Msg:  fmt.Sprintf("spark: unsupported timestamp unit %v", unit),
			Code: adbc.StatusNotImplemented,
		}
	}
}
