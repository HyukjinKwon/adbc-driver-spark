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
	"fmt"

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
		lit.LiteralType = &connect.Expression_Literal_Long{Long: int64(arr.Value(row))}
	case *array.Float32:
		lit.LiteralType = &connect.Expression_Literal_Float{Float: arr.Value(row)}
	case *array.Float64:
		lit.LiteralType = &connect.Expression_Literal_Double{Double: arr.Value(row)}
	case *array.String:
		lit.LiteralType = &connect.Expression_Literal_String_{String_: arr.Value(row)}
	case *array.LargeString:
		lit.LiteralType = &connect.Expression_Literal_String_{String_: arr.Value(row)}
	case *array.Binary:
		lit.LiteralType = &connect.Expression_Literal_Binary{Binary: arr.Value(row)}
	case *array.LargeBinary:
		lit.LiteralType = &connect.Expression_Literal_Binary{Binary: arr.Value(row)}
	case *array.Date32:
		lit.LiteralType = &connect.Expression_Literal_Date{Date: int32(arr.Value(row))}
	default:
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("spark: cannot bind parameter of Arrow type %s", col.DataType()),
			Code: adbc.StatusNotImplemented,
		}
	}
	return lit, nil
}
