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
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	connect "github.com/HyukjinKwon/adbc-driver-spark/internal/sparkconnect/proto/spark/connect"
)

// buildArray builds a single-element Arrow array of the given type using the
// supplied append closure, and returns it. The caller is responsible for
// releasing the array.
func buildArray(t *testing.T, bldr array.Builder, appendFn func()) arrow.Array {
	t.Helper()
	appendFn()
	return bldr.NewArray()
}

// TestColumnToLiteralNull verifies a null value yields a Null literal type.
func TestColumnToLiteralNull(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewBooleanBuilder(alloc)
	defer bldr.Release()
	arr := buildArray(t, bldr, func() { bldr.AppendNull() })
	defer arr.Release()

	lit, err := columnToLiteral(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := lit.LiteralType.(*connect.Expression_Literal_Null); !ok {
		t.Errorf("expected Null literal, got %T", lit.LiteralType)
	}
}

// TestColumnToLiteralTypes covers every supported scalar Arrow type and asserts
// both the literal variant and its decoded value.
func TestColumnToLiteralTypes(t *testing.T) {
	alloc := memory.DefaultAllocator

	tests := []struct {
		name  string
		build func() arrow.Array
		check func(*testing.T, *connect.Expression_Literal)
	}{
		{
			name: "boolean",
			build: func() arrow.Array {
				b := array.NewBooleanBuilder(alloc)
				defer b.Release()
				b.Append(true)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Boolean); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if !lit.GetBoolean() {
					t.Error("want true")
				}
			},
		},
		{
			name: "int8",
			build: func() arrow.Array {
				b := array.NewInt8Builder(alloc)
				defer b.Release()
				b.Append(-5)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Byte); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetByte() != -5 {
					t.Errorf("got %d", lit.GetByte())
				}
			},
		},
		{
			name: "int16",
			build: func() arrow.Array {
				b := array.NewInt16Builder(alloc)
				defer b.Release()
				b.Append(-300)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Short); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetShort() != -300 {
					t.Errorf("got %d", lit.GetShort())
				}
			},
		},
		{
			name: "int32",
			build: func() arrow.Array {
				b := array.NewInt32Builder(alloc)
				defer b.Release()
				b.Append(123456)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Integer); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetInteger() != 123456 {
					t.Errorf("got %d", lit.GetInteger())
				}
			},
		},
		{
			name: "int64",
			build: func() arrow.Array {
				b := array.NewInt64Builder(alloc)
				defer b.Release()
				b.Append(9876543210)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Long); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetLong() != 9876543210 {
					t.Errorf("got %d", lit.GetLong())
				}
			},
		},
		{
			name: "uint8 widens to short",
			build: func() arrow.Array {
				b := array.NewUint8Builder(alloc)
				defer b.Release()
				b.Append(200)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Short); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetShort() != 200 {
					t.Errorf("got %d", lit.GetShort())
				}
			},
		},
		{
			name: "uint16 widens to integer",
			build: func() arrow.Array {
				b := array.NewUint16Builder(alloc)
				defer b.Release()
				b.Append(60000)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Integer); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetInteger() != 60000 {
					t.Errorf("got %d", lit.GetInteger())
				}
			},
		},
		{
			name: "uint32 widens to long",
			build: func() arrow.Array {
				b := array.NewUint32Builder(alloc)
				defer b.Release()
				b.Append(4000000000)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Long); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetLong() != 4000000000 {
					t.Errorf("got %d", lit.GetLong())
				}
			},
		},
		{
			name: "uint64 to long",
			build: func() arrow.Array {
				b := array.NewUint64Builder(alloc)
				defer b.Release()
				b.Append(42)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Long); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetLong() != 42 {
					t.Errorf("got %d", lit.GetLong())
				}
			},
		},
		{
			name: "float32",
			build: func() arrow.Array {
				b := array.NewFloat32Builder(alloc)
				defer b.Release()
				b.Append(1.5)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Float); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetFloat() != 1.5 {
					t.Errorf("got %v", lit.GetFloat())
				}
			},
		},
		{
			name: "float64",
			build: func() arrow.Array {
				b := array.NewFloat64Builder(alloc)
				defer b.Release()
				b.Append(2.25)
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Double); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetDouble() != 2.25 {
					t.Errorf("got %v", lit.GetDouble())
				}
			},
		},
		{
			name: "string",
			build: func() arrow.Array {
				b := array.NewStringBuilder(alloc)
				defer b.Release()
				b.Append("hello world")
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_String_); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				// Guards the use-after-free clone fix: value must be correct.
				if lit.GetString_() != "hello world" {
					t.Errorf("got %q", lit.GetString_())
				}
			},
		},
		{
			name: "large string",
			build: func() arrow.Array {
				b := array.NewLargeStringBuilder(alloc)
				defer b.Release()
				b.Append("big")
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_String_); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetString_() != "big" {
					t.Errorf("got %q", lit.GetString_())
				}
			},
		},
		{
			name: "binary",
			build: func() arrow.Array {
				b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
				defer b.Release()
				b.Append([]byte{0x01, 0x02, 0xff})
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Binary); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				// Guards the use-after-free clone fix: bytes must be correct.
				if !bytes.Equal(lit.GetBinary(), []byte{0x01, 0x02, 0xff}) {
					t.Errorf("got %v", lit.GetBinary())
				}
			},
		},
		{
			name: "large binary",
			build: func() arrow.Array {
				b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.LargeBinary)
				defer b.Release()
				b.Append([]byte{0xde, 0xad})
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Binary); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if !bytes.Equal(lit.GetBinary(), []byte{0xde, 0xad}) {
					t.Errorf("got %v", lit.GetBinary())
				}
			},
		},
		{
			name: "date32",
			build: func() arrow.Array {
				b := array.NewDate32Builder(alloc)
				defer b.Release()
				b.Append(arrow.Date32(19000))
				return b.NewArray()
			},
			check: func(t *testing.T, lit *connect.Expression_Literal) {
				if _, ok := lit.LiteralType.(*connect.Expression_Literal_Date); !ok {
					t.Fatalf("got %T", lit.LiteralType)
				}
				if lit.GetDate() != 19000 {
					t.Errorf("got %d", lit.GetDate())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr := tt.build()
			defer arr.Release()
			lit, err := columnToLiteral(arr, 0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.check(t, lit)
		})
	}
}

// TestColumnToLiteralStringValueSurvivesRelease asserts the string value is a
// clone, not an alias of the underlying Arrow buffer. We capture the literal,
// release the array, and confirm the value is still intact.
func TestColumnToLiteralStringValueSurvivesRelease(t *testing.T) {
	alloc := memory.DefaultAllocator
	b := array.NewStringBuilder(alloc)
	b.Append("persisted")
	arr := b.NewArray()
	b.Release()

	lit, err := columnToLiteral(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	arr.Release()

	if lit.GetString_() != "persisted" {
		t.Errorf("string value did not survive Release: %q", lit.GetString_())
	}
}

// TestColumnToLiteralUnsupported verifies an unsupported Arrow type yields a
// StatusNotImplemented adbc.Error.
func TestColumnToLiteralUnsupported(t *testing.T) {
	alloc := memory.DefaultAllocator
	// MonthInterval is not handled by columnToLiteral; the non-null value forces
	// the type switch (not the null branch) to run.
	b := array.NewMonthIntervalBuilder(alloc)
	defer b.Release()
	b.Append(arrow.MonthInterval(3))
	arr := b.NewArray()
	defer arr.Release()

	_, err := columnToLiteral(arr, 0)
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
	var ae adbc.Error
	if !errors.As(err, &ae) {
		t.Fatalf("expected adbc.Error, got %T", err)
	}
	if ae.Code != adbc.StatusNotImplemented {
		t.Errorf("Code = %v, want StatusNotImplemented", ae.Code)
	}
}
