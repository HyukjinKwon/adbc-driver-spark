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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNewDriverAndDatabaseValidation(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	if _, err := drv.NewDatabase(map[string]string{}); err == nil {
		t.Fatal("expected error when no URI is provided")
	}
	db, err := drv.NewDatabase(map[string]string{adbc.OptionKeyURI: "sc://localhost:15002"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer db.Close()
}

func TestBuildTableTypes(t *testing.T) {
	rr := buildTableTypes(memory.DefaultAllocator)
	defer rr.Release()

	var got []string
	for rr.Next() {
		col := rr.RecordBatch().Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			got = append(got, col.Value(i))
		}
	}
	if rr.Err() != nil {
		t.Fatal(rr.Err())
	}
	if len(got) != len(sparkTableTypes) {
		t.Fatalf("got %d table types, want %d", len(got), len(sparkTableTypes))
	}
}

func TestBuildInfo(t *testing.T) {
	c := &connection{alloc: memory.DefaultAllocator}
	rr, err := c.buildInfo(context.Background(), []adbc.InfoCode{adbc.InfoDriverName, adbc.InfoVendorName})
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()

	rows := 0
	for rr.Next() {
		rows += int(rr.RecordBatch().NumRows())
	}
	if rr.Err() != nil {
		t.Fatal(rr.Err())
	}
	if rows != 2 {
		t.Fatalf("expected 2 info rows, got %d", rows)
	}
}

func TestLikeMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"%", "anything", true},
		{"a%", "abc", true},
		{"a%", "xbc", false},
		{"a_c", "abc", true},
		{"a_c", "ac", false},
		{"default", "default", true},
		{"def%", "default", true},
	}
	for _, tc := range cases {
		if got := likeMatch(tc.pattern, tc.s); got != tc.want {
			t.Errorf("likeMatch(%q, %q) = %v, want %v", tc.pattern, tc.s, got, tc.want)
		}
	}
}

func TestQuoteIdent(t *testing.T) {
	if got := quoteIdent("ab`c"); got != "`ab``c`" {
		t.Errorf("quoteIdent = %q", got)
	}
}
