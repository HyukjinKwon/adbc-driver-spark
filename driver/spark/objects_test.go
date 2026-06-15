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
	"reflect"
	"testing"
)

// TestQuoteIdentEdgeCases verifies backtick escaping and various identifiers.
func TestQuoteIdentEdgeCases(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"table", "`table`"},
		{"", "``"},
		{"a`b", "`a``b`"},
		{"`", "````"},
		{"a``b", "`a````b`"},
		{"my.table", "`my.table`"},
		{"with space", "`with space`"},
	}
	for _, tt := range tests {
		if got := quoteIdent(tt.in); got != tt.want {
			t.Errorf("quoteIdent(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestIsPattern verifies wildcard detection.
func TestIsPattern(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"plain", false},
		{"", false},
		{"foo%", true},
		{"f_o", true},
		{"%", true},
		{"_", true},
		{"a%b_c", true},
		{"no-wildcards-here", false},
	}
	for _, tt := range tests {
		if got := isPattern(tt.in); got != tt.want {
			t.Errorf("isPattern(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

// TestEscapeLike verifies single-quote escaping for SQL literals.
func TestEscapeLike(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"plain", "plain"},
		{"O'Brien", "O''Brien"},
		{"''", "''''"},
		{"no quotes", "no quotes"},
		{"%pattern_", "%pattern_"},
	}
	for _, tt := range tests {
		if got := escapeLike(tt.in); got != tt.want {
			t.Errorf("escapeLike(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func strPtr(s string) *string { return &s }

// TestFilterByPattern verifies the client-side pattern/exact filtering.
func TestFilterByPattern(t *testing.T) {
	names := []string{"default", "demo", "prod", "preprod"}

	tests := []struct {
		name    string
		pattern *string
		want    []string
	}{
		{"nil pattern returns all", nil, names},
		{"empty pattern returns all", strPtr(""), names},
		{"exact match", strPtr("prod"), []string{"prod"}},
		{"exact no match", strPtr("missing"), nil},
		{"prefix wildcard", strPtr("de%"), []string{"default", "demo"}},
		{"contains wildcard", strPtr("%prod"), []string{"prod", "preprod"}},
		{"single char wildcard", strPtr("dem_"), []string{"demo"}},
		{"match all", strPtr("%"), names},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterByPattern(names, tt.pattern)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterByPattern(%v) = %v, want %v", tt.pattern, got, tt.want)
			}
		})
	}
}

// TestLikeMatchMore extends coverage of the LIKE matcher with trickier cases.
func TestLikeMatchMore(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"", "", true},
		{"", "x", false},
		{"%%", "anything", true},
		{"a%b", "axxxb", true},
		{"a%b", "axxxc", false},
		{"%abc", "xyzabc", true},
		{"%abc", "abcx", false},
		{"a_c_e", "abcde", true},
		{"a_c_e", "abcdef", false},
		{"abc", "abc", true},
		{"abc", "abcd", false},
	}
	for _, tt := range tests {
		if got := likeMatch(tt.pattern, tt.s); got != tt.want {
			t.Errorf("likeMatch(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.want)
		}
	}
}
