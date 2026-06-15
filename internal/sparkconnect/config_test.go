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

import "testing"

func TestParseConnectionString(t *testing.T) {
	tests := []struct {
		name    string
		conn    string
		want    Config
		wantErr bool
	}{
		{
			name: "host and port",
			conn: "sc://localhost:15002",
			want: Config{Host: "localhost", Port: 15002, UserAgent: defaultUserAgent},
		},
		{
			name: "default port",
			conn: "sc://example.com",
			want: Config{Host: "example.com", Port: defaultPort, UserAgent: defaultUserAgent},
		},
		{
			name: "params with token and user",
			conn: "sc://host:443/;token=secret;user_id=alice;use_ssl=true",
			want: Config{Host: "host", Port: 443, Token: "secret", UserID: "alice", UseTLS: true, UserAgent: defaultUserAgent},
		},
		{
			name: "bare host:port without scheme",
			conn: "127.0.0.1:15002",
			want: Config{Host: "127.0.0.1", Port: 15002, UserAgent: defaultUserAgent},
		},
		{
			name: "ipv6 literal",
			conn: "sc://[::1]:15002",
			want: Config{Host: "::1", Port: 15002, UserAgent: defaultUserAgent},
		},
		{
			name:    "empty",
			conn:    "",
			wantErr: true,
		},
		{
			name:    "bad port",
			conn:    "sc://host:notaport",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseConnectionString(tt.conn)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Host != tt.want.Host || got.Port != tt.want.Port ||
				got.Token != tt.want.Token || got.UserID != tt.want.UserID ||
				got.UseTLS != tt.want.UseTLS || got.UserAgent != tt.want.UserAgent {
				t.Errorf("ParseConnectionString(%q) = %+v, want %+v", tt.conn, *got, tt.want)
			}
		})
	}
}

func TestEndpoint(t *testing.T) {
	cfg := &Config{Host: "h", Port: 123}
	if got := cfg.Endpoint(); got != "h:123" {
		t.Errorf("Endpoint() = %q, want %q", got, "h:123")
	}
}

func TestExtraParamsBecomeHeaders(t *testing.T) {
	cfg, err := ParseConnectionString("sc://host/;x-custom=value")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Headers["x-custom"] != "value" {
		t.Errorf("expected header x-custom=value, got %v", cfg.Headers)
	}
}
