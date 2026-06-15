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

// TestParseConnectionStringDefaults verifies the default values applied to a
// freshly parsed minimal connection string.
func TestParseConnectionStringDefaults(t *testing.T) {
	cfg, err := ParseConnectionString("sc://host")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != defaultPort {
		t.Errorf("Port = %d, want %d", cfg.Port, defaultPort)
	}
	if cfg.UserAgent != defaultUserAgent {
		t.Errorf("UserAgent = %q, want %q", cfg.UserAgent, defaultUserAgent)
	}
	if cfg.UseTLS {
		t.Error("UseTLS should default to false")
	}
	if cfg.Headers == nil {
		t.Error("Headers map should be initialized")
	}
	if cfg.Token != "" || cfg.UserID != "" || cfg.SessionID != "" {
		t.Errorf("expected empty token/user/session, got %+v", *cfg)
	}
}

// TestParseConnectionStringSchemes verifies scheme handling, including TLS
// inference and rejection of unsupported schemes.
func TestParseConnectionStringSchemes(t *testing.T) {
	tests := []struct {
		name       string
		conn       string
		wantTLS    bool
		wantHost   string
		wantErr    bool
		wantPortIs int
	}{
		{name: "sc scheme", conn: "sc://h:1", wantHost: "h", wantPortIs: 1},
		{name: "grpc scheme", conn: "grpc://h:1", wantHost: "h", wantPortIs: 1},
		{name: "grpcs scheme implies tls", conn: "grpcs://h:1", wantTLS: true, wantHost: "h", wantPortIs: 1},
		{name: "sc+tls scheme implies tls", conn: "sc+tls://h:1", wantTLS: true, wantHost: "h", wantPortIs: 1},
		{name: "no scheme", conn: "h:1", wantHost: "h", wantPortIs: 1},
		{name: "uppercase scheme normalized", conn: "SC://h:1", wantHost: "h", wantPortIs: 1},
		{name: "unsupported scheme", conn: "http://h:1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConnectionString(tt.conn)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tt.conn)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.UseTLS != tt.wantTLS {
				t.Errorf("UseTLS = %v, want %v", cfg.UseTLS, tt.wantTLS)
			}
			if cfg.Host != tt.wantHost {
				t.Errorf("Host = %q, want %q", cfg.Host, tt.wantHost)
			}
			if cfg.Port != tt.wantPortIs {
				t.Errorf("Port = %d, want %d", cfg.Port, tt.wantPortIs)
			}
		})
	}
}

// TestParseConnectionStringParams verifies the recognized parameter keys and
// their aliases populate the correct Config fields.
func TestParseConnectionStringParams(t *testing.T) {
	conn := "sc://host:443/;" +
		"token=secret;" +
		"user_id=alice;" +
		"user_agent=myagent;" +
		"session_id=sess-123;" +
		"use_ssl=true"
	cfg, err := ParseConnectionString(conn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token != "secret" {
		t.Errorf("Token = %q", cfg.Token)
	}
	if cfg.UserID != "alice" {
		t.Errorf("UserID = %q", cfg.UserID)
	}
	if cfg.UserAgent != "myagent" {
		t.Errorf("UserAgent = %q", cfg.UserAgent)
	}
	if cfg.SessionID != "sess-123" {
		t.Errorf("SessionID = %q", cfg.SessionID)
	}
	if !cfg.UseTLS {
		t.Error("UseTLS should be true")
	}
	if cfg.Port != 443 {
		t.Errorf("Port = %d, want 443", cfg.Port)
	}
}

// TestParseConnectionStringParamAliases verifies the alternate spellings of the
// known parameter keys.
func TestParseConnectionStringParamAliases(t *testing.T) {
	tests := []struct {
		name  string
		conn  string
		check func(*testing.T, *Config)
	}{
		{"userid alias", "sc://h/;userid=bob", func(t *testing.T, c *Config) {
			if c.UserID != "bob" {
				t.Errorf("UserID = %q", c.UserID)
			}
		}},
		{"user alias", "sc://h/;user=carol", func(t *testing.T, c *Config) {
			if c.UserID != "carol" {
				t.Errorf("UserID = %q", c.UserID)
			}
		}},
		{"useragent alias", "sc://h/;useragent=ua", func(t *testing.T, c *Config) {
			if c.UserAgent != "ua" {
				t.Errorf("UserAgent = %q", c.UserAgent)
			}
		}},
		{"sessionid alias", "sc://h/;sessionid=s", func(t *testing.T, c *Config) {
			if c.SessionID != "s" {
				t.Errorf("SessionID = %q", c.SessionID)
			}
		}},
		{"usessl alias false", "sc://h/;usessl=false", func(t *testing.T, c *Config) {
			if c.UseTLS {
				t.Error("UseTLS should be false")
			}
		}},
		{"use_tls alias", "sc://h/;use_tls=true", func(t *testing.T, c *Config) {
			if !c.UseTLS {
				t.Error("UseTLS should be true")
			}
		}},
		{"usetls alias", "sc://h/;usetls=1", func(t *testing.T, c *Config) {
			if !c.UseTLS {
				t.Error("UseTLS should be true")
			}
		}},
		{"ssl alias", "sc://h/;ssl=true", func(t *testing.T, c *Config) {
			if !c.UseTLS {
				t.Error("UseTLS should be true")
			}
		}},
		{"key case-insensitive", "sc://h/;TOKEN=tok", func(t *testing.T, c *Config) {
			if c.Token != "tok" {
				t.Errorf("Token = %q", c.Token)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConnectionString(tt.conn)
			if err != nil {
				t.Fatal(err)
			}
			tt.check(t, cfg)
		})
	}
}

// TestParseConnectionStringHeaders verifies that unknown params become headers,
// including multiple custom headers and URL-escaped values.
func TestParseConnectionStringHeaders(t *testing.T) {
	cfg, err := ParseConnectionString("sc://host/;x-one=a;x-two=b%20c")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Headers["x-one"] != "a" {
		t.Errorf("x-one = %q", cfg.Headers["x-one"])
	}
	// %20 should be unescaped to a space.
	if cfg.Headers["x-two"] != "b c" {
		t.Errorf("x-two = %q, want %q", cfg.Headers["x-two"], "b c")
	}
}

// TestParseConnectionStringBareSemicolonParams verifies the bare ";" form (no
// trailing slash before the parameter list) is accepted.
func TestParseConnectionStringBareSemicolonParams(t *testing.T) {
	cfg, err := ParseConnectionString("sc://host:1234;token=t")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "host" || cfg.Port != 1234 || cfg.Token != "t" {
		t.Errorf("got %+v", *cfg)
	}
}

// TestParseConnectionStringTrailingSlash verifies a trailing slash with no
// params is stripped from the host.
func TestParseConnectionStringTrailingSlash(t *testing.T) {
	cfg, err := ParseConnectionString("sc://host:1/")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "host" || cfg.Port != 1 {
		t.Errorf("got host=%q port=%d", cfg.Host, cfg.Port)
	}
}

// TestParseConnectionStringErrors verifies malformed inputs are rejected.
func TestParseConnectionStringErrors(t *testing.T) {
	tests := []struct {
		name string
		conn string
	}{
		{"empty", ""},
		{"unsupported scheme", "ftp://h"},
		{"missing host", "sc://"},
		{"missing host with params", "sc:///;token=t"},
		{"bad port", "sc://h:abc"},
		{"malformed param no equals", "sc://h/;justakey"},
		{"invalid bool", "sc://h/;use_ssl=notabool"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseConnectionString(tt.conn); err == nil {
				t.Fatalf("expected error for %q", tt.conn)
			}
		})
	}
}

// TestParseConnectionStringIPv6 verifies bracketed IPv6 literals with and
// without a port.
func TestParseConnectionStringIPv6(t *testing.T) {
	cfg, err := ParseConnectionString("sc://[2001:db8::1]:9999")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "2001:db8::1" || cfg.Port != 9999 {
		t.Errorf("got host=%q port=%d", cfg.Host, cfg.Port)
	}

	cfg, err = ParseConnectionString("sc://[::1]")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "::1" || cfg.Port != defaultPort {
		t.Errorf("got host=%q port=%d, want ::1 and default port", cfg.Host, cfg.Port)
	}
}

// TestEndpointFormatting verifies Endpoint joins host and port.
func TestEndpointFormatting(t *testing.T) {
	cfg, err := ParseConnectionString("sc://example.com:7077")
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Endpoint(); got != "example.com:7077" {
		t.Errorf("Endpoint() = %q", got)
	}
}
