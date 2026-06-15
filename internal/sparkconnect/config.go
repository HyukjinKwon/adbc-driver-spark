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

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Config captures everything required to dial a Spark Connect endpoint. It is
// produced by parsing a Spark Connect connection string (the "sc://" form used
// by the official clients) and then optionally overlaid with explicit options.
type Config struct {
	// Host is the gRPC host of the Spark Connect server.
	Host string
	// Port is the gRPC port of the Spark Connect server.
	Port int
	// UseTLS enables transport-level security (TLS) for the gRPC channel.
	UseTLS bool
	// Token is an optional bearer token sent as an Authorization header.
	Token string
	// UserID identifies the user for the remote session.
	UserID string
	// UserAgent is reported to the server for telemetry and debugging.
	UserAgent string
	// SessionID, when set, pins the client to a specific server-side session.
	SessionID string
	// Headers are extra gRPC metadata headers sent on every request.
	Headers map[string]string
}

const (
	defaultPort      = 15002
	defaultUserAgent = "adbc-driver-spark"
	scScheme         = "sc"
)

// ParseConnectionString parses a Spark Connect connection string of the form
//
//	sc://host:port/;key=value;key=value
//
// It also tolerates plain "host:port" and "grpc://"/"grpcs://" forms so that
// the driver is forgiving about the exact spelling callers use.
func ParseConnectionString(conn string) (*Config, error) {
	cfg := &Config{
		Port:      defaultPort,
		UserAgent: defaultUserAgent,
		Headers:   map[string]string{},
	}
	if conn == "" {
		return nil, fmt.Errorf("spark connect: empty connection string")
	}

	scheme := ""
	rest := conn
	if i := strings.Index(conn, "://"); i >= 0 {
		scheme = strings.ToLower(conn[:i])
		rest = conn[i+3:]
	}

	switch scheme {
	case "", scScheme, "grpc":
		cfg.UseTLS = false
	case "grpcs", "sc+tls":
		cfg.UseTLS = true
	default:
		return nil, fmt.Errorf("spark connect: unsupported scheme %q", scheme)
	}

	// Split host[:port] from the parameter list. Parameters are introduced by
	// the first "/;" (canonical Spark form) or a bare ";".
	hostPort := rest
	params := ""
	if i := strings.Index(rest, "/;"); i >= 0 {
		hostPort = rest[:i]
		params = rest[i+2:]
	} else if i := strings.Index(rest, ";"); i >= 0 {
		hostPort = rest[:i]
		params = rest[i+1:]
	} else if i := strings.Index(rest, "/"); i >= 0 {
		hostPort = rest[:i]
	}
	hostPort = strings.TrimSuffix(hostPort, "/")

	if hostPort == "" {
		return nil, fmt.Errorf("spark connect: missing host in connection string %q", conn)
	}
	if host, port, ok := splitHostPort(hostPort); ok {
		cfg.Host = host
		if port != "" {
			p, err := strconv.Atoi(port)
			if err != nil {
				return nil, fmt.Errorf("spark connect: invalid port %q: %w", port, err)
			}
			cfg.Port = p
		}
	} else {
		cfg.Host = hostPort
	}

	if params != "" {
		for _, kv := range strings.Split(params, ";") {
			if kv == "" {
				continue
			}
			key, value, found := strings.Cut(kv, "=")
			if !found {
				return nil, fmt.Errorf("spark connect: malformed parameter %q", kv)
			}
			if v, err := url.QueryUnescape(value); err == nil {
				value = v
			}
			if err := cfg.applyParam(strings.ToLower(strings.TrimSpace(key)), value); err != nil {
				return nil, err
			}
		}
	}
	return cfg, nil
}

func (c *Config) applyParam(key, value string) error {
	switch key {
	case "user_id", "userid", "user":
		c.UserID = value
	case "token":
		c.Token = value
	case "user_agent", "useragent":
		c.UserAgent = value
	case "session_id", "sessionid":
		c.SessionID = value
	case "use_ssl", "usessl", "use_tls", "usetls", "ssl":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("spark connect: invalid boolean for %q: %w", key, err)
		}
		c.UseTLS = b
	default:
		// Unknown parameters are forwarded as gRPC headers, matching the
		// extensibility of the reference Spark Connect clients.
		c.Headers[key] = value
	}
	return nil
}

func splitHostPort(s string) (host, port string, ok bool) {
	// IPv6 literal in brackets, e.g. [::1]:15002
	if strings.HasPrefix(s, "[") {
		end := strings.Index(s, "]")
		if end < 0 {
			return "", "", false
		}
		host = s[1:end]
		if len(s) > end+1 && s[end+1] == ':' {
			return host, s[end+2:], true
		}
		return host, "", true
	}
	if i := strings.LastIndex(s, ":"); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", true
}

// Endpoint returns the host:port string used to dial gRPC.
func (c *Config) Endpoint() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
