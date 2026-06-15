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
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/HyukjinKwon/adbc-driver-spark/internal/sparkconnect"
)

// database is the concrete adbc.Database implementation. It holds the parsed
// connection configuration and produces connections that share it.
type database struct {
	alloc memory.Allocator

	mu  sync.Mutex
	cfg *sparkconnect.Config
}

var _ adbc.Database = (*database)(nil)

// SetOptions applies the supplied options to the database. It may be called
// before Open to configure the connection. Recognized keys are documented on
// the OptionKey* constants.
func (d *database) SetOptions(opts map[string]string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	uri := firstNonEmpty(opts[OptionKeyURI], opts["adbc.spark.uri"])
	if uri == "" && d.cfg == nil {
		return adbc.Error{
			Msg:  "spark: a connection URI is required (set the \"uri\" option to an sc:// string)",
			Code: adbc.StatusInvalidArgument,
		}
	}

	cfg := d.cfg
	if uri != "" {
		parsed, err := sparkconnect.ParseConnectionString(uri)
		if err != nil {
			return adbc.Error{Msg: err.Error(), Code: adbc.StatusInvalidArgument}
		}
		cfg = parsed
	}

	for key, value := range opts {
		switch key {
		case OptionKeyURI, "adbc.spark.uri":
			// already handled
		case OptionKeyToken, adbc.OptionKeyPassword:
			cfg.Token = value
		case OptionKeyUserID, adbc.OptionKeyUsername:
			cfg.UserID = value
		case OptionKeyUserAgent:
			cfg.UserAgent = value
		case OptionKeySessionID:
			cfg.SessionID = value
		case OptionKeyTLSEnabled:
			b, err := strconv.ParseBool(value)
			if err != nil {
				return adbc.Error{
					Msg:  "spark: invalid boolean for " + OptionKeyTLSEnabled + ": " + value,
					Code: adbc.StatusInvalidArgument,
				}
			}
			cfg.UseTLS = b
		default:
			// Any "adbc.spark.headers.<NAME>" key sets a gRPC metadata header,
			// mirroring how the connection string forwards unknown URI parameters
			// as headers. This keeps the option and URI paths at parity.
			if name, ok := strings.CutPrefix(key, OptionKeyHeaderPrefix); ok && name != "" {
				if cfg.Headers == nil {
					cfg.Headers = map[string]string{}
				}
				cfg.Headers[name] = value
				continue
			}
			// Reject unrecognized driver-specific keys rather than silently
			// dropping them (a common cause of "my token was ignored"). Other
			// standard adbc.* keys are accepted but unused.
			if strings.HasPrefix(key, "adbc.spark.") {
				return adbc.Error{
					Msg:  "spark: unknown option " + key,
					Code: adbc.StatusNotImplemented,
				}
			}
		}
	}

	d.cfg = cfg
	return nil
}

// Open establishes a Spark Connect session and returns a live connection.
func (d *database) Open(ctx context.Context) (adbc.Connection, error) {
	d.mu.Lock()
	cfg := d.cfg
	d.mu.Unlock()

	if cfg == nil {
		return nil, adbc.Error{
			Msg:  "spark: database is not configured; set the \"uri\" option first",
			Code: adbc.StatusInvalidState,
		}
	}

	client, err := sparkconnect.Dial(ctx, cfg, d.alloc)
	if err != nil {
		return nil, adbc.Error{Msg: err.Error(), Code: adbc.StatusIO}
	}
	return &connection{db: d, client: client, alloc: d.alloc}, nil
}

// Close releases resources associated with the database. Connections opened
// from it have independent lifetimes and must be closed separately.
func (d *database) Close() error { return nil }

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
