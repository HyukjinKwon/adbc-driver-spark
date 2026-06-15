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

// Package spark implements an Apache Arrow Database Connectivity (ADBC) driver
// for Apache Spark Connect.
//
// The driver speaks the Spark Connect gRPC protocol directly, so it requires no
// local Spark or JVM installation. Query results are returned as native Apache
// Arrow data with zero additional copies, which makes it a natural fit for the
// Arrow-based analytics ecosystem (pandas, Polars, DuckDB, and anything else
// that consumes ADBC).
//
// The simplest way to use the driver from Go is:
//
//	drv := spark.NewDriver(memory.DefaultAllocator)
//	db, err := drv.NewDatabase(map[string]string{
//		adbc.OptionKeyURI: "sc://localhost:15002",
//	})
//	// handle err
//	defer db.Close()
//
//	cnxn, err := db.Open(ctx)
//	// handle err
//	defer cnxn.Close()
//
//	stmt, err := cnxn.NewStatement()
//	// handle err
//	defer stmt.Close()
//
//	stmt.SetSqlQuery("SELECT 1 AS id")
//	reader, _, err := stmt.ExecuteQuery(ctx)
//	// consume reader ...
package spark

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Driver and vendor identification reported through GetInfo.
const (
	// DriverName is the human-readable name of this driver.
	DriverName = "ADBC Spark Connect Driver (Go)"
	// VendorName identifies the database product this driver targets.
	VendorName = "Apache Spark"
)

// Version is the semantic version of the driver. It is overridden at build
// time for release artifacts via -ldflags.
var Version = "0.1.0"

// Driver-specific option keys. The standard adbc.OptionKey* keys are accepted
// where they are meaningful (for example adbc.OptionKeyURI, OptionKeyUsername,
// and OptionKeyPassword); the keys below expose Spark Connect specifics.
const (
	// OptionKeyURI is the Spark Connect connection string, e.g.
	// "sc://host:port/;token=...;user_id=...". Equivalent to adbc.OptionKeyURI.
	OptionKeyURI = adbc.OptionKeyURI
	// OptionKeyToken sets the bearer token used for authentication.
	OptionKeyToken = "adbc.spark.token"
	// OptionKeyUserID sets the Spark user id for the remote session.
	OptionKeyUserID = "adbc.spark.user_id"
	// OptionKeyUserAgent sets the user agent reported to the server.
	OptionKeyUserAgent = "adbc.spark.user_agent"
	// OptionKeySessionID pins the client to a specific server-side session id.
	OptionKeySessionID = "adbc.spark.session_id"
	// OptionKeyTLSEnabled forces TLS on ("true") or off ("false"), overriding
	// whatever the connection string implies.
	OptionKeyTLSEnabled = "adbc.spark.tls.enabled"
)

// driver is the concrete adbc.Driver implementation for Spark Connect.
type driver struct {
	alloc memory.Allocator
}

// NewDriver constructs a Spark Connect ADBC driver using the supplied Arrow
// allocator. If alloc is nil, memory.DefaultAllocator is used. The returned
// value satisfies both adbc.Driver and adbc.DriverWithContext.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	return &driver{alloc: alloc}
}

var (
	_ adbc.Driver            = (*driver)(nil)
	_ adbc.DriverWithContext = (*driver)(nil)
)

// NewDatabase creates a new database handle from the supplied options.
func (d *driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	db := &database{alloc: d.alloc}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}
	return db, nil
}

// NewDatabaseWithContext creates a new database handle. The context is accepted
// for interface compatibility; database construction performs no I/O.
func (d *driver) NewDatabaseWithContext(_ context.Context, opts map[string]string) (adbc.Database, error) {
	return d.NewDatabase(opts)
}
