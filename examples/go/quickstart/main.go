// SPDX-License-Identifier: Apache-2.0

// Command quickstart connects to a Spark Connect server with the native Go ADBC
// driver, runs a query, and streams the Apache Arrow results.
//
// Start a Spark Connect server first (default sc://localhost:15002), then:
//
//	go run ./examples/go/quickstart
//
// Override the endpoint with SPARK_REMOTE (or SPARK_CONNECT_URI), for example:
//
//	SPARK_REMOTE='sc://spark.example.com:443/;token=<jwt>;use_ssl=true' \
//	    go run ./examples/go/quickstart
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/HyukjinKwon/adbc-driver-spark/driver/spark"
)

func main() {
	if err := run(context.Background(), endpoint()); err != nil {
		log.Fatal(err)
	}
}

// endpoint resolves the Spark Connect URI from the environment, defaulting to
// the local server.
func endpoint() string {
	for _, key := range []string{"SPARK_REMOTE", "SPARK_CONNECT_URI"} {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return "sc://localhost:15002"
}

func run(ctx context.Context, uri string) error {
	// 1. Build a driver and open a database from the connection string. The
	//    Arrow allocator is shared by everything the driver creates.
	drv := spark.NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		adbc.OptionKeyURI: uri,
		// Other options exist, e.g. spark.OptionKeyToken for a bearer token.
	})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	// 2. Open a connection. One connection maps to one Spark Connect session.
	cnxn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer cnxn.Close()

	// 3. Create a statement, set the SQL, and execute it.
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return fmt.Errorf("new statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT id, id * id AS square FROM range(5)"); err != nil {
		return fmt.Errorf("set query: %w", err)
	}

	// ExecuteQuery returns an Arrow RecordReader. Spark Connect does not report
	// an affected-row count up front, so the second return value is -1 here.
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	defer reader.Release()

	// 4. Iterate the reader, one Arrow record batch at a time. The columns are
	//    native Arrow arrays, so reading them is zero-copy.
	fmt.Println("schema:", reader.Schema())
	for reader.Next() {
		rec := reader.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		squares := rec.Column(1).(*array.Int64)
		for i := 0; i < int(rec.NumRows()); i++ {
			fmt.Printf("  id=%d square=%d\n", ids.Value(i), squares.Value(i))
		}
	}
	return reader.Err()
}
