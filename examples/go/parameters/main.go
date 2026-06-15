// SPDX-License-Identifier: Apache-2.0

// Command parameters runs a prepared statement with bound positional
// parameters. Parameters are positional ? placeholders; the driver binds a
// single row of values, one column per placeholder, in column order.
//
// Start a Spark Connect server first (default sc://localhost:15002), then:
//
//	go run ./examples/go/parameters
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/HyukjinKwon/adbc-driver-spark/driver/spark"
)

func main() {
	if err := run(context.Background(), endpoint()); err != nil {
		log.Fatal(err)
	}
}

func endpoint() string {
	for _, key := range []string{"SPARK_REMOTE", "SPARK_CONNECT_URI"} {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return "sc://localhost:15002"
}

func run(ctx context.Context, uri string) error {
	alloc := memory.DefaultAllocator

	drv := spark.NewDriver(alloc)
	db, err := drv.NewDatabase(map[string]string{adbc.OptionKeyURI: uri})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		return fmt.Errorf("new statement: %w", err)
	}
	defer stmt.Close()

	// Two positional placeholders: the first is a lower bound, the second a
	// string returned alongside each row.
	if err := stmt.SetSqlQuery("SELECT id, ? AS tag FROM range(10) WHERE id > ?"); err != nil {
		return fmt.Errorf("set query: %w", err)
	}

	// Prepare acknowledges the statement; Spark Connect validates it on first
	// execution.
	if err := stmt.Prepare(ctx); err != nil {
		return fmt.Errorf("prepare: %w", err)
	}

	// Build a single-row Arrow record holding one value per ? placeholder, in
	// the order the placeholders appear: tag (string), then the bound id (int).
	params := bindRow(alloc)
	defer params.Release()
	if err := stmt.Bind(ctx, params); err != nil {
		return fmt.Errorf("bind: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	defer reader.Release()

	fmt.Println("rows where id > 6:")
	for reader.Next() {
		rec := reader.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		tags := rec.Column(1).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			fmt.Printf("  id=%d tag=%s\n", ids.Value(i), tags.Value(i))
		}
	}
	return reader.Err()
}

// bindRow builds a one-row Arrow record carrying the parameter values in
// placeholder order. The driver converts each column of this row into a Spark
// SQL literal substituted for the matching ? in the query.
func bindRow(alloc memory.Allocator) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tag", Type: arrow.BinaryTypes.String},
		{Name: "min_id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.StringBuilder).Append("matched")
	bldr.Field(1).(*array.Int64Builder).Append(6)

	return bldr.NewRecord()
}
