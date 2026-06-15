// SPDX-License-Identifier: Apache-2.0

// Command metadata inspects catalog metadata through the ADBC connection API:
// GetObjects walks catalogs/schemas/tables/columns, GetTableSchema returns one
// table's Arrow schema, and GetTableTypes lists the table types Spark exposes.
//
// Start a Spark Connect server first (default sc://localhost:15002), then:
//
//	go run ./examples/go/metadata
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

func endpoint() string {
	for _, key := range []string{"SPARK_REMOTE", "SPARK_CONNECT_URI"} {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return "sc://localhost:15002"
}

func run(ctx context.Context, uri string) error {
	drv := spark.NewDriver(memory.DefaultAllocator)
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

	// Create a temporary view so this example has something to introspect even
	// on an otherwise empty server.
	if err := createView(ctx, cnxn); err != nil {
		return err
	}

	if err := showTableTypes(ctx, cnxn); err != nil {
		return err
	}
	if err := showTableSchema(ctx, cnxn, "example_metadata_view"); err != nil {
		return err
	}
	return showObjects(ctx, cnxn)
}

func createView(ctx context.Context, cnxn adbc.Connection) error {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(
		"CREATE OR REPLACE TEMPORARY VIEW example_metadata_view AS " +
			"SELECT id, CAST(id AS STRING) AS label FROM range(3)",
	); err != nil {
		return err
	}
	// ExecuteUpdate runs a statement for its side effects and discards rows.
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

// showTableTypes prints the table types from Connection.GetTableTypes.
func showTableTypes(ctx context.Context, cnxn adbc.Connection) error {
	reader, err := cnxn.GetTableTypes(ctx)
	if err != nil {
		return fmt.Errorf("get table types: %w", err)
	}
	defer reader.Release()

	fmt.Println("table types:")
	for reader.Next() {
		col := reader.RecordBatch().Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			fmt.Printf("  %s\n", col.Value(i))
		}
	}
	return reader.Err()
}

// showTableSchema prints the Arrow schema of one table via GetTableSchema. The
// catalog and db schema are nil here because temporary views are session-local.
func showTableSchema(ctx context.Context, cnxn adbc.Connection, table string) error {
	schema, err := cnxn.GetTableSchema(ctx, nil, nil, table)
	if err != nil {
		return fmt.Errorf("get table schema: %w", err)
	}
	fmt.Printf("\nschema of %s:\n", table)
	for _, f := range schema.Fields() {
		fmt.Printf("  %s: %s\n", f.Name, f.Type)
	}
	return nil
}

// showObjects walks the catalog hierarchy via GetObjects at full column depth
// and prints the catalog and schema names it finds.
func showObjects(ctx context.Context, cnxn adbc.Connection) error {
	reader, err := cnxn.GetObjects(ctx, adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("get objects: %w", err)
	}
	defer reader.Release()

	fmt.Println("\ncatalogs:")
	for reader.Next() {
		// The top-level result has a catalog_name column (index 0).
		names := reader.RecordBatch().Column(0).(*array.String)
		for i := 0; i < names.Len(); i++ {
			fmt.Printf("  %s\n", names.Value(i))
		}
	}
	return reader.Err()
}
