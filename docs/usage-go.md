<!-- SPDX-License-Identifier: Apache-2.0 -->
# Using from Go

Go programs use the native driver directly, with no shared library or cgo. The
driver implements the standard `github.com/apache/arrow-adbc/go/adbc`
interfaces, so the usage matches every other arrow-adbc Go driver.

## Install

```bash
go get github.com/HyukjinKwon/adbc-driver-spark
```

The driver lives at `github.com/HyukjinKwon/adbc-driver-spark/driver/spark`. It
depends on `github.com/apache/arrow-adbc/go/adbc` and
`github.com/apache/arrow-go/v18`. Go 1.25 or newer is required.

## Creating a driver and running a query

`NewDriver` takes an `arrow/memory.Allocator` and returns an `adbc.Driver`. From
there you create a database, open a connection, and run statements.

```go
package main

import (
	"context"
	"fmt"

	spark "github.com/HyukjinKwon/adbc-driver-spark/driver/spark"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
	ctx := context.Background()

	drv := spark.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		"uri": "sc://localhost:15002",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT id, id * id AS square FROM range(5)"); err != nil {
		panic(err)
	}

	reader, rowsAffected, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		panic(err)
	}
	defer reader.Release()
	_ = rowsAffected // -1 for a SELECT

	for reader.Next() {
		fmt.Println(reader.Record())
	}
	if err := reader.Err(); err != nil {
		panic(err)
	}
}
```

## Options

Database, connection, and statement options are plain `map[string]string` /
`SetOption` calls.

```go
db, err := drv.NewDatabase(map[string]string{
	"uri":                        "sc://spark.example.com:443",
	"adbc.spark.connect.token":   "eyJhbGci...",
	"adbc.spark.connect.use_ssl": "true",
})
```

You can also build the database empty and set options afterward:

```go
db, _ := drv.NewDatabase(nil)
db.(adbc.PostInitOptions).SetOptions(map[string]string{
	"uri": "sc://localhost:15002",
})
```

See the [Configuration Reference](configuration.md) for every key.

## ExecuteUpdate, prepared statements, and metadata

```go
// DDL/DML.
stmt.SetSqlQuery("INSERT INTO events VALUES (1, 'click')")
affected, err := stmt.ExecuteUpdate(ctx)

// Prepared statement with bound parameters.
stmt.SetSqlQuery("SELECT * FROM events WHERE id > ?")
stmt.Prepare(ctx)
// build an Arrow record of parameters, then:
stmt.Bind(ctx, params)        // or stmt.BindStream(ctx, paramReader)
reader, _, err := stmt.ExecuteQuery(ctx)

// Metadata.
schema, err := cnxn.GetTableSchema(ctx, nil, strPtr("default"), "events")
```

See [Querying Data](querying.md) and [Metadata and Catalogs](metadata.md) for
fuller examples.

## database/sql registration

The native arrow-adbc interfaces are the supported surface for Go. If you need a
`database/sql` driver, wrap the ADBC connection with arrow-adbc's
`database/sql` adapter where available, registering this driver as the
underlying ADBC driver.

!!! note
    Records returned by the reader are valid only until the next call to
    `Next()`. Retain a record (`rec.Retain()`) if you need to keep it longer,
    and `Release()` it when done.
