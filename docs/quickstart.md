<!-- SPDX-License-Identifier: Apache-2.0 -->
# Quickstart

The shortest path from a running **Apache Spark Connect** server to an Arrow
result set.

## Start a Spark Connect server

```bash
# From a Spark distribution (Spark 4.0.x or 4.1.x)
./sbin/start-connect-server.sh \
  --packages org.apache.spark:spark-connect_2.13:4.0.0
# Spark Connect listens on sc://localhost:15002 by default
```

If you already have an endpoint (for example a managed or Databricks-style
service), skip this step and use its `sc://` URL. See [Connecting and
Authentication](connecting.md).

## Run a query

=== "Python"

    ```python
    import adbc_driver_spark.dbapi as dbapi

    with dbapi.connect("sc://localhost:15002") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS id, 'hi' AS msg")
            print(cur.fetchall())            # [(1, 'hi')]

            cur.execute("SELECT id, id * id AS square FROM range(5)")
            table = cur.fetch_arrow_table()  # pyarrow.Table
            print(table.to_pandas())
    ```

=== "Go"

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

        reader, _, err := stmt.ExecuteQuery(ctx)
        if err != nil {
            panic(err)
        }
        defer reader.Release()

        for reader.Next() {
            fmt.Println(reader.Record())
        }
    }
    ```

!!! tip
    `fetch_arrow_table()` returns a `pyarrow.Table` with zero copy from the
    Arrow batches Spark streamed back. Use `fetch_df()` for a pandas DataFrame.

## Next steps

- [Connecting and Authentication](connecting.md): tokens, TLS, and headers.
- [Querying Data](querying.md): streaming, DDL/DML, prepared statements.
- [Python DBAPI](python-dbapi.md): cursors and DataFrame integration.
- [Examples](examples.md): runnable programs for every language.
