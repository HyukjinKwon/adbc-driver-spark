<!-- SPDX-License-Identifier: Apache-2.0 -->
# Quickstart

The shortest path from a running **Apache Spark Connect** server to an Arrow
result set.

## Start a Spark Connect server

```bash
# From a Spark 4.x distribution (the Connect server is bundled)
./sbin/start-connect-server.sh
# Spark Connect listens on sc://localhost:15002 by default
# On Spark 3.5.x (which does not bundle it) add:
#   --packages org.apache.spark:spark-connect_2.13:3.5.8
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

=== "C"

    ```c
    #include <adbc.h>

    /* Configure the database: point the driver manager at the shared library
     * and set the Spark Connect URI, then init and open a connection. */
    struct AdbcError error = {0};
    struct AdbcDatabase database = {0};
    AdbcDatabaseNew(&database, &error);
    AdbcDatabaseSetOption(&database, "driver",
                          "/path/to/libadbc_driver_spark.so", &error);
    AdbcDatabaseSetOption(&database, "uri", "sc://localhost:15002", &error);
    AdbcDatabaseInit(&database, &error);

    struct AdbcConnection connection = {0};
    AdbcConnectionNew(&connection, &error);
    AdbcConnectionInit(&connection, &database, &error);

    /* Run a query and read the Arrow result stream. */
    struct AdbcStatement statement = {0};
    AdbcStatementNew(&connection, &statement, &error);
    AdbcStatementSetSqlQuery(&statement,
                             "SELECT id, id * id AS square FROM range(5)", &error);

    struct ArrowArrayStream stream = {0};
    int64_t rows_affected = -1;
    AdbcStatementExecuteQuery(&statement, &stream, &rows_affected, &error);
    /* Consume `stream` with nanoarrow or the Arrow C data interface. */
    ```

    !!! note
        The full setup/teardown (error checking, releases) and the compile
        command live in [Using from C and C++](usage-c.md).

=== "R"

    ```r
    library(adbcdrivermanager)

    # Wrap the shared library, init the database with the Spark Connect URI,
    # and open a connection.
    drv <- adbc_driver(Sys.getenv("SPARK_DRIVER"))
    db <- adbc_database_init(drv, uri = "sc://localhost:15002")
    con <- adbc_connection_init(db)

    # read_adbc() runs the query and returns a streaming Arrow result.
    df <- read_adbc(con, "SELECT id, id * id AS square FROM range(5)") |>
        as.data.frame()
    print(df)
    ```

=== "Rust"

    ```rust
    use adbc_core::options::{AdbcVersion, OptionDatabase};
    use adbc_core::{Connection, Database, Driver, Statement};
    use adbc_driver_manager::ManagedDriver;

    // entrypoint = None uses the default AdbcDriverInit symbol.
    let mut driver = ManagedDriver::load_dynamic_from_filename(driver_path, None, AdbcVersion::V110)?;
    let mut database = driver.new_database_with_opts([(OptionDatabase::Uri, "sc://localhost:15002".into())])?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement.set_sql_query("SELECT id, id * id AS square FROM range(5)")?;

    let reader = statement.execute()?;   // a RecordBatchReader
    let mut rows = 0usize;
    for batch in reader {
        rows += batch?.num_rows();
    }
    println!("read {rows} rows");
    ```

    !!! note
        `driver_path` is the path to `libadbc_driver_spark.{so,dylib,dll}`. The
        full setup (dependencies, error handling) lives in
        [Using from Rust](usage-rust.md).

=== "Ruby"

    ```ruby
    require "adbc"

    # driver is the path to libadbc_driver_spark.{so,dylib,dll}.
    ADBC::Database.open(driver: driver, uri: "sc://localhost:15002") do |database|
      database.connect do |connection|
        # query runs the SQL and returns an Arrow::Table.
        table = connection.query("SELECT id, id * id AS square FROM range(5)")
        puts table
      end
    end
    ```

    !!! note
        See [Using from Ruby](usage-ruby.md) for installing the `red-adbc` gem
        and the full setup.

!!! tip
    `fetch_arrow_table()` returns a `pyarrow.Table` with zero copy from the
    Arrow batches Spark streamed back. Use `fetch_df()` for a pandas DataFrame.

## Next steps

- [Connecting and Authentication](connecting.md): tokens, TLS, and headers.
- [Querying Data](querying.md): streaming, DDL/DML, prepared statements.
- [Python DBAPI](python-dbapi.md): cursors and DataFrame integration.
- [Examples](examples.md): runnable programs for every language.
