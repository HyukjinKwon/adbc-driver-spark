<!-- SPDX-License-Identifier: Apache-2.0 -->
# Querying Data

The driver executes SQL through standard ADBC statements and returns results as
Arrow record batches. This page covers reads, streaming, DDL and DML, and
prepared statements with parameter binding.

## Executing a SELECT

A statement runs SQL with `SetSqlQuery` then `ExecuteQuery`, which returns an
Arrow `RecordReader` and a row count (`-1` when unknown).

=== "Python"

    ```python
    import adbc_driver_spark.dbapi as dbapi

    with dbapi.connect("sc://localhost:15002") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, id * id AS square FROM range(10)")
            table = cur.fetch_arrow_table()   # pyarrow.Table
            print(table.num_rows)
    ```

=== "Go"

    ```go
    stmt, _ := cnxn.NewStatement()
    defer stmt.Close()

    stmt.SetSqlQuery("SELECT id, id * id AS square FROM range(10)")
    reader, rowsAffected, err := stmt.ExecuteQuery(ctx)
    if err != nil {
        panic(err)
    }
    defer reader.Release()
    _ = rowsAffected // -1 for a SELECT
    ```

## Streaming Arrow results

Results arrive as a stream of Arrow record batches. Iterate the reader to
process arbitrarily large results without holding the whole result in memory.

=== "Python"

    ```python
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM range(1000000)")
        reader = cur.fetch_record_batch()      # pyarrow.RecordBatchReader
        total = 0
        for batch in reader:
            total += batch.num_rows
        print(total)
    ```

=== "Go"

    ```go
    stmt.SetSqlQuery("SELECT * FROM range(1000000)")
    reader, _, _ := stmt.ExecuteQuery(ctx)
    defer reader.Release()

    var total int64
    for reader.Next() {
        rec := reader.Record()           // valid until the next Next()
        total += rec.NumRows()
    }
    if err := reader.Err(); err != nil {
        panic(err)
    }
    ```

!!! tip
    Streaming avoids materializing the full result. Use `fetch_arrow_table()`
    only when you want the entire result as a single `pyarrow.Table`.

## Materializing results

| Helper                  | Returns                |
|-------------------------|------------------------|
| `cur.fetchall()`        | a list of row tuples (PEP 249) |
| `cur.fetchmany(n)`      | up to `n` row tuples   |
| `cur.fetch_arrow_table()` | a `pyarrow.Table`    |
| `cur.fetch_record_batch()` | a `pyarrow.RecordBatchReader` |
| `cur.fetch_df()`        | a pandas `DataFrame`   |

## DDL and DML with ExecuteUpdate

For statements that do not return rows (DDL such as `CREATE TABLE`, DML such as
`INSERT`), use the update path. It returns the number of affected rows, or `-1`
when the server does not report a count.

=== "Python"

    ```python
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS events (id BIGINT, kind STRING) USING parquet")
        cur.execute("INSERT INTO events VALUES (1, 'click'), (2, 'view')")
        print(cur.rowcount)   # affected rows, or -1
    ```

=== "Go"

    ```go
    stmt.SetSqlQuery("CREATE TABLE IF NOT EXISTS events (id BIGINT, kind STRING) USING parquet")
    if _, err := stmt.ExecuteUpdate(ctx); err != nil {
        panic(err)
    }

    stmt.SetSqlQuery("INSERT INTO events VALUES (1, 'click'), (2, 'view')")
    affected, err := stmt.ExecuteUpdate(ctx)
    if err != nil {
        panic(err)
    }
    fmt.Println(affected) // affected rows, or -1
    ```

## Prepared statements and parameter binding

Prepare a statement once, then bind parameters before each execution. The driver
binds parameters as an Arrow record, so values stay columnar.

### Positional parameters

The Python paramstyle is `qmark`: use `?` placeholders.

=== "Python"

    ```python
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM events WHERE id > ? AND kind = ?",
            parameters=[1, "click"],
        )
        print(cur.fetchall())
    ```

=== "Go"

    ```go
    import (
        "github.com/apache/arrow-go/v18/arrow"
        "github.com/apache/arrow-go/v18/arrow/array"
        "github.com/apache/arrow-go/v18/arrow/memory"
    )

    stmt.SetSqlQuery("SELECT * FROM events WHERE id > ? AND kind = ?")
    if err := stmt.Prepare(ctx); err != nil {
        panic(err)
    }

    // Build a one-row record holding the bind values.
    schema := arrow.NewSchema([]arrow.Field{
        {Name: "0", Type: arrow.PrimitiveTypes.Int64},
        {Name: "1", Type: arrow.BinaryTypes.String},
    }, nil)
    b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
    defer b.Release()
    b.Field(0).(*array.Int64Builder).Append(1)
    b.Field(1).(*array.StringBuilder).Append("click")
    params := b.NewRecord()
    defer params.Release()

    if err := stmt.Bind(ctx, params); err != nil {
        panic(err)
    }
    reader, _, err := stmt.ExecuteQuery(ctx)
    if err != nil {
        panic(err)
    }
    defer reader.Release()
    ```

### Named parameters

Spark SQL named markers (`:name`) are supported when you bind by name.

```python
with conn.cursor() as cur:
    cur.execute(
        "SELECT * FROM events WHERE id > :min_id",
        parameters={"min_id": 1},
    )
    print(cur.fetchall())
```

!!! note
    For repeated execution over many parameter sets, bind a stream of records
    (`BindStream` in Go). This sends one prepared plan and multiple parameter
    batches.

See [Type Mapping](type-mapping.md) for how Spark types map to Arrow, and
[Python DBAPI](python-dbapi.md) for the full cursor API.
