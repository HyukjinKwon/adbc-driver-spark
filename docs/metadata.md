<!-- SPDX-License-Identifier: Apache-2.0 -->
# Metadata and Catalogs

The driver implements the ADBC metadata API so you can introspect catalogs,
schemas, tables, and columns without parsing SQL output. Behind the scenes it
uses **Apache Spark Connect** catalog relations and `AnalyzePlan`.

## The metadata methods

| Method            | Returns                                                     |
|-------------------|-------------------------------------------------------------|
| `GetObjects`      | A hierarchical catalog: catalogs, schemas, tables, columns. |
| `GetTableSchema`  | The Arrow schema of a single table.                         |
| `GetTableTypes`   | The table types the server reports (for example `TABLE`, `VIEW`). |
| `GetInfo`         | Driver and server info (names, versions, ADBC version).     |

`GetObjects` takes a depth (catalogs, db_schemas, tables, or all) plus optional
filters for catalog, schema, table name pattern, table types, and column name
pattern.

## Listing catalogs, schemas, and tables

=== "Python"

    ```python
    import adbc_driver_spark.dbapi as dbapi

    with dbapi.connect("sc://localhost:15002") as conn:
        # The DBAPI connection exposes ADBC metadata helpers that return
        # pyarrow objects.
        catalogs = conn.adbc_get_objects(depth="catalogs").read_all()
        print(catalogs.to_pylist())

        # Tables in a given catalog and schema.
        tables = conn.adbc_get_objects(
            depth="tables",
            catalog_filter="spark_catalog",
            db_schema_filter="default",
        ).read_all()
        print(tables.to_pylist())
    ```

=== "Go"

    ```go
    import (
        "github.com/apache/arrow-adbc/go/adbc"
    )

    // Walk the full object hierarchy for one schema.
    reader, err := cnxn.GetObjects(
        ctx,
        adbc.ObjectDepthAll,
        nil,                         // catalog
        strPtr("default"),           // dbSchema
        nil,                         // tableName
        nil,                         // columnName
        nil,                         // tableTypes
    )
    if err != nil {
        panic(err)
    }
    defer reader.Release()
    for reader.Next() {
        fmt.Println(reader.Record())
    }
    ```

=== "C"

    ```c
    /* AdbcConnectionGetObjects walks the catalog/schema/table/column hierarchy
     * and returns the result as an Arrow C stream. Pass NULL for filters you
     * do not want to apply. */
    struct ArrowArrayStream stream = {0};
    AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL,
                             NULL,        /* catalog */
                             "default",   /* db_schema */
                             NULL,        /* table_name */
                             NULL,        /* table_types */
                             NULL,        /* column_name */
                             &stream, &error);
    /* Read `stream` with nanoarrow. */
    ```

    !!! note
        The full setup/teardown (creating the database and connection, error
        checking, releases) and the compile command live in
        [Using from C and C++](usage-c.md).

=== "R"

    ```r
    # adbc_connection_get_objects() returns the hierarchy as an Arrow stream.
    objects <- con |>
        adbc_connection_get_objects(depth = "all", db_schema = "default") |>
        as.data.frame()
    print(objects)
    ```

Equivalently, you can use SQL discovery statements such as `SHOW CATALOGS`,
`SHOW DATABASES`, and `SHOW TABLES`, which return ordinary Arrow result sets.

```python
with conn.cursor() as cur:
    cur.execute("SHOW TABLES IN default")
    print(cur.fetchall())
```

## Inspecting a table schema

=== "Python"

    ```python
    schema = conn.adbc_get_table_schema(
        catalog="spark_catalog",
        db_schema="default",
        table_name="events",
    )
    print(schema)   # pyarrow.Schema
    ```

=== "Go"

    ```go
    schema, err := cnxn.GetTableSchema(
        ctx,
        strPtr("spark_catalog"), // catalog
        strPtr("default"),       // dbSchema
        "events",                // tableName
    )
    if err != nil {
        panic(err)
    }
    fmt.Println(schema)          // *arrow.Schema
    ```

=== "C"

    ```c
    /* Fetch the Arrow schema of a single table. */
    struct ArrowSchema schema = {0};
    AdbcConnectionGetTableSchema(&connection,
                                 "spark_catalog", /* catalog */
                                 "default",       /* db_schema */
                                 "events",        /* table_name */
                                 &schema, &error);
    /* Inspect `schema` (e.g. schema.n_children columns), then release it. */
    ```

=== "R"

    ```r
    schema <- adbc_connection_get_table_schema(
        con,
        catalog = "spark_catalog",
        db_schema = "default",
        table_name = "events")
    print(schema)
    ```

## Table types

=== "Python"

    ```python
    table_types = conn.adbc_get_table_types()
    print(table_types)   # e.g. ['TABLE', 'VIEW', ...]
    ```

=== "Go"

    ```go
    reader, err := cnxn.GetTableTypes(ctx)
    if err != nil {
        panic(err)
    }
    defer reader.Release()
    for reader.Next() {
        fmt.Println(reader.Record())
    }
    ```

=== "C"

    ```c
    /* The reported table types arrive as an Arrow C stream. */
    struct ArrowArrayStream stream = {0};
    AdbcConnectionGetTableTypes(&connection, &stream, &error);
    /* Read `stream` with nanoarrow, e.g. "TABLE", "VIEW". */
    ```

=== "R"

    ```r
    table_types <- con |>
        adbc_connection_get_table_types() |>
        as.data.frame()
    print(table_types)   # e.g. TABLE, VIEW
    ```

## Driver and server info

`GetInfo` reports identifying details such as the driver name and version and
the Spark vendor name and version.

=== "Python"

    ```python
    info = conn.adbc_get_info()
    print(info)   # dict of info codes to values
    ```

=== "Go"

    ```go
    reader, err := cnxn.GetInfo(ctx, nil) // nil => all info codes
    if err != nil {
        panic(err)
    }
    defer reader.Release()
    for reader.Next() {
        fmt.Println(reader.Record())
    }
    ```

=== "C"

    ```c
    /* Pass NULL info codes (and 0 length) to request all of them. The result
     * is an Arrow C stream of info code/value pairs. */
    struct ArrowArrayStream stream = {0};
    AdbcConnectionGetInfo(&connection, NULL, 0, &stream, &error);
    /* Read `stream` with nanoarrow. */
    ```

=== "R"

    ```r
    info <- con |>
        adbc_connection_get_info() |>
        as.data.frame()
    print(info)
    ```

!!! note
    The column types reported by `GetObjects` and `GetTableSchema` follow the
    Spark to Arrow mapping in [Type Mapping](type-mapping.md).
