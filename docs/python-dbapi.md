<!-- SPDX-License-Identifier: Apache-2.0 -->
# Python DBAPI

The `adbc_driver_spark.dbapi` module is a PEP 249 (DBAPI 2.0) interface over the
**Apache Spark Connect** driver. If you have used `sqlite3`, `psycopg`, or any
DBAPI driver, the surface will be familiar, with Arrow-native extensions layered
on top.

## Connecting

```python
import adbc_driver_spark.dbapi as dbapi

conn = dbapi.connect("sc://localhost:15002")
```

`connect()` accepts:

| Parameter     | Purpose                                                        |
|---------------|----------------------------------------------------------------|
| `uri`         | Spark Connect URI (`sc://host:port/;k=v;...`). Defaults to `sc://localhost:15002`. |
| `token`       | Convenience bearer token. Implies `use_ssl=True`.              |
| `use_ssl`     | Force TLS on or off.                                           |
| `db_kwargs`   | Extra database options (see `adbc_driver_spark.DatabaseOptions`). |
| `conn_kwargs` | Extra connection options (standard `adbc.connection.*` keys). |
| `autocommit`  | Defaults to `True`. Spark Connect has no transactions.         |

```python
conn = dbapi.connect(
    "sc://spark.example.com:443",
    token="eyJhbGci...",
    db_kwargs={"adbc.spark.user_id": "analyst"},
)
```

!!! note
    There is also a low-level entrypoint, `adbc_driver_spark.connect(uri,
    db_kwargs=...)`, which returns an `adbc_driver_manager.AdbcDatabase`. Use the
    DBAPI module unless you specifically need the raw ADBC handles.

## Context managers

Both connections and cursors are context managers. Use them to guarantee
cleanup.

```python
with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS id, 'hi' AS msg")
        print(cur.fetchall())   # [(1, 'hi')]
```

## Cursors and fetching

```python
with conn.cursor() as cur:
    cur.execute("SELECT id FROM range(100)")

    one = cur.fetchone()        # a single row tuple, or None
    some = cur.fetchmany(10)    # up to 10 rows
    rest = cur.fetchall()       # all remaining rows

    print(cur.description)      # column metadata, PEP 249 style
    print(cur.rowcount)         # affected rows for DML, else -1
```

Use `executemany()` to run the same statement over many parameter sets:

```python
with conn.cursor() as cur:
    cur.executemany(
        "INSERT INTO events VALUES (?, ?)",
        [(1, "click"), (2, "view"), (3, "scroll")],
    )
```

## Arrow and DataFrame integration

The cursor exposes Arrow-native fetch helpers so you can hand results to
pandas, Polars, or any Arrow consumer with no row-by-row conversion.

=== "pandas"

    ```python
    with conn.cursor() as cur:
        cur.execute("SELECT id, id * id AS square FROM range(10)")
        df = cur.fetch_df()              # pandas.DataFrame
        print(df.head())
    ```

=== "PyArrow"

    ```python
    with conn.cursor() as cur:
        cur.execute("SELECT id, id * id AS square FROM range(10)")
        table = cur.fetch_arrow_table()  # pyarrow.Table
        print(table.schema)
    ```

=== "Polars"

    ```python
    import polars as pl

    with conn.cursor() as cur:
        cur.execute("SELECT id, id * id AS square FROM range(10)")
        table = cur.fetch_arrow_table()  # pyarrow.Table
        df = pl.from_arrow(table)        # polars.DataFrame, zero copy
        print(df)
    ```

For very large results, stream batches instead of materializing the whole
table:

```python
with conn.cursor() as cur:
    cur.execute("SELECT * FROM range(1000000)")
    reader = cur.fetch_record_batch()   # pyarrow.RecordBatchReader
    for batch in reader:
        ...                             # process each pyarrow.RecordBatch
```

## Parameters

The module `paramstyle` is `qmark`: use `?` placeholders and pass parameters
positionally. Binding by name (`:name`) through the DBAPI is not supported.

```python
with conn.cursor() as cur:
    cur.execute(
        "SELECT * FROM events WHERE id > ? AND kind = ?",
        parameters=[1, "click"],
    )
    print(cur.fetchall())
```

## Error handling

DBAPI exception classes are re-exported, so you can catch the standard
hierarchy:

```python
from adbc_driver_spark.dbapi import OperationalError, ProgrammingError

try:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM does_not_exist")
except ProgrammingError as exc:
    print("bad SQL:", exc)
except OperationalError as exc:
    print("server or connection issue:", exc)
```

See [Querying Data](querying.md) for prepared statements and DDL/DML, and
[Metadata and Catalogs](metadata.md) for introspection.
