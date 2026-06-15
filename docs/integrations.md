<!-- SPDX-License-Identifier: Apache-2.0 -->
# Ecosystem integrations

Because the driver returns native Apache Arrow data through the standard ADBC
interface, it plugs into the wider Arrow and ADBC ecosystem with no special
glue. The same patterns you would use with any other ADBC driver (PostgreSQL,
SQLite, Snowflake) work here against **Apache Spark Connect**.

Every example on this page is executed against a live Spark Connect server on
every CI run (Spark 3.5.x, 4.0.x, and 4.1.x), so it stays correct. The runnable sources
live under [`examples/python/`](https://github.com/HyukjinKwon/adbc-driver-spark/tree/main/examples/python).

!!! note "Install the integration you need"
    ```bash
    pip install adbc-driver-spark pyarrow
    pip install polars duckdb "pandas>=2.0"   # as needed per integration
    ```

## pandas

pandas 2.0+ accepts an ADBC connection in `read_sql` and pulls Arrow batches
under the hood, which is faster and preserves types better than the legacy path.

```python
import pandas as pd
import adbc_driver_spark.dbapi as dbapi

with dbapi.connect("sc://localhost:15002") as conn:
    df = pd.read_sql("SELECT id, id * id AS square FROM range(10)", conn)
    print(df)
```

The cursor also offers a driver-native shortcut:

```python
with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT AVG(id) AS mean_id FROM range(100)")
        print(cur.fetch_df())          # pandas.DataFrame
```

## Polars

Polars reads directly from the ADBC connection with `pl.read_database`.

```python
import polars as pl
import adbc_driver_spark.dbapi as dbapi

with dbapi.connect("sc://localhost:15002") as conn:
    df = pl.read_database(
        "SELECT id, id * id AS square FROM range(10)",
        connection=conn,
    )
    print(df.select(pl.col("square").sum()))
```

## DuckDB

Push heavy aggregation to Spark, then do fast local analytics in DuckDB on the
same Arrow buffers. DuckDB scans an Arrow table by referencing the Python
variable name in SQL, with no copy.

```python
import duckdb
import adbc_driver_spark.dbapi as dbapi

with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id, id % 3 AS bucket FROM range(1000)")
        spark_result = cur.fetch_arrow_table()

    rows = duckdb.sql(
        "SELECT bucket, COUNT(*) AS n FROM spark_result GROUP BY bucket ORDER BY bucket"
    ).fetchall()
    print(rows)
```

## PyArrow streaming

For large results, stream Arrow record batches with bounded memory using
`fetch_record_batch`, which returns a `pyarrow.RecordBatchReader`.

```python
import pyarrow.compute as pc
import adbc_driver_spark.dbapi as dbapi

with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id, id * id AS square FROM range(100000)")
        reader = cur.fetch_record_batch()

        total_rows = 0
        running_sum = 0
        for batch in reader:               # one batch at a time
            total_rows += batch.num_rows
            running_sum += pc.sum(batch.column("square")).as_py()
        print(total_rows, running_sum)
```

## Other Arrow consumers

Any library that understands the Arrow C stream interface or a `pyarrow.Table`
works the same way. `cursor.fetch_arrow_table()` and `cursor.fetch_record_batch()`
feed Datafusion, Polars, DuckDB, NumPy (via `to_pandas`), and the Arrow PyCapsule
protocol without bespoke code.

## Writing data

The driver supports writing through SQL (`CREATE TABLE`, `INSERT`, and
`CREATE TABLE ... AS SELECT`) via `cursor.execute`:

```python
with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TEMP VIEW demo AS SELECT id FROM range(5)")
        cur.execute("SELECT COUNT(*) FROM demo")
        print(cur.fetchone())
```

!!! warning "Bulk ADBC ingest is not yet implemented"
    The ADBC bulk-ingest path (`cursor.adbc_ingest(...)`, used to push a pandas
    or Arrow table into a new table in one call) is not implemented yet and
    raises `NotSupportedError`. Use SQL `INSERT` or `CREATE TABLE ... AS SELECT`
    to write data for now. Progress is tracked on the issue tracker.
