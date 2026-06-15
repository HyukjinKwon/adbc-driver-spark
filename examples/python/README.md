<!-- SPDX-License-Identifier: Apache-2.0 -->
# Python examples

Runnable examples for the ADBC Spark Connect driver using its DBAPI 2.0
(PEP 249) interface. Each file is self-contained and standalone.

## Setup

```bash
pip install adbc-driver-spark pyarrow pandas
```

Start a Spark Connect server reachable at `sc://localhost:15002` (the default
URI). For a local Apache Spark build:

```bash
# Spark 4.x bundles the Connect server:
./sbin/start-connect-server.sh
# On Spark 3.5.x add: --packages org.apache.spark:spark-connect_2.13:3.5.8
```

## Examples

| File | What it shows |
| --- | --- |
| [`01_quickstart.py`](01_quickstart.py) | Connect, run a `SELECT`, and print the rows. |
| [`02_arrow_and_pandas.py`](02_arrow_and_pandas.py) | Fetch results as a zero-copy Arrow table with `fetch_arrow_table()` and as a pandas DataFrame with `fetch_df()`. |
| [`03_parameters.py`](03_parameters.py) | Bind positional `?` parameters in prepared statements (the driver's `qmark` style). |
| [`04_metadata.py`](04_metadata.py) | List catalogs, schemas, tables, and columns, and fetch a table's Arrow schema. |
| [`05_ddl_and_dml.py`](05_ddl_and_dml.py) | Run `CREATE` / `INSERT` via `cursor.execute`, then `SELECT` the rows back. |
| [`06_auth_tls.py`](06_auth_tls.py) | Connect with a bearer `token=` and `use_ssl=True` (Databricks-style). Needs a real server, so the connect call is commented. |
| [`07_polars.py`](07_polars.py) | Read a query straight into a Polars DataFrame with `pl.read_database`. |
| [`08_duckdb.py`](08_duckdb.py) | Hand Spark's Arrow output to DuckDB and run local SQL over it, zero-copy. |
| [`09_pyarrow_streaming.py`](09_pyarrow_streaming.py) | Stream a large result as a pyarrow `RecordBatchReader` with bounded memory. |
| [`10_pandas_read_sql.py`](10_pandas_read_sql.py) | Read with `pandas.read_sql` over the ADBC connection (the standard pandas + ADBC path). |

The integration examples (07 to 10) need the relevant library installed:

```bash
pip install polars duckdb "pandas>=2.0"
```

## Run

Each example runs directly:

```bash
python 01_quickstart.py
```

Examples 07 to 10 honor the `SPARK_CONNECT_URI` environment variable (default
`sc://localhost:15002`). For the others, edit the `sc://` URI near the top of the
file (or the `SPARK_HOST` / `SPARK_TOKEN` environment variables in
`06_auth_tls.py`).

## Continuous validation

Every example here except `06_auth_tls.py` is executed against a live Spark
Connect server (Spark 4.0.x and 4.1.x) on every CI run, so the documented
integrations cannot silently break. See `.github/workflows/e2e.yml`.
