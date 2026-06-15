<!-- SPDX-License-Identifier: Apache-2.0 -->
# Examples

Runnable examples live in the
[`examples/`](https://github.com/HyukjinKwon/adbc-driver-spark/tree/main/examples)
directory of the repository, organized by language. Each one connects to a
Spark Connect server at `sc://localhost:15002` by default; override the URI to
point at your own endpoint.

All examples assume a running **Apache Spark Connect** server. See the
[Quickstart](quickstart.md) for how to start one. The Python examples are
executed against live Spark 4.0.x and 4.1.x servers on every CI run.

## Python

In [`examples/python/`](https://github.com/HyukjinKwon/adbc-driver-spark/tree/main/examples/python):

| File | What it shows |
|------|---------------|
| `01_quickstart.py` | Connect, run a `SELECT`, and print rows. |
| `02_arrow_and_pandas.py` | `fetch_arrow_table()` (zero-copy) and `fetch_df()` into pandas. |
| `03_parameters.py` | Prepared statements with positional `?` (`qmark`) parameters. |
| `04_metadata.py` | List catalogs, schemas, tables, columns, and fetch a table schema. |
| `05_ddl_and_dml.py` | `CREATE` / `INSERT` via `cursor.execute`, then `SELECT`. |
| `06_auth_tls.py` | Connect with a bearer `token=` over TLS (Databricks-style). |
| `07_polars.py` | Read straight into a Polars DataFrame with `pl.read_database`. |
| `08_duckdb.py` | Query Spark's Arrow output in DuckDB, zero-copy. |
| `09_pyarrow_streaming.py` | Stream a large result as a pyarrow `RecordBatchReader`. |
| `10_pandas_read_sql.py` | `pandas.read_sql` over the ADBC connection. |

Examples 07 to 10 are covered in depth on the
[Ecosystem Integrations](integrations.md) page.

Run one with:

```bash
pip install adbc-driver-spark pyarrow pandas
python examples/python/01_quickstart.py
```

## Go

In [`examples/go/`](https://github.com/HyukjinKwon/adbc-driver-spark/tree/main/examples/go):

| Directory | What it shows |
|-----------|---------------|
| `quickstart/` | `NewDriver`, open a database, run a query, iterate the `RecordReader`. |
| `metadata/` | `GetObjects`, `GetTableSchema`, `GetTableTypes`. |
| `parameters/` | `Prepare` plus `Bind` for parameter binding. |

Run one with:

```bash
go run ./examples/go/quickstart
```

## C

In [`examples/c/`](https://github.com/HyukjinKwon/adbc-driver-spark/tree/main/examples/c):
a minimal program that loads `libadbc_driver_spark` through the ADBC driver
manager and runs a query. See [Using from C and C++](usage-c.md) for the build
commands.

## R

In [`examples/r/`](https://github.com/HyukjinKwon/adbc-driver-spark/tree/main/examples/r):
an `adbcdrivermanager` script that connects and queries. See
[Using from R](usage-r.md).

!!! tip
    Set the URI through an environment variable so the same example works
    against a local server and a remote one, for example
    `SPARK_CONNECT_URI=sc://spark.example.com:443`.
