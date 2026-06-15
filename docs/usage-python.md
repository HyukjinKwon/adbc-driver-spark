<!-- SPDX-License-Identifier: Apache-2.0 -->
# Using from Python

Python programs use the `adbc-driver-spark` package, which bundles the shared
library and exposes it through the standard ADBC driver manager and a PEP 249
(DBAPI 2.0) interface. The usage matches every other ADBC Python driver, so it
talks to **Apache Spark Connect** with the same code shape you would use for
PostgreSQL or SQLite over ADBC.

## Install

```bash
pip install adbc-driver-spark
```

This pulls in the prebuilt shared library for your platform plus
`adbc-driver-manager` and `pyarrow`.

## Connecting and running a query

The DBAPI facade is the most common entry point. `connect()` defaults to
`sc://localhost:15002`, and both the connection and cursor are context managers.

```python
import adbc_driver_spark.dbapi as dbapi

with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id, id * id AS square FROM range(5)")
        for row in cur.fetchall():
            print(row)
```

`connect()` also accepts `token=`, `use_ssl=`, `db_kwargs=`, and `conn_kwargs=`
for authenticated and TLS endpoints (see
[Connecting and Authentication](connecting.md)).

## Arrow and pandas

Because results stream back as native Arrow, fetching an Arrow table is
zero-copy; converting to pandas reuses those buffers.

```python
with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM range(10)")
        table = cur.fetch_arrow_table()   # pyarrow.Table
        df = cur.fetch_df()               # pandas.DataFrame (re-run the query)
```

Polars, DuckDB, and any other Arrow consumer work the same way; see
[Ecosystem Integrations](integrations.md).

## DDL, DML, and parameters

```python
with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TEMP VIEW v AS SELECT id FROM range(100)")
        # Positional (qmark) parameters.
        cur.execute("SELECT COUNT(*) FROM v WHERE id < ?", (10,))
        print(cur.fetchone())
```

## Metadata

ADBC metadata helpers live on the connection (not the cursor).

```python
with dbapi.connect("sc://localhost:15002") as conn:
    print(conn.adbc_get_table_types())                 # ['TABLE', 'VIEW', ...]
    schema = conn.adbc_get_table_schema("v")           # pyarrow.Schema
    objects = conn.adbc_get_objects(depth="all").read_all()
```

See [Metadata and Catalogs](metadata.md) for the full structure.

## Low-level (without DBAPI)

If you want the raw ADBC handles rather than the DBAPI wrapper:

```python
import adbc_driver_spark

db = adbc_driver_spark.connect("sc://localhost:15002")
# db is an adbc_driver_manager.AdbcDatabase; pair it with AdbcConnection /
# AdbcStatement, or just use the dbapi module above.
```

## Next steps

- [Python DBAPI](python-dbapi.md): the full PEP 249 reference (cursors,
  fetch methods, transactions, type handling).
- [Ecosystem Integrations](integrations.md): pandas, Polars, DuckDB, PyArrow.
- [Configuration Reference](configuration.md): every connection option.
