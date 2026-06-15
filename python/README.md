# adbc-driver-spark (Python)

[Apache Arrow ADBC](https://arrow.apache.org/adbc/) driver for
[Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).

It lets you run SQL against a Spark Connect server and get results back as
Apache Arrow, with a standard ADBC and DBAPI 2.0 (PEP 249) interface. The
package bundles a native shared library built from Go, so there is no JVM and
no PySpark dependency.

## Install

```bash
pip install adbc-driver-spark
```

Optional extras: `adbc-driver-spark[pandas]` for `fetch_df()`,
`adbc-driver-spark[polars]` for Polars output.

## Quickstart (DBAPI 2.0)

```python
import adbc_driver_spark.dbapi as dbapi

with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id, id * 2 AS doubled FROM range(5)")
        print(cur.fetchall())
        # Arrow / pandas in one shot:
        cur.execute("SELECT * FROM range(1000)")
        table = cur.fetch_arrow_table()   # pyarrow.Table
        df = cur.fetch_df()               # pandas.DataFrame (needs [pandas])
```

Connect to a secured server with a bearer token (TLS is implied):

```python
conn = dbapi.connect("sc://my-host:443", token="my-jwt-token")
```

## Low level ADBC

```python
import adbc_driver_spark

db = adbc_driver_spark.connect(
    "sc://localhost:15002",
    db_kwargs={adbc_driver_spark.DatabaseOptions.USER_AGENT.value: "my-app/1.0"},
)
db.close()
```

## Options

See `adbc_driver_spark.DatabaseOptions`. Everything that can go in the connection string
(`sc://host:port/;token=...;use_ssl=true`) can also be passed via `db_kwargs`.

## Development

The native library `libadbc_driver_spark.{so,dylib,dll}` must sit inside the
`adbc_driver_spark/` package directory (or on the loader path) at runtime. From
a source checkout:

```bash
make python-dev      # builds the Go shared lib, copies it into the package,
                     # and `pip install -e python`
pytest python/tests  # unit tests run without a server; integration tests
                     # are skipped unless SPARK_CONNECT_URI is set
```

See the [project documentation](https://hyukjinkwon.github.io/adbc-driver-spark/)
for the full guide.
