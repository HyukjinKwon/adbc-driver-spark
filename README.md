# ADBC Driver for Spark Connect

[![CI](https://github.com/HyukjinKwon/adbc-driver-spark/actions/workflows/ci.yml/badge.svg)](https://github.com/HyukjinKwon/adbc-driver-spark/actions/workflows/ci.yml)
[![Docs](https://github.com/HyukjinKwon/adbc-driver-spark/actions/workflows/docs.yml/badge.svg)](https://hyukjinkwon.github.io/adbc-driver-spark/)
[![PyPI](https://img.shields.io/pypi/v/adbc-driver-spark.svg)](https://pypi.org/project/adbc-driver-spark/)
[![Go Reference](https://pkg.go.dev/badge/github.com/HyukjinKwon/adbc-driver-spark.svg)](https://pkg.go.dev/github.com/HyukjinKwon/adbc-driver-spark)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An [Apache Arrow ADBC](https://arrow.apache.org/adbc/) driver for
[Apache Spark Connect](https://spark.apache.org/spark-connect/). It speaks the
Spark Connect gRPC protocol and exposes it through the standard ADBC API, so you
get Arrow-native result sets from Spark with zero copy into pandas, Polars,
DuckDB, or any Arrow consumer.

> Documentation: <https://hyukjinkwon.github.io/adbc-driver-spark/>

## Why this driver

[ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) is a
vendor-neutral API for moving Arrow data in and out of databases, in the same
spirit as JDBC and ODBC but columnar from end to end. Spark Connect already
returns query results as Arrow IPC batches over gRPC, which makes it a natural
fit for ADBC: there is no row-by-row conversion and no driver-side reshaping.

- **Arrow native, end to end.** Results stream from Spark as Arrow batches and
  reach your application as Arrow record batches. No per-row boxing.
- **One driver, every language.** The driver is built in Go and compiled to a
  C-ABI shared library that exposes the standard `AdbcDriverInit` entrypoint. It
  loads through the ADBC driver manager from C/C++, Python, R, Ruby, Rust, and Go.
- **Standard surface.** Python users get a PEP 249 (DBAPI 2.0) interface and
  `fetch_arrow_table()` / `fetch_df()` helpers. C/C++ users get the plain ADBC
  C API. No bespoke client to learn.
- **Production focused.** TLS and bearer-token auth, session and configuration
  options, metadata introspection (catalogs, schemas, tables, columns), prepared
  statements with parameter binding, and a CI matrix across Linux, macOS, and
  Windows.

## Install

### Python

```bash
pip install adbc-driver-spark
```

This pulls in the prebuilt shared library for your platform, plus
`adbc-driver-manager` and `pyarrow`.

### Go

```bash
go get github.com/HyukjinKwon/adbc-driver-spark
```

### C / C++ / R and other languages

Download the shared library (`libadbc_driver_spark.{so,dylib,dll}`) from the
[Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases) page, or
build it from source (see [Installation](https://hyukjinkwon.github.io/adbc-driver-spark/installation/)),
then load it with your language's ADBC driver manager.

## Quickstart

Start a Spark Connect server (Spark 4.0.x or 4.1.x):

```bash
# From a Spark 4.x distribution (the Connect server is bundled)
./sbin/start-connect-server.sh
# Spark Connect listens on sc://localhost:15002 by default
# (On Spark 3.5.x, which does not bundle it, add:
#  --packages org.apache.spark:spark-connect_2.13:3.5.8)
```

### Python

```python
import adbc_driver_spark.dbapi as dbapi

with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id, id * id AS square FROM range(5)")
        table = cur.fetch_arrow_table()   # pyarrow.Table
        print(table.to_pandas())
```

### Go

```go
package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	spark "github.com/HyukjinKwon/adbc-driver-spark/driver/spark"
)

func main() {
	drv := spark.NewDriver(memory.DefaultAllocator)
	db, _ := drv.NewDatabase(map[string]string{
		"uri": "sc://localhost:15002",
	})
	defer db.Close()

	cnxn, _ := db.Open(context.Background())
	defer cnxn.Close()

	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()

	stmt.SetSqlQuery("SELECT id, id * id AS square FROM range(5)")
	reader, _, _ := stmt.ExecuteQuery(context.Background())
	defer reader.Release()

	for reader.Next() {
		fmt.Println(reader.Record())
	}
}
```

See the [examples](examples/) directory and the
[documentation](https://hyukjinkwon.github.io/adbc-driver-spark/quickstart/)
for C, R, and more.

## Connecting and authentication

Connections use the standard Spark Connect connection string, passed as the ADBC
`uri` option:

```
sc://host:port/;token=<jwt>;use_ssl=true;user_id=<id>;user_agent=<ua>
```

Common options:

| Option                                   | Meaning                                   |
|------------------------------------------|-------------------------------------------|
| `uri`                                    | Spark Connect connection string (required)|
| `adbc.spark.connect.token`               | Bearer token for authentication           |
| `adbc.spark.connect.use_ssl`             | `true` or `false`                         |
| `adbc.spark.connect.user_id`             | Spark Connect user id                     |
| `adbc.spark.connect.user_agent`          | Custom user agent string                  |
| `adbc.spark.connect.headers.<NAME>`      | Extra gRPC metadata header                |
| `adbc.spark.connect.timeout_seconds`     | Per-RPC timeout                           |

See the [Configuration Reference](https://hyukjinkwon.github.io/adbc-driver-spark/configuration/)
for the full list.

## Features

- SQL execution returning Arrow record batches.
- Prepared statements with Arrow parameter binding.
- DML and DDL via `ExecuteUpdate`.
- Metadata: `GetObjects`, `GetTableSchema`, `GetTableTypes`, `GetInfo`.
- Full Spark to Arrow type mapping, including decimal, timestamp, timestamp_ntz,
  array, map, and struct. See [Type Mapping](https://hyukjinkwon.github.io/adbc-driver-spark/type-mapping/).
- TLS and bearer-token authentication.
- Works against Spark Connect on Spark 3.5.x and 4.0.x, and Databricks Connect
  compatible endpoints.

## Compatibility

| Component        | Supported                                            |
|------------------|------------------------------------------------------|
| Spark Connect    | Spark 3.5.x, 4.0.x (protos pinned to 4.0.0)          |
| ADBC API         | 1.1.0                                                |
| Python           | 3.9 - 3.13                                            |
| Go               | 1.25+                                                |
| Platforms        | Linux (x86_64, aarch64), macOS (x86_64, arm64), Windows (x86_64) |

See [Compatibility and Conformance](https://hyukjinkwon.github.io/adbc-driver-spark/compatibility/)
for the ADBC conformance matrix and known limitations.

## Documentation

Full guides live at <https://hyukjinkwon.github.io/adbc-driver-spark/>:

- [Installation](https://hyukjinkwon.github.io/adbc-driver-spark/installation/)
- [Quickstart](https://hyukjinkwon.github.io/adbc-driver-spark/quickstart/)
- [Connecting and Authentication](https://hyukjinkwon.github.io/adbc-driver-spark/connecting/)
- [Querying Data](https://hyukjinkwon.github.io/adbc-driver-spark/querying/)
- [Python DBAPI](https://hyukjinkwon.github.io/adbc-driver-spark/python-dbapi/)
- [Type Mapping](https://hyukjinkwon.github.io/adbc-driver-spark/type-mapping/)
- [Architecture](https://hyukjinkwon.github.io/adbc-driver-spark/architecture/)

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to set
up a development environment, run the tests, and submit changes. By participating
you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
