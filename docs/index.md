<!-- SPDX-License-Identifier: Apache-2.0 -->
# ADBC Driver for Spark Connect

An [Apache Arrow ADBC](https://arrow.apache.org/adbc/) driver for **Apache Spark Connect**.
It speaks the Spark Connect gRPC protocol and exposes it through the standard ADBC
API, so you get Arrow-native result sets from Spark with zero copy into pandas,
Polars, DuckDB, or any Arrow consumer.

## What it is

[ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) is a
vendor-neutral API for moving Arrow data in and out of databases, in the same
spirit as JDBC and ODBC but columnar from end to end. Spark Connect already
returns query results as Arrow IPC batches over gRPC, which makes it a natural
fit for ADBC: there is no row-by-row conversion and no driver-side reshaping.

The driver is written in Go, compiled to a C-ABI shared library
(`libadbc_driver_spark`) that exports the standard `AdbcDriverInit` entrypoint,
and shipped to every language that has an ADBC driver manager. Go users can also
import the native driver directly.

## Why ADBC plus Spark Connect

- **Arrow native, end to end.** Results stream from Spark as Arrow batches and
  reach your application as Arrow record batches. No per-row boxing.
- **One driver, every language.** A single shared library loads through the ADBC
  driver manager from C/C++, Python, R, Ruby, Rust, and more. The native Go
  driver is available as a regular Go module.
- **A standard surface.** Python users get a PEP 249 (DBAPI 2.0) interface plus
  `fetch_arrow_table()` and `fetch_df()` helpers. C/C++ users get the plain ADBC
  C API. There is no bespoke client to learn.
- **Production focused.** TLS and bearer-token auth, session and configuration
  options, metadata introspection (catalogs, schemas, tables, columns), prepared
  statements with parameter binding, and a CI matrix across Linux, macOS, and
  Windows.

## Feature highlights

- SQL execution returning Arrow record batches.
- Streaming results via an Arrow `RecordReader` (Go) or record batch reader (Python).
- DDL and DML through `ExecuteUpdate`.
- Prepared statements with Arrow parameter binding (positional `?` and named).
- Metadata: `GetObjects`, `GetTableSchema`, `GetTableTypes`, `GetInfo`.
- Full Spark to Arrow type mapping, including decimal, timestamp, timestamp_ntz,
  array, map, and struct. See [Type Mapping](type-mapping.md).
- TLS and bearer-token authentication, custom gRPC headers, session reuse.

!!! note
    Spark Connect is autocommit only. The driver reports transaction operations
    as `ADBC_STATUS_NOT_IMPLEMENTED`. See [Compatibility](compatibility.md).

## Where to go next

- [Installation](installation.md): install for Python, Go, and C/C++/R.
- [Quickstart](quickstart.md): the shortest path to a query.
- [Connecting and Authentication](connecting.md): the `sc://` connection string.
- [Querying Data](querying.md): SQL, streaming, prepared statements.
- [Python DBAPI](python-dbapi.md): PEP 249 usage and DataFrame integration.
- [Configuration Reference](configuration.md): every option.
- [Architecture](architecture.md): how the driver works under the hood.

## Project

This project is maintained by Hyukjin Kwon. The source lives at
[HyukjinKwon/adbc-driver-spark](https://github.com/HyukjinKwon/adbc-driver-spark)
and is licensed under the Apache License, Version 2.0. It is not affiliated with,
endorsed by, or sponsored by the Apache Software Foundation. Apache, Apache
Arrow, Apache Spark, Arrow, and Spark are trademarks of the Apache Software
Foundation.
