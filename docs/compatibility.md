<!-- SPDX-License-Identifier: Apache-2.0 -->
# Compatibility and Conformance

This page documents which **Apache Spark Connect** versions the driver supports,
which ADBC features it implements, and how each language consumes it.

## Supported Spark versions

| Spark version | Status     | Notes |
|---------------|------------|-------|
| 4.1.x         | Supported  | Primary target. |
| 4.0.x         | Supported  | Wire-compatible; Spark Connect protos are pinned to v4.1.2. |
| 3.5.x         | Supported  | Uses the Scala 2.13 build of Spark 3.5. |

Every supported line is exercised on every CI run against a live Spark Connect
server (Spark 4.1.x, 4.0.x, and 3.5.x). The driver targets the stable Spark
Connect gRPC surface, so it is wire-compatible across these versions. Databricks
Connect compatible endpoints that speak the same protocol work as well; see
[Connecting and Authentication](connecting.md).

## ADBC feature conformance

| ADBC capability                       | Supported | Notes |
|---------------------------------------|-----------|-------|
| SQL query execution (Arrow results)   | Yes       | `SetSqlQuery` + `ExecuteQuery`. |
| Result streaming (record batches)     | Yes       | Arrow IPC streamed from the server. |
| `ExecuteUpdate` (DDL/DML)             | Yes       | Returns affected rows, or `-1` if unknown. |
| Prepared statements                   | Yes       | `Prepare`. |
| Parameter binding                     | Yes       | `Bind` / `BindStream`, positional `?` and named. |
| `GetObjects`                          | Yes       | Catalogs, schemas, tables, columns. |
| `GetTableSchema`                      | Yes       | |
| `GetTableTypes`                       | Yes       | |
| `GetInfo`                             | Yes       | Driver and server info. |
| TLS / bearer-token auth               | Yes       | |
| Custom gRPC headers                   | Yes       | `adbc.spark.headers.<NAME>`. |
| Autocommit                            | Yes       | Always on. |
| Transactions (`Commit` / `Rollback`)  | No        | Reports `ADBC_STATUS_NOT_IMPLEMENTED`. |
| Substrait execution                   | No        | Reports `ADBC_STATUS_NOT_IMPLEMENTED`. |

!!! warning
    Spark Connect has no multi-statement transactions. Any attempt to disable
    autocommit, commit, or roll back returns `ADBC_STATUS_NOT_IMPLEMENTED`. Plan
    workloads around autocommit semantics.

## Spark Connect RPCs used

The driver maps ADBC operations onto these Spark Connect service RPCs:

| RPC                  | Used for |
|----------------------|----------|
| `ExecutePlan`        | Running SQL and streaming Arrow IPC results. Reattachable execution is used for long-running queries. |
| `AnalyzePlan`        | Resolving result and table schemas (`GetTableSchema`). |
| `Config`             | Reading and setting session configuration. |
| `ReattachExecute`    | Resuming a result stream after an interruption. |
| `ReleaseExecute`     | Releasing server-side execution state when done. |

Metadata methods (`GetObjects` and friends) are served through Spark catalog
relations submitted via `ExecutePlan`. See [Architecture](architecture.md).

## Language and driver-manager support

| Language | How it loads the driver | Reference |
|----------|-------------------------|-----------|
| Python   | Bundled shared library via `adbc_driver_manager` | [Python DBAPI](python-dbapi.md) |
| Go       | Native module, no cgo | [Using from Go](usage-go.md) |
| C / C++  | Shared library via the ADBC driver manager | [Using from C and C++](usage-c.md) |
| R        | Shared library via `adbcdrivermanager` | [Using from R](usage-r.md) |
| Ruby, Rust, others | Shared library via the language's ADBC driver manager | [Installation](installation.md) |

| Component  | Supported |
|------------|-----------|
| ADBC API   | 1.1.0 |
| Python     | 3.9 - 3.13 |
| Go         | 1.25+ |
| Platforms  | Linux (x86_64, aarch64), macOS (x86_64, arm64), Windows (x86_64) |
