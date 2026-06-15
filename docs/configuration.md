<!-- SPDX-License-Identifier: Apache-2.0 -->
# Configuration Reference

This page lists every option the driver understands, grouped by the ADBC level
at which it is set (database, connection, or statement). Spark-specific options
use the `adbc.spark.` prefix; standard ADBC options use the `adbc.`
prefix.

Options can be supplied as a `map[string]string` (Go), through `db_kwargs` /
`conn_kwargs` (Python), or inline in the `sc://` connection string where a
matching string field exists.

## Database options

Set these when creating the database (Go `NewDatabase`, Python
`adbc_driver_spark.connect(..., db_kwargs=...)`).

| Option key                            | Connection-string field | Meaning |
|---------------------------------------|--------------------------|---------|
| `uri`                                 | (the whole string)       | Spark Connect connection string. Required. Defaults to `sc://localhost:15002`. |
| `adbc.spark.token`            | `token`                  | Bearer token, sent as `Authorization: Bearer`. Implies TLS. |
| `adbc.spark.tls.enabled`      | `use_ssl` (or `DatabaseOptions.TLS_ENABLED`) | Enable TLS for the gRPC channel: `true` or `false`. Inferred `true` when a token is set. |
| `adbc.spark.user_id`          | `user_id`                | Spark Connect user id for the session. |
| `adbc.spark.user_agent`       | `user_agent`             | User agent advertised to the server. Defaults to `adbc-driver-spark/<version>`. |
| `adbc.spark.session_id`       | `session_id`             | Reuse an existing session id (a UUID). A new session is created when omitted. |
| `adbc.spark.headers.<NAME>`   | (option only)            | Arbitrary gRPC metadata header `<NAME>`. Repeat the prefix per header. |

## Connection options

Set these on the connection (Python `conn_kwargs`, or after open).

| Option key                            | Meaning |
|---------------------------------------|---------|
| `adbc.connection.catalog`             | Set the current catalog (standard ADBC option; runs `USE CATALOG`). |
| `adbc.connection.db_schema`           | Set the current schema/database (standard ADBC option; runs `USE <db>`). |
| `adbc.connection.autocommit`          | Autocommit flag. Always effectively `true`. See below. |

### Autocommit and transactions

Spark Connect has no multi-statement transactions, so the driver runs in
autocommit mode. Setting `adbc.connection.autocommit` to `false`, or calling
`Commit` / `Rollback`, reports `ADBC_STATUS_NOT_IMPLEMENTED`. The Python DBAPI
`connect(..., autocommit=False)` is accepted for API symmetry but transactions
remain unavailable. See [Compatibility](compatibility.md).

## Statement options

Set these on a statement before execution.

| Option key                       | Meaning |
|----------------------------------|---------|
| `adbc.rpc.result_queue_size`     | Number of result batches to prefetch from the server (standard ADBC option). |

## Standard ADBC option keys

The driver honors the standard ADBC keys where they apply:

| Option key                          | Applies to | Meaning |
|-------------------------------------|------------|---------|
| `uri`                               | database   | Connection string. |
| `adbc.connection.catalog`           | connection | Current catalog. |
| `adbc.connection.db_schema`         | connection | Current schema/database. |
| `adbc.connection.autocommit`        | connection | Autocommit (read-only here). |
| `adbc.rpc.result_queue_size`        | statement  | Result prefetch depth. |

## Examples

=== "Python"

    ```python
    import adbc_driver_spark.dbapi as dbapi

    conn = dbapi.connect(
        "sc://spark.example.com:443",
        token="eyJhbGci...",                                  # implies use_ssl=True
        db_kwargs={
            "adbc.spark.user_id": "analyst",
            "adbc.spark.headers.x-request-source": "etl",
        },
        conn_kwargs={
            "adbc.connection.catalog": "spark_catalog",
            "adbc.connection.db_schema": "default",
        },
    )
    ```

=== "Go"

    ```go
    db, err := drv.NewDatabase(map[string]string{
        "uri":                                  "sc://spark.example.com:443",
        "adbc.spark.token":                     "eyJhbGci...",
        "adbc.spark.tls.enabled":               "true",
        "adbc.spark.user_id":                   "analyst",
        "adbc.spark.headers.x-request-source":  "etl",
    })
    ```

In Python, the Spark-specific keys are also available as enum values on
`adbc_driver_spark.DatabaseOptions`, so you can reference
`DatabaseOptions.TOKEN.value` instead of the raw string.
