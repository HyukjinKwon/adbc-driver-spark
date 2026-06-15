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

The driver does not implement connection-level option setters; calling
`SetOption` on a connection returns `ADBC_STATUS_NOT_IMPLEMENTED`. To select a
catalog or schema, run it as SQL on a statement (`USE CATALOG <c>`, `USE <db>`),
or pass the catalog and schema arguments to the metadata methods
(`GetObjects`, `GetTableSchema`).

### Autocommit and transactions

Spark Connect has no multi-statement transactions, so the driver always operates
in autocommit mode. `Commit` and `Rollback` report
`ADBC_STATUS_NOT_IMPLEMENTED`. The Python DBAPI `connect(..., autocommit=False)`
is accepted for API symmetry but transactions remain unavailable. See
[Compatibility](compatibility.md).

## Statement options

The driver defines no statement-level options; calling `SetOption` on a
statement returns `ADBC_STATUS_NOT_IMPLEMENTED`. Results are streamed one batch
at a time with no tunable prefetch (see [Architecture](architecture.md)).

## Standard ADBC option keys

The driver honors these standard ADBC database keys (all set at the database
level), in addition to the `adbc.spark.` keys above:

| Option key   | Applies to | Meaning |
|--------------|------------|---------|
| `uri`        | database   | Spark Connect connection string. |
| `username`   | database   | Mapped to the Spark Connect user id. |
| `password`   | database   | Mapped to the bearer token. |

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
    )

    # Select a catalog or schema with SQL rather than a connection option.
    with conn.cursor() as cur:
        cur.execute("USE CATALOG spark_catalog")
        cur.execute("USE default")
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
