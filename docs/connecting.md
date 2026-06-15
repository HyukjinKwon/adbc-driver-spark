<!-- SPDX-License-Identifier: Apache-2.0 -->
# Connecting and Authentication

Connections use the standard **Apache Spark Connect** connection string, passed
to the driver as the ADBC `uri` option.

## The connection string

```
sc://host:port/;token=<jwt>;use_ssl=true;user_id=<id>;user_agent=<ua>;session_id=<uuid>
```

The scheme is always `sc://`. The host and port default to `localhost:15002`,
which is where a local Spark Connect server listens. Everything after the path
separator (`/;`) is a list of `key=value` pairs separated by semicolons.

| Field        | Meaning                                              | Default |
|--------------|------------------------------------------------------|---------|
| `host:port`  | Spark Connect server address                         | `localhost:15002` |
| `token`      | Bearer token (sent as `Authorization: Bearer`)       | none |
| `use_ssl`    | `true` or `false`; enable TLS for the gRPC channel   | inferred `true` when a token is set |
| `user_id`    | Spark Connect user id for the session                | none |
| `user_agent` | User agent advertised to the server                  | `adbc-driver-spark/<version>` |
| `session_id` | Reuse an existing session id (a UUID)                | new session |

!!! note
    Each option in the connection string also has an equivalent ADBC option key
    (for example `adbc.spark.connect.token`). Inline string fields and option
    keys can be mixed; option keys take precedence. See the
    [Configuration Reference](configuration.md).

## Connecting without authentication

For a local, plaintext Spark Connect server, the URI is all you need.

=== "Python"

    ```python
    import adbc_driver_spark.dbapi as dbapi

    with dbapi.connect("sc://localhost:15002") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_user() AS who")
            print(cur.fetchall())
    ```

=== "Go"

    ```go
    db, err := drv.NewDatabase(map[string]string{
        "uri": "sc://localhost:15002",
    })
    ```

=== "C"

    ```c
    /* The uri option is all you need for a local, plaintext server. */
    AdbcDatabaseNew(&database, &error);
    AdbcDatabaseSetOption(&database, "driver",
                          "/path/to/libadbc_driver_spark.so", &error);
    AdbcDatabaseSetOption(&database, "uri", "sc://localhost:15002", &error);
    AdbcDatabaseInit(&database, &error);
    ```

    !!! note
        The full setup/teardown (error checking, opening a connection,
        releases) and the compile command live in
        [Using from C and C++](usage-c.md).

=== "R"

    ```r
    library(adbcdrivermanager)

    drv <- adbc_driver(Sys.getenv("SPARK_DRIVER"))
    db <- adbc_database_init(drv, uri = "sc://localhost:15002")
    con <- adbc_connection_init(db)
    ```

## TLS and bearer tokens

To reach a TLS endpoint that requires a bearer token, set the token and enable
SSL. When a token is supplied the driver enables TLS automatically, because a
bearer token over plaintext would leak the credential.

=== "Python"

    ```python
    import adbc_driver_spark.dbapi as dbapi

    # Inline in the URI.
    uri = "sc://spark.example.com:443/;token=eyJhbGci...;use_ssl=true"
    with dbapi.connect(uri) as conn:
        ...

    # Or with the convenience keyword (implies use_ssl=True).
    with dbapi.connect("sc://spark.example.com:443", token="eyJhbGci...") as conn:
        ...
    ```

=== "Go"

    ```go
    db, err := drv.NewDatabase(map[string]string{
        "uri":                          "sc://spark.example.com:443",
        "adbc.spark.connect.token":     "eyJhbGci...",
        "adbc.spark.connect.use_ssl":   "true",
    })
    ```

=== "C"

    ```c
    /* Auth settings are plain database options. Setting a token enables TLS. */
    AdbcDatabaseSetOption(&database, "uri", "sc://spark.example.com:443", &error);
    AdbcDatabaseSetOption(&database, "adbc.spark.connect.token", "eyJhbGci...",
                          &error);
    AdbcDatabaseSetOption(&database, "adbc.spark.connect.use_ssl", "true", &error);
    ```

=== "R"

    ```r
    # Pass Spark options as named arguments to adbc_database_init().
    db <- adbc_database_init(
        drv,
        uri = "sc://spark.example.com:443",
        adbc.spark.connect.token = "eyJhbGci...",
        adbc.spark.connect.use_ssl = "true")
    ```

!!! warning
    Never commit tokens to source control. Read them from the environment or a
    secret store and inject them at connect time.

## Custom headers

Arbitrary gRPC metadata headers can be attached to every request using the
`adbc.spark.connect.headers.<NAME>` option prefix, or inline header fields in
the URI where the server supports them.

=== "Python"

    ```python
    import adbc_driver_spark
    import adbc_driver_spark.dbapi as dbapi

    headers = adbc_driver_spark.DatabaseOptions.HEADER_PREFIX.value  # "adbc.spark.connect.headers."
    with dbapi.connect(
        "sc://spark.example.com:443",
        token="eyJhbGci...",
        db_kwargs={f"{headers}x-request-source": "analytics-team"},
    ) as conn:
        ...
    ```

=== "Go"

    ```go
    db, err := drv.NewDatabase(map[string]string{
        "uri":                                       "sc://spark.example.com:443",
        "adbc.spark.connect.token":                  "eyJhbGci...",
        "adbc.spark.connect.headers.x-request-source": "analytics-team",
    })
    ```

=== "C"

    ```c
    /* Attach a gRPC metadata header with the headers.<NAME> option prefix. */
    AdbcDatabaseSetOption(&database, "uri", "sc://spark.example.com:443", &error);
    AdbcDatabaseSetOption(&database, "adbc.spark.connect.token", "eyJhbGci...",
                          &error);
    AdbcDatabaseSetOption(&database,
                          "adbc.spark.connect.headers.x-request-source",
                          "analytics-team", &error);
    ```

=== "R"

    ```r
    db <- adbc_database_init(
        drv,
        uri = "sc://spark.example.com:443",
        adbc.spark.connect.token = "eyJhbGci...",
        `adbc.spark.connect.headers.x-request-source` = "analytics-team")
    ```

## Databricks-style bearer token

Databricks Connect compatible endpoints authenticate with a personal access
token (or OAuth token) carried as a bearer token over TLS, and identify the
workspace cluster through a header. The shape is the same as any other
token-and-TLS endpoint.

=== "Python"

    ```python
    import os
    import adbc_driver_spark
    import adbc_driver_spark.dbapi as dbapi

    headers = adbc_driver_spark.DatabaseOptions.HEADER_PREFIX.value
    with dbapi.connect(
        "sc://dbc-12345678-90ab.cloud.databricks.com:443",
        token=os.environ["DATABRICKS_TOKEN"],   # implies use_ssl=True
        db_kwargs={
            f"{headers}x-databricks-cluster-id": os.environ["DATABRICKS_CLUSTER_ID"],
        },
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW CATALOGS")
            print(cur.fetchall())
    ```

=== "Go"

    ```go
    db, err := drv.NewDatabase(map[string]string{
        "uri":                                          "sc://dbc-12345678-90ab.cloud.databricks.com:443",
        "adbc.spark.connect.token":                     os.Getenv("DATABRICKS_TOKEN"),
        "adbc.spark.connect.use_ssl":                   "true",
        "adbc.spark.connect.headers.x-databricks-cluster-id": os.Getenv("DATABRICKS_CLUSTER_ID"),
    })
    ```

=== "C"

    ```c
    /* Token over TLS plus the cluster id header, read from the environment. */
    AdbcDatabaseSetOption(&database, "uri",
                          "sc://dbc-12345678-90ab.cloud.databricks.com:443", &error);
    AdbcDatabaseSetOption(&database, "adbc.spark.connect.token",
                          getenv("DATABRICKS_TOKEN"), &error);
    AdbcDatabaseSetOption(&database, "adbc.spark.connect.use_ssl", "true", &error);
    AdbcDatabaseSetOption(&database,
                          "adbc.spark.connect.headers.x-databricks-cluster-id",
                          getenv("DATABRICKS_CLUSTER_ID"), &error);
    ```

=== "R"

    ```r
    db <- adbc_database_init(
        drv,
        uri = "sc://dbc-12345678-90ab.cloud.databricks.com:443",
        adbc.spark.connect.token = Sys.getenv("DATABRICKS_TOKEN"),
        adbc.spark.connect.use_ssl = "true",
        `adbc.spark.connect.headers.x-databricks-cluster-id` =
            Sys.getenv("DATABRICKS_CLUSTER_ID"))
    ```

## Session reuse

Pass `session_id` (a UUID) to attach to an existing Spark Connect session
instead of creating a new one. This lets temporary views and session
configuration carry across connections.

```python
with dbapi.connect(
    "sc://localhost:15002/;session_id=2a1b9c64-7c1d-4e2f-9a3b-0f8e6d5c4b3a"
) as conn:
    ...
```

See [Configuration Reference](configuration.md) for the full option list and
[Troubleshooting](troubleshooting.md) if a connection fails.
