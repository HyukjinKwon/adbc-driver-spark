<!-- SPDX-License-Identifier: Apache-2.0 -->
# Using from R

R loads the shared library through the
[`adbcdrivermanager`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/)
package, the same way it loads any other ADBC driver.

## Prerequisites

Install the ADBC driver manager (and optionally the Arrow R package) from CRAN:

```r
install.packages("adbcdrivermanager")
install.packages("arrow")   # optional, for Arrow-native results
```

Obtain `libadbc_driver_spark.{so,dylib,dll}` from
[Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases) or build it
from source (see [Installation](installation.md)).

## Connecting and querying

Construct a driver from the shared library path, initialize a database with the
`sc://` URI, then open a connection and run a query.

```r
library(adbcdrivermanager)

# Point at the shared library. Use the path for your platform.
driver <- adbc_driver("/path/to/libadbc_driver_spark.so")

# Initialize the database with the Spark Connect URI.
db <- adbc_database_init(driver, uri = "sc://localhost:15002")

# Open a connection.
con <- adbc_connection_init(db)

# Run a query. read_adbc returns an Arrow stream you can collect.
result <- con |>
  read_adbc("SELECT id, id * id AS square FROM range(5)") |>
  tibble::as_tibble()

print(result)

# Clean up.
adbc_connection_release(con)
adbc_database_release(db)
```

## Authentication

Pass Spark options as additional named arguments to `adbc_database_init`. For a
bearer token over TLS:

```r
db <- adbc_database_init(
  driver,
  uri = "sc://spark.example.com:443",
  adbc.spark.connect.token = Sys.getenv("SPARK_TOKEN"),
  adbc.spark.connect.use_ssl = "true"
)
```

See the [Configuration Reference](configuration.md) for the full option list.

## DDL and DML

For statements that do not return rows, use `execute_adbc`:

```r
con |> execute_adbc("CREATE TABLE IF NOT EXISTS events (id BIGINT, kind STRING) USING parquet")
con |> execute_adbc("INSERT INTO events VALUES (1, 'click')")
```

!!! tip
    Use `manage_lifecycle()` from `adbcdrivermanager` to scope database,
    connection, and statement objects so they are released automatically.

!!! note
    Results arrive as Arrow streams, which collect into a tibble or a base R
    data frame with no per-row conversion, following the
    [Type Mapping](type-mapping.md).
