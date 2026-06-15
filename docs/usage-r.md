<!-- SPDX-License-Identifier: Apache-2.0 -->
# Using from R

R loads the shared library through the
[`adbcdrivermanager`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/)
package, the same way it loads any other ADBC driver. Against
**Apache Spark Connect**, `adbc_driver()` wraps the shared library and the
manager resolves the default `AdbcDriverInit` entrypoint that the driver
exports, so no entrypoint argument is needed.

## Prerequisites

Install the ADBC driver manager (and optionally the Arrow R package) from CRAN:

```r
install.packages("adbcdrivermanager")
install.packages("arrow")   # optional, for Arrow-native results
```

Obtain `libadbc_driver_spark.{so,dylib,dll}` by downloading the tarball for your
platform from
[Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases) (or build it
from source, see [Installation](installation.md)). Start a Spark Connect server
reachable at `sc://localhost:15002` before running.

## The example script

The runnable example lives at
[`examples/r/quickstart.R`](https://github.com/HyukjinKwon/adbc-driver-spark/blob/main/examples/r/quickstart.R).
It wraps the shared library with `adbc_driver()`, initializes a database with the
`sc://` URI, opens a connection, runs a query with `read_adbc()`, and
materializes the Arrow result as a base R `data.frame`. The `with_adbc()` helper
closes each handle when the block exits, even on error.

```r
# SPDX-License-Identifier: Apache-2.0

library(adbcdrivermanager)

driver_path <- Sys.getenv("SPARK_DRIVER")
uri <- Sys.getenv("SPARK_REMOTE", "sc://localhost:15002")

if (!nzchar(driver_path)) {
  stop("Set SPARK_DRIVER to the libadbc_driver_spark shared library path.")
}

# adbc_driver() wraps the shared library. The default entrypoint
# ("AdbcDriverInit") is exactly what the Spark driver exports, so no entrypoint
# argument is needed.
drv <- adbc_driver(driver_path)

# Initialize the database with the Spark Connect URI, then open a connection
# (one Spark Connect session). The with_adbc() helper closes each handle when
# the block exits, even on error.
db <- adbc_database_init(drv, uri = uri)
with_adbc(db, {
  con <- adbc_connection_init(db)
  with_adbc(con, {
    # read_adbc() executes a query and returns a streaming result; as.data.frame
    # materializes it. The driver returns native Arrow data underneath.
    result <- read_adbc(con, "SELECT id, id * id AS square FROM range(5)")
    df <- as.data.frame(result)
    print(df)
  })
})
```

## Running

Download the shared library tarball for your platform from the
[Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases) page,
extract it, point `SPARK_DRIVER` at the extracted library, and run the script
with `Rscript`:

```bash
# Download the shared library for your platform from the Releases page
curl -fsSL -o adbc-spark.tar.gz \
  https://github.com/HyukjinKwon/adbc-driver-spark/releases/latest/download/libadbc_driver_spark-linux-x86_64.tar.gz
tar xzf adbc-spark.tar.gz
export SPARK_DRIVER="$PWD/libadbc_driver_spark.so"   # .dylib on macOS, .dll on Windows

export SPARK_REMOTE=sc://localhost:15002
Rscript examples/r/quickstart.R
```

Pick the matching asset for your platform: `libadbc_driver_spark-linux-x86_64.tar.gz`,
`libadbc_driver_spark-linux-aarch64.tar.gz`, `libadbc_driver_spark-macos-x86_64.tar.gz`,
`libadbc_driver_spark-macos-arm64.tar.gz`, or `libadbc_driver_spark-windows-x86_64.tar.gz`.

Alternatively, if you already have the Python package installed
(`pip install adbc-driver-spark`), the bundled library is at:

```bash
export SPARK_DRIVER=$(python -c \
  "import adbc_driver_spark, pathlib; \
   print(next(pathlib.Path(adbc_driver_spark.__file__).parent.glob('libadbc_driver_spark.*')))")
```

## Connecting and querying interactively

The same lifecycle works without the `with_adbc()` blocks. Construct a driver
from the shared library path, initialize a database with the `sc://` URI, then
open a connection and run a query.

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
