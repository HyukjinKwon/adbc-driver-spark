<!-- SPDX-License-Identifier: Apache-2.0 -->
# R example

[`quickstart.R`](quickstart.R) loads the Spark Connect driver through
[`adbcdrivermanager`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/),
runs a query, and reads the result into a `data.frame`.

## Setup

```r
install.packages("adbcdrivermanager")
```

Start a Spark Connect server reachable at `sc://localhost:15002`, and locate the
Spark Connect driver shared library. The copy bundled in the Python wheel works
well (`libadbc_driver_spark.so` on Linux, `.dylib` on macOS,
`adbc_driver_spark.dll` on Windows).

## Run

```bash
export SPARK_DRIVER=/path/to/libadbc_driver_spark.so
export SPARK_REMOTE=sc://localhost:15002
Rscript quickstart.R
```

If the driver came from the Python wheel, you can find it with:

```bash
export SPARK_DRIVER=$(python -c \
  "import adbc_driver_spark, pathlib; \
   print(next(pathlib.Path(adbc_driver_spark.__file__).parent.glob('libadbc_driver_spark.*')))")
```

## What it shows

- Wrapping the driver shared library with `adbc_driver()` (default
  `AdbcDriverInit` entrypoint).
- Initializing a database with a `sc://` URI via `adbc_database_init()`.
- Opening a connection and running a query with `read_adbc()`.
- Materializing the Arrow result as an R `data.frame`.
