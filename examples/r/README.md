<!-- SPDX-License-Identifier: Apache-2.0 -->
# R example

[`quickstart.R`](quickstart.R) loads the Spark Connect driver through
[`adbcdrivermanager`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/),
runs a query, and reads the result into a `data.frame`.

## Setup

```r
install.packages("adbcdrivermanager")
```

Start a Spark Connect server reachable at `sc://localhost:15002`, and obtain the
Spark Connect driver shared library by downloading the tarball for your platform
from the [Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases)
page.

## Run

Download the shared library tarball for your platform, extract it, and point
`SPARK_DRIVER` at the extracted library. Each tarball extracts to the current
directory and contains `libadbc_driver_spark.{so,dylib,dll}` plus LICENSE and
NOTICE:

```bash
# Download the shared library for your platform from the Releases page
curl -fsSL -o adbc-spark.tar.gz \
  https://github.com/HyukjinKwon/adbc-driver-spark/releases/latest/download/libadbc_driver_spark-linux-x86_64.tar.gz
tar xzf adbc-spark.tar.gz
export SPARK_DRIVER="$PWD/libadbc_driver_spark.so"   # .dylib on macOS, .dll on Windows

export SPARK_REMOTE=sc://localhost:15002
Rscript quickstart.R
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

## What it shows

- Wrapping the driver shared library with `adbc_driver()` (default
  `AdbcDriverInit` entrypoint).
- Initializing a database with a `sc://` URI via `adbc_database_init()`.
- Opening a connection and running a query with `read_adbc()`.
- Materializing the Arrow result as an R `data.frame`.
