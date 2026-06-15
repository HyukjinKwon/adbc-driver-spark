<!-- SPDX-License-Identifier: Apache-2.0 -->
# C example

[`quickstart.c`](quickstart.c) loads the Spark Connect driver through the ADBC
**driver manager**, runs a query, and reads the Arrow result stream.

## How it works

The driver manager (`libadbc_driver_manager`) provides the implementations of
the `AdbcDatabase` / `AdbcConnection` / `AdbcStatement` functions declared in
[`adbc.h`](../../c/arrow-adbc/adbc.h). You tell it which driver to load by
setting the `driver` database option to the path of the Spark Connect shared
library. The manager `dlopen()`s the library and resolves the default
`AdbcDriverInit` entrypoint, which the Spark driver exports, so no `entrypoint`
option is required.

## Build

You need the ADBC C headers (`adbc.h`, vendored here) and the driver manager
shared library. Install the driver manager from your package manager or build
it from [apache/arrow-adbc](https://github.com/apache/arrow-adbc), then:

```bash
cc examples/c/quickstart.c \
    -Ic/arrow-adbc \
    -ladbc_driver_manager \
    -o quickstart
```

Add `-I`/`-L` flags pointing at wherever the driver manager headers and library
live on your system if they are not on the default search path.

## Run

Point `SPARK_DRIVER` at the Spark Connect shared library. Download the tarball
for your platform from the
[Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases) page and
extract it. Each tarball extracts to the current directory and contains
`libadbc_driver_spark.{so,dylib,dll}` plus LICENSE and NOTICE:

```bash
# Download the shared library for your platform from the Releases page
curl -fsSL -o adbc-spark.tar.gz \
  https://github.com/HyukjinKwon/adbc-driver-spark/releases/latest/download/libadbc_driver_spark-linux-x86_64.tar.gz
tar xzf adbc-spark.tar.gz
export SPARK_DRIVER="$PWD/libadbc_driver_spark.so"   # .dylib on macOS, .dll on Windows

export SPARK_REMOTE=sc://localhost:15002
./quickstart
```

Pick the matching asset for your platform: `libadbc_driver_spark-linux-x86_64.tar.gz`,
`libadbc_driver_spark-linux-aarch64.tar.gz`, `libadbc_driver_spark-macos-x86_64.tar.gz`,
`libadbc_driver_spark-macos-arm64.tar.gz`, or `libadbc_driver_spark-windows-x86_64.tar.gz`.
Start a Spark Connect server reachable at the `SPARK_REMOTE` URI before running.

Alternatively, if you already have the Python package installed
(`pip install adbc-driver-spark`), the bundled library is at:

```bash
export SPARK_DRIVER=$(python -c \
  "import adbc_driver_spark, pathlib; \
   print(next(pathlib.Path(adbc_driver_spark.__file__).parent.glob('libadbc_driver_spark.*')))")
```

## Going further

The example counts rows and batches to stay dependency-free. To read individual
column values, consume the returned `ArrowArrayStream` with
[nanoarrow](https://github.com/apache/arrow-nanoarrow) or the Arrow C++ library
via the Arrow C data interface.
