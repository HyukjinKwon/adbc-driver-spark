<!-- SPDX-License-Identifier: Apache-2.0 -->
# Ruby example

Uses [Red ADBC](https://rubygems.org/gems/red-adbc) (the `adbc` gem), the Ruby
bindings for ADBC, to load `libadbc_driver_spark` and query a Spark Connect
server.

## Setup

```bash
gem install red-adbc
```

`red-adbc` depends on the ADBC GLib system library; the
`rubygems-requirements-system` plugin installs it automatically on supported
platforms.

## Run

Download the shared library tarball for your platform from the
[Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases) page and
extract it. Each tarball extracts to the current directory and contains
`libadbc_driver_spark.{so,dylib,dll}` plus LICENSE and NOTICE:

```bash
# Download the shared library for your platform from the Releases page
curl -fsSL -o adbc-spark.tar.gz \
  https://github.com/HyukjinKwon/adbc-driver-spark/releases/latest/download/libadbc_driver_spark-linux-x86_64.tar.gz
tar xzf adbc-spark.tar.gz
export SPARK_DRIVER="$PWD/libadbc_driver_spark.so"   # .dylib on macOS, .dll on Windows

SPARK_REMOTE=sc://localhost:15002 ruby quickstart.rb
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
