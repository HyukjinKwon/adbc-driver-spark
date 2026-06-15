<!-- SPDX-License-Identifier: Apache-2.0 -->
# Using from Ruby

Ruby programs use [Red ADBC](https://rubygems.org/gems/red-adbc) (the `adbc`
gem), the Ruby bindings for ADBC. It loads the **Apache Spark Connect** driver
shared library through the ADBC driver manager and resolves the standard
`AdbcDriverInit` entrypoint, returning results as Apache Arrow.

## Install

```bash
gem install red-adbc
```

`red-adbc` depends on the ADBC GLib system library; the
`rubygems-requirements-system` plugin installs it automatically on supported
platforms.

## Connecting and running a query

```ruby
require "adbc"

driver = ENV.fetch("SPARK_DRIVER")   # libadbc_driver_spark.{so,dylib,dll}
uri = ENV.fetch("SPARK_REMOTE", "sc://localhost:15002")

ADBC::Database.open(driver: driver, uri: uri) do |database|
  database.connect do |connection|
    # query runs the SQL and returns an Arrow::Table.
    table = connection.query("SELECT id, id * id AS square FROM range(5)")
    puts table
  end
end
```

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

The complete example is in
[`examples/ruby/`](https://github.com/HyukjinKwon/adbc-driver-spark/tree/main/examples/ruby)
and is run against a live Spark Connect server on every CI run.

!!! tip
    Pass authentication and TLS options as extra keyword arguments to
    `ADBC::Database.open`, for example `token:` and `use_ssl:`. See the
    [Configuration Reference](configuration.md).
