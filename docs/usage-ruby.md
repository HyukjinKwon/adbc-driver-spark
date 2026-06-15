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

```bash
SPARK_DRIVER=/path/to/libadbc_driver_spark.so \
SPARK_REMOTE=sc://localhost:15002 \
ruby quickstart.rb
```

The complete example is in
[`examples/ruby/`](https://github.com/HyukjinKwon/adbc-driver-spark/tree/main/examples/ruby)
and is run against a live Spark Connect server on every CI run.

!!! tip
    Pass authentication and TLS options as extra keyword arguments to
    `ADBC::Database.open`, for example `token:` and `use_ssl:`. See the
    [Configuration Reference](configuration.md).
