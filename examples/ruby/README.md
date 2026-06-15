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

```bash
export SPARK_DRIVER=$(python -c \
  "import adbc_driver_spark, os; print(os.path.join(os.path.dirname(adbc_driver_spark.__file__), 'libadbc_driver_spark.so'))")
SPARK_REMOTE=sc://localhost:15002 ruby quickstart.rb
```
