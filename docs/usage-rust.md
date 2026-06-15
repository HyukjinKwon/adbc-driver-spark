<!-- SPDX-License-Identifier: Apache-2.0 -->
# Using from Rust

Rust programs load the driver through the [`adbc_driver_manager`](https://crates.io/crates/adbc_driver_manager)
crate, which `dlopen`s the **Apache Spark Connect** driver shared library and
resolves its standard `AdbcDriverInit` entrypoint. The crate builds the driver
manager itself, so no system package is required.

## Dependencies

```toml
# Cargo.toml
[dependencies]
adbc_core = "0.23"
adbc_driver_manager = "0.23"
```

## Connecting and running a query

```rust
use std::env;
use std::error::Error;

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_manager::ManagedDriver;

fn main() -> Result<(), Box<dyn Error>> {
    let driver_path = env::var("SPARK_DRIVER")?;      // libadbc_driver_spark.{so,dylib,dll}
    let uri = env::var("SPARK_REMOTE").unwrap_or_else(|_| "sc://localhost:15002".to_string());

    // entrypoint = None uses the default AdbcDriverInit symbol.
    let mut driver = ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::V110)?;
    let mut database = driver.new_database_with_opts([(OptionDatabase::Uri, uri.as_str().into())])?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement.set_sql_query("SELECT id, id * id AS square FROM range(5)")?;

    let reader = statement.execute()?;   // a RecordBatchReader
    let mut rows = 0usize;
    for batch in reader {
        rows += batch?.num_rows();
    }
    println!("read {rows} rows");
    Ok(())
}
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

SPARK_REMOTE=sc://localhost:15002 cargo run
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
[`examples/rust/`](https://github.com/HyukjinKwon/adbc-driver-spark/tree/main/examples/rust)
and is run against a live Spark Connect server on every CI run.

!!! tip
    For TLS and bearer-token endpoints, add the matching database options, for
    example `(OptionDatabase::Other("adbc.spark.connect.token".into()), token.into())`.
    See the [Configuration Reference](configuration.md).
