// SPDX-License-Identifier: Apache-2.0
//! Quickstart: connect to a Spark Connect server from Rust through ADBC.
//!
//! The ADBC driver manager loads the Spark Connect driver shared library and
//! resolves its standard `AdbcDriverInit` entrypoint. Point `SPARK_DRIVER` at
//! `libadbc_driver_spark.{so,dylib,dll}` and `SPARK_REMOTE` at the server.
//!
//! Run:
//!   SPARK_DRIVER=/path/to/libadbc_driver_spark.so \
//!   SPARK_REMOTE=sc://localhost:15002 \
//!   cargo run

use std::env;
use std::error::Error;

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_manager::ManagedDriver;

fn main() -> Result<(), Box<dyn Error>> {
    let driver_path = env::var("SPARK_DRIVER")
        .map_err(|_| "set SPARK_DRIVER to the libadbc_driver_spark shared library path")?;
    let uri = env::var("SPARK_REMOTE").unwrap_or_else(|_| "sc://localhost:15002".to_string());

    // entrypoint = None uses the default AdbcDriverInit symbol.
    let mut driver = ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::V110)?;
    let mut database = driver.new_database_with_opts([(OptionDatabase::Uri, uri.as_str().into())])?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement.set_sql_query("SELECT id, id * id AS square FROM range(5)")?;

    let reader = statement.execute()?;
    let mut rows = 0usize;
    let mut batches = 0usize;
    for batch in reader {
        let batch = batch?;
        batches += 1;
        rows += batch.num_rows();
    }
    println!("read {rows} row(s) in {batches} batch(es)");
    Ok(())
}
