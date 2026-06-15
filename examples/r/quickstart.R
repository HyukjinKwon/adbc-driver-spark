# SPDX-License-Identifier: Apache-2.0

# quickstart.R - connect to a Spark Connect server through the ADBC driver
# manager for R, run a query, and read the result as a data.frame.
#
# The adbcdrivermanager package loads any ADBC driver shared library and exposes
# the standard database/connection/statement lifecycle plus convenience helpers
# like read_adbc(). You point it at the Spark Connect driver shared library with
# adbc_driver(); the manager resolves the default "AdbcDriverInit" entrypoint
# that the driver exports.
#
# Prerequisites:
#   - A Spark Connect server reachable at sc://localhost:15002.
#   - install.packages("adbcdrivermanager")
#   - The Spark Connect driver shared library. The copy bundled in the Python
#     wheel works (libadbc_driver_spark.so / .dylib / adbc_driver_spark.dll).
#
# Run:
#   SPARK_DRIVER=/path/to/libadbc_driver_spark.so \
#   SPARK_REMOTE=sc://localhost:15002 \
#       Rscript quickstart.R

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
