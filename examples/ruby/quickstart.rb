# SPDX-License-Identifier: Apache-2.0
#
# Quickstart: connect to a Spark Connect server from Ruby through ADBC.
#
# Red ADBC (the "adbc" gem) wraps the ADBC driver manager. It loads the Spark
# Connect driver shared library and resolves its standard AdbcDriverInit
# entrypoint. Point SPARK_DRIVER at libadbc_driver_spark.{so,dylib,dll} and
# SPARK_REMOTE at the server.
#
# Setup:
#   gem install red-adbc   # pulls in the ADBC GLib system library
#
# Run:
#   SPARK_DRIVER=/path/to/libadbc_driver_spark.so \
#   SPARK_REMOTE=sc://localhost:15002 \
#   ruby quickstart.rb

require "adbc"

driver = ENV.fetch("SPARK_DRIVER")
uri = ENV.fetch("SPARK_REMOTE", "sc://localhost:15002")

ADBC::Database.open(driver: driver, uri: uri) do |database|
  database.connect do |connection|
    # query returns an Arrow record batch reader; read it into a table to print.
    # query runs the SQL and returns an Arrow::Table.
    table = connection.query("SELECT id, id * id AS square FROM range(5)")
    puts table
  end
end
