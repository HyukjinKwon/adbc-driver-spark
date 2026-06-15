# SPDX-License-Identifier: Apache-2.0
"""Quickstart: connect to a Spark Connect server and run a query.

This is the smallest useful program you can write with the ADBC Spark Connect
driver via its DBAPI 2.0 (PEP 249) facade.

Prerequisites
-------------
- A Spark Connect server reachable at sc://localhost:15002 (the default).
  Start one with, for example:

      ./sbin/start-connect-server.sh \
          --packages org.apache.spark:spark-connect_2.13:4.0.0

- The driver installed:  pip install adbc-driver-spark

Run
---
    python 01_quickstart.py
"""

import adbc_driver_spark.dbapi as dbapi

# The default URI is sc://localhost:15002, so connect() with no arguments
# targets a local Spark Connect server. Using the connection as a context
# manager guarantees the session and gRPC channel are closed on exit.
with dbapi.connect("sc://localhost:15002") as conn:
    # A cursor is also a context manager; close it when done to free resources.
    with conn.cursor() as cur:
        # Spark SQL: range(5) produces ids 0..4; we add a derived column.
        cur.execute("SELECT id, id * id AS square FROM range(5)")

        # cur.description follows PEP 249: a tuple per column whose first
        # element is the column name.
        column_names = [col[0] for col in cur.description]
        print("columns:", column_names)

        # fetchall() materializes every row as a list of Python tuples.
        for row in cur.fetchall():
            print(row)
