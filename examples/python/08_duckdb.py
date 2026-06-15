# SPDX-License-Identifier: Apache-2.0
"""DuckDB: query Spark Connect results in-process with zero-copy Arrow.

Because the driver returns native Apache Arrow, the result of a Spark query can
be handed to DuckDB without copying. This lets you push heavy aggregation to
Spark, then do fast local analytics, joins, or window functions in DuckDB on the
same Arrow buffers. Any Arrow consumer (DuckDB, Polars, pandas, Datafusion)
works the same way.

Prerequisites
-------------
- A Spark Connect server (default sc://localhost:15002, override with the
  SPARK_CONNECT_URI environment variable).
- pip install adbc-driver-spark duckdb pyarrow

Run
---
    python 08_duckdb.py
"""

import os

import duckdb

import adbc_driver_spark.dbapi as dbapi

URI = os.environ.get("SPARK_CONNECT_URI", "sc://localhost:15002")

with dbapi.connect(URI) as conn:
    with conn.cursor() as cur:
        # Aggregate on the Spark side, return Arrow.
        cur.execute("SELECT id, id % 3 AS bucket FROM range(1000)")
        spark_result = cur.fetch_arrow_table()

    # DuckDB can scan the Arrow table directly by referencing the Python
    # variable name in SQL. No data is copied out of the Arrow buffers.
    out = duckdb.sql(
        """
        SELECT bucket, COUNT(*) AS n, SUM(id) AS total
        FROM spark_result
        GROUP BY bucket
        ORDER BY bucket
        """
    ).fetchall()

    print("Per-bucket aggregation computed in DuckDB over Spark's Arrow output:")
    for bucket, n, total in out:
        print(f"  bucket={bucket}  count={n}  sum={total}")
