# SPDX-License-Identifier: Apache-2.0
"""Polars: read a Spark Connect query straight into a Polars DataFrame.

Polars understands the ADBC DBAPI connection object directly through
`pl.read_database`, so results stream from Spark as Arrow and land in Polars
without any row-by-row conversion. This is the same code path you would use with
any other ADBC driver (PostgreSQL, SQLite, Snowflake), which is the point of
ADBC: one interface, many engines.

Prerequisites
-------------
- A Spark Connect server (default sc://localhost:15002, override with the
  SPARK_CONNECT_URI environment variable).
- pip install adbc-driver-spark polars

Run
---
    python 07_polars.py
"""

import os

import polars as pl

import adbc_driver_spark.dbapi as dbapi

URI = os.environ.get("SPARK_CONNECT_URI", "sc://localhost:15002")

with dbapi.connect(URI) as conn:
    # pl.read_database accepts the ADBC connection and uses its Arrow fetch path.
    df = pl.read_database(
        "SELECT id, id * id AS square, CAST(id AS DOUBLE) / 2 AS half FROM range(10)",
        connection=conn,
    )

    print("Polars DataFrame:")
    print(df)

    # Polars expressions run on the Arrow-backed columns.
    summary = df.select(
        pl.col("square").sum().alias("sum_square"),
        pl.col("half").mean().alias("avg_half"),
    )
    print("Aggregated in Polars:")
    print(summary)
