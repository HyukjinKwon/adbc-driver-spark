# SPDX-License-Identifier: Apache-2.0
"""pandas: read a Spark Connect query with pandas.read_sql over ADBC.

pandas 2.0+ accepts an ADBC DBAPI connection in `read_sql` and uses its Arrow
fetch path, which is faster and preserves types better than the legacy
row-by-row path. This is the standard pandas plus ADBC integration and works
identically across every ADBC driver.

Prerequisites
-------------
- A Spark Connect server (default sc://localhost:15002, override with the
  SPARK_CONNECT_URI environment variable).
- pip install adbc-driver-spark "pandas>=2.0" pyarrow

Run
---
    python 10_pandas_read_sql.py
"""

import os

import pandas as pd

import adbc_driver_spark.dbapi as dbapi

URI = os.environ.get("SPARK_CONNECT_URI", "sc://localhost:15002")

with dbapi.connect(URI) as conn:
    # Pass the ADBC connection straight to pandas. pandas detects the ADBC
    # interface and pulls Arrow batches under the hood.
    df = pd.read_sql(
        "SELECT id, CONCAT('row-', CAST(id AS STRING)) AS label FROM range(5)",
        conn,
    )

    print("pandas DataFrame:")
    print(df)
    print("\ndtypes:")
    print(df.dtypes)

    # The fetch_df() cursor helper is an equivalent, driver-native shortcut.
    with conn.cursor() as cur:
        cur.execute("SELECT AVG(id) AS mean_id FROM range(100)")
        print("\nfetch_df() helper:")
        print(cur.fetch_df())
