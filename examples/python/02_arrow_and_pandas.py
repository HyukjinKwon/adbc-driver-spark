# SPDX-License-Identifier: Apache-2.0
"""Arrow and pandas: fetch results as an Arrow table and as a pandas DataFrame.

The driver speaks the Spark Connect protocol and returns native Apache Arrow
data, so fetching an Arrow table is zero-copy: the bytes the server sent are
handed back without any per-row Python conversion. Converting to pandas reuses
those same Arrow buffers where the dtypes allow it.

Prerequisites
-------------
- A Spark Connect server at sc://localhost:15002.
- pip install adbc-driver-spark pyarrow pandas

Run
---
    python 02_arrow_and_pandas.py
"""

import adbc_driver_spark.dbapi as dbapi

with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id,
                   id * id        AS square,
                   CAST(id AS DOUBLE) / 2 AS half
            FROM range(10)
            """
        )

        # fetch_arrow_table() returns a pyarrow.Table built directly from the
        # Arrow record batches streamed by the server. No row-by-row Python
        # objects are created, which is what makes this efficient (zero-copy).
        table = cur.fetch_arrow_table()
        print("Arrow schema:")
        print(table.schema)
        print(f"rows: {table.num_rows}, columns: {table.num_columns}")

        # A pyarrow.Table is column-oriented. Reading a single column is cheap
        # and shares memory with the table; .to_pylist() is only for display.
        print("square column:", table.column("square").to_pylist())

    # Re-run the query to demonstrate the pandas path on a fresh cursor.
    with conn.cursor() as cur:
        cur.execute("SELECT id, id * id AS square FROM range(10)")

        # fetch_df() converts the Arrow result into a pandas DataFrame. The
        # conversion goes through Arrow, so it is far faster than building the
        # DataFrame from Python tuples.
        df = cur.fetch_df()
        print("\npandas DataFrame:")
        print(df)
        print("\ndtypes:")
        print(df.dtypes)
