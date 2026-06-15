# SPDX-License-Identifier: Apache-2.0
"""Streaming: consume a large result as an Arrow RecordBatchReader.

For large results you usually do not want to materialize everything at once.
`cursor.fetch_record_batch()` returns a pyarrow.RecordBatchReader that yields
Arrow record batches as the driver streams them from Spark Connect, so memory
stays bounded to roughly one batch at a time.

Prerequisites
-------------
- A Spark Connect server (default sc://localhost:15002, override with the
  SPARK_CONNECT_URI environment variable).
- pip install adbc-driver-spark pyarrow

Run
---
    python 09_pyarrow_streaming.py
"""

import os

import pyarrow.compute as pc

import adbc_driver_spark.dbapi as dbapi

URI = os.environ.get("SPARK_CONNECT_URI", "sc://localhost:15002")

with dbapi.connect(URI) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id, id * id AS square FROM range(100000)")

        # A RecordBatchReader is a streaming iterator of Arrow batches.
        reader = cur.fetch_record_batch()

        total_rows = 0
        running_sum = 0
        n_batches = 0
        for batch in reader:
            n_batches += 1
            total_rows += batch.num_rows
            # Column access is vectorized; pc.sum runs over the Arrow buffer
            # without building Python row objects.
            running_sum += pc.sum(batch.column("square")).as_py()

        print(f"streamed {total_rows} rows in {n_batches} Arrow batch(es)")
        print(f"sum(square) = {running_sum}")
