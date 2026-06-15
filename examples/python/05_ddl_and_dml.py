# SPDX-License-Identifier: Apache-2.0
"""DDL and DML: create a table, insert rows, then read them back.

Spark Connect runs every statement in autocommit mode (there are no
multi-statement transactions), so CREATE / INSERT take effect immediately. Use
``cur.execute`` for statements that do not return rows just as you would for a
SELECT; the driver runs them for their side effects.

This example uses a managed table in the session-local ``default`` database and
drops it at the end so it can be re-run cleanly.

Prerequisites
-------------
- A Spark Connect server at sc://localhost:15002 with write access to a
  catalog/database (the built-in spark_catalog.default works for local servers).
- pip install adbc-driver-spark

Run
---
    python 05_ddl_and_dml.py
"""

import adbc_driver_spark.dbapi as dbapi

TABLE = "example_ddl_dml"

with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        # DDL: start from a clean slate, then create the table.
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(
            f"""
            CREATE TABLE {TABLE} (
                id   INT,
                name STRING
            ) USING parquet
            """
        )
        print(f"created table {TABLE}")

        # DML: a literal multi-row insert.
        cur.execute(
            f"INSERT INTO {TABLE} VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')"
        )

        # DML: a parameterized insert. Positional ? placeholders bind one row
        # of typed values (the driver binds a single parameter row per call).
        cur.execute(f"INSERT INTO {TABLE} VALUES (?, ?)", (4, "dave"))
        print("inserted rows")

        # Read it back, ordered for stable output.
        cur.execute(f"SELECT id, name FROM {TABLE} ORDER BY id")
        print("contents:")
        for row in cur.fetchall():
            print(" ", row)

        # Clean up so the example is idempotent.
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        print(f"dropped table {TABLE}")
