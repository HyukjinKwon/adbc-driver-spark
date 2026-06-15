# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the License).

"""Integration tests against a live Spark Connect server.

Run with a server available::

    SPARK_CONNECT_URI=sc://localhost:15002 pytest python/tests/test_integration.py

They are skipped automatically when ``SPARK_CONNECT_URI`` is unset (see
``conftest.py``).
"""

import pytest

pytestmark = pytest.mark.integration


def test_select_literals(connection):
    with connection.cursor() as cur:
        cur.execute("SELECT 1 AS id, 'hi' AS msg")
        assert cur.fetchall() == [(1, "hi")]


def test_range_count(connection):
    with connection.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM range(1000)")
        (row,) = cur.fetchall()
        assert row[0] == 1000


def test_fetch_arrow_table(connection):
    import pyarrow as pa

    with connection.cursor() as cur:
        cur.execute("SELECT id FROM range(10)")
        table = cur.fetch_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 10
        assert table.column_names == ["id"]


def test_fetch_dataframe(connection):
    pd = pytest.importorskip("pandas")

    with connection.cursor() as cur:
        cur.execute("SELECT id, id * 2 AS doubled FROM range(5)")
        df = cur.fetch_df()
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["id", "doubled"]
        assert df["doubled"].tolist() == [0, 2, 4, 6, 8]


def test_parameter_binding(connection):
    with connection.cursor() as cur:
        cur.execute("SELECT * FROM range(100) WHERE id < ?", (10,))
        rows = cur.fetchall()
        assert len(rows) == 10


def test_metadata_table_types(connection):
    # ADBC metadata helpers live on the Connection, not the Cursor.
    table_types = connection.adbc_get_table_types()
    assert "TABLE" in table_types or "VIEW" in table_types
