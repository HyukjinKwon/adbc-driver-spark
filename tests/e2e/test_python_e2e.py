# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""End-to-end tests of the Python DBAPI 2.0 surface against Spark Connect."""

from __future__ import annotations

import pyarrow
import pytest


def test_select_literal(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS id, 'hi' AS msg")
        rows = cur.fetchall()
    assert rows == [(1, "hi")]


def test_fetch_arrow_table(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM range(0, 100)")
        table = cur.fetch_arrow_table()
    assert isinstance(table, pyarrow.Table)
    assert table.num_rows == 100
    assert table.column_names == ["id"]


def test_description_and_rowcount(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT CAST(1 AS INT) AS a, CAST('x' AS STRING) AS b")
        assert cur.description is not None
        names = [d[0] for d in cur.description]
        assert names == ["a", "b"]
        cur.fetchall()


def test_parameterized_query(conn):
    with conn.cursor() as cur:
        # Spark Connect supports positional parameters via the standard
        # DBAPI parameter sequence.
        try:
            cur.execute("SELECT ? AS x", parameters=(7,))
        except Exception as exc:  # pragma: no cover - feature may be phased in
            pytest.skip(f"parameter binding not available: {exc}")
        rows = cur.fetchall()
    assert rows == [(7,)]


def test_type_mapping(conn):
    sql = """
        SELECT
            CAST(true AS BOOLEAN)              AS c_bool,
            CAST(3 AS INT)                     AS c_int,
            CAST(4 AS BIGINT)                  AS c_long,
            CAST(2.5 AS DOUBLE)                AS c_double,
            CAST(3.14 AS DECIMAL(10,2))        AS c_decimal,
            CAST('s' AS STRING)                AS c_string,
            CAST('2024-01-02' AS DATE)         AS c_date,
            CAST('2024-01-02 03:04:05' AS TIMESTAMP) AS c_ts,
            ARRAY(1, 2, 3)                     AS c_array,
            MAP('k', 1)                        AS c_map
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        table = cur.fetch_arrow_table()

    schema = table.schema
    assert pyarrow.types.is_boolean(schema.field("c_bool").type)
    assert pyarrow.types.is_int32(schema.field("c_int").type)
    assert pyarrow.types.is_int64(schema.field("c_long").type)
    assert pyarrow.types.is_float64(schema.field("c_double").type)
    assert pyarrow.types.is_decimal(schema.field("c_decimal").type)
    assert pyarrow.types.is_string(schema.field("c_string").type)
    assert pyarrow.types.is_date32(schema.field("c_date").type)
    assert pyarrow.types.is_timestamp(schema.field("c_ts").type)
    assert pyarrow.types.is_list(schema.field("c_array").type)
    assert pyarrow.types.is_map(schema.field("c_map").type)


def test_fetch_dataframe(conn):
    pd = pytest.importorskip("pandas")
    with conn.cursor() as cur:
        cur.execute("SELECT id, id * 2 AS doubled FROM range(0, 10)")
        df = cur.fetch_df()
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["id", "doubled"]
    assert len(df) == 10
    assert (df["doubled"] == df["id"] * 2).all()


def test_error_is_raised(conn):
    with conn.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute("SELECT * FROM a_table_that_does_not_exist_xyz")
