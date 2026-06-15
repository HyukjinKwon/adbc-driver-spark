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

"""Pytest fixtures for the Python end-to-end suite.

These tests require both the installed ``adbc_driver_spark`` package (which
bundles the compiled shared library) and a reachable Spark Connect server. They
skip cleanly when either is unavailable, so they are safe to collect anywhere.
"""

from __future__ import annotations

import os

import pytest

SPARK_CONNECT_URI = os.environ.get("SPARK_CONNECT_URI", "")


@pytest.fixture(scope="session")
def spark_uri() -> str:
    if not SPARK_CONNECT_URI:
        pytest.skip("SPARK_CONNECT_URI not set; skipping Python e2e tests")
    return SPARK_CONNECT_URI


@pytest.fixture()
def dbapi(spark_uri):
    """Yield a fresh DBAPI module bound to a live connection helper."""
    adbc_dbapi = pytest.importorskip(
        "adbc_driver_spark.dbapi",
        reason="adbc_driver_spark is not installed (build the wheel first)",
    )
    return adbc_dbapi


@pytest.fixture()
def conn(dbapi, spark_uri):
    connection = dbapi.connect(spark_uri)
    try:
        yield connection
    finally:
        connection.close()
