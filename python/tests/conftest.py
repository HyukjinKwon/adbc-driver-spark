# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the License).

"""Shared pytest fixtures for the adbc-driver-spark Python tests.

Unit tests run anywhere. Integration tests connect to a real Spark Connect
server and are skipped unless ``SPARK_CONNECT_URI`` is set in the environment.
"""

import os

import pytest


@pytest.fixture(scope="session")
def spark_connect_uri() -> str:
    uri = os.environ.get("SPARK_CONNECT_URI")
    if not uri:
        pytest.skip("SPARK_CONNECT_URI not set; skipping integration test")
    return uri


@pytest.fixture
def connection(spark_connect_uri):
    import adbc_driver_spark.dbapi as dbapi

    conn = dbapi.connect(spark_connect_uri)
    try:
        yield conn
    finally:
        conn.close()
