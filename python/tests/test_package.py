# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the License).

"""Unit tests for the adbc-driver-spark package surface.

These do not require a Spark Connect server or the native library: they verify
the public API, option names, and the wiring of ``connect()`` by stubbing out
the native ``AdbcDatabase``/``AdbcConnection`` objects.
"""

from unittest import mock

import pytest


def test_version_is_a_string():
    import adbc_driver_spark

    assert isinstance(adbc_driver_spark.__version__, str)
    assert adbc_driver_spark.__version__.count(".") >= 2


def test_public_exports():
    import adbc_driver_spark

    for name in ("connect", "DatabaseOptions"):
        assert hasattr(adbc_driver_spark, name)


def test_database_option_names_match_driver():
    # These must match the native Go driver's option keys exactly (see
    # driver/spark/driver.go), otherwise the loaded library silently ignores
    # them.
    import adbc_driver_spark as a

    assert a.DatabaseOptions.TOKEN.value == "adbc.spark.token"
    assert a.DatabaseOptions.TLS_ENABLED.value == "adbc.spark.tls.enabled"
    assert a.DatabaseOptions.USER_ID.value == "adbc.spark.user_id"
    assert a.DatabaseOptions.USER_AGENT.value == "adbc.spark.user_agent"
    assert a.DatabaseOptions.SESSION_ID.value == "adbc.spark.session_id"


def test_default_uri():
    import adbc_driver_spark

    assert adbc_driver_spark.DEFAULT_URI == "sc://localhost:15002"


def test_driver_path_returns_string():
    import adbc_driver_spark

    # In a source checkout with no bundled library, this falls back to the bare
    # driver name. Either way it must be a non-empty string.
    adbc_driver_spark._driver_path.cache_clear()
    path = adbc_driver_spark._driver_path()
    assert isinstance(path, str) and path


def test_connect_passes_driver_and_uri():
    import adbc_driver_manager

    import adbc_driver_spark

    with mock.patch.object(adbc_driver_manager, "AdbcDatabase") as fake_db:
        adbc_driver_spark.connect("sc://example:15002")
    _, kwargs = fake_db.call_args
    assert kwargs["uri"] == "sc://example:15002"
    assert "driver" in kwargs
    # The shared library exports the standard AdbcDriverInit symbol, so the
    # driver manager resolves the entrypoint by default (none is passed).
    assert "entrypoint" not in kwargs


def test_connect_merges_db_kwargs():
    import adbc_driver_manager

    import adbc_driver_spark as a

    with mock.patch.object(adbc_driver_manager, "AdbcDatabase") as fake_db:
        a.connect(
            "sc://h:1",
            db_kwargs={a.DatabaseOptions.USER_AGENT.value: "x/1"},
        )
    _, kwargs = fake_db.call_args
    assert kwargs[a.DatabaseOptions.USER_AGENT.value] == "x/1"


def test_dbapi_module_globals():
    import adbc_driver_spark.dbapi as dbapi

    assert dbapi.paramstyle == "qmark"
    assert dbapi.apilevel == "2.0"
    assert dbapi.threadsafety in (0, 1, 2, 3)
    for exc in ("Error", "DatabaseError", "ProgrammingError", "NotSupportedError"):
        assert hasattr(dbapi, exc)


def test_dbapi_connect_token_implies_ssl():
    import adbc_driver_spark
    import adbc_driver_spark.dbapi as dbapi

    captured = {}

    def fake_lowlevel_connect(uri, db_kwargs=None):
        captured.update(db_kwargs or {})
        raise RuntimeError("stop after capturing db_kwargs")

    with mock.patch.object(adbc_driver_spark, "connect", fake_lowlevel_connect):
        with pytest.raises(RuntimeError):
            dbapi.connect("sc://h:443", token="jwt")

    assert captured[adbc_driver_spark.DatabaseOptions.TOKEN.value] == "jwt"
    assert captured[adbc_driver_spark.DatabaseOptions.TLS_ENABLED.value] == "true"


def test_dbapi_connect_use_ssl_false():
    import adbc_driver_spark
    import adbc_driver_spark.dbapi as dbapi

    captured = {}

    def fake_lowlevel_connect(uri, db_kwargs=None):
        captured.update(db_kwargs or {})
        raise RuntimeError("stop")

    with mock.patch.object(adbc_driver_spark, "connect", fake_lowlevel_connect):
        with pytest.raises(RuntimeError):
            dbapi.connect("sc://h:15002", use_ssl=False)

    assert captured[adbc_driver_spark.DatabaseOptions.TLS_ENABLED.value] == "false"
