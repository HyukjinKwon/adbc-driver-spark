# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Low-level ADBC bindings for the Spark Connect driver.

This package wraps the native ``libadbc_driver_spark`` shared library (built
from Go) and exposes it through the standard :mod:`adbc_driver_manager` API.
For a PEP 249 (DBAPI 2.0) interface, use :mod:`adbc_driver_spark.dbapi`.

Example
-------
>>> import adbc_driver_spark
>>> db = adbc_driver_spark.connect("sc://localhost:15002")
>>> db.close()
"""

import enum
import functools
import typing

import adbc_driver_manager

from ._version import __version__  # noqa: F401

__all__ = [
    "DatabaseOptions",
    "__version__",
    "connect",
]

#: The Spark Connect default endpoint, used when no URI is supplied.
DEFAULT_URI = "sc://localhost:15002"


class DatabaseOptions(enum.Enum):
    """Database-level options recognized by the Spark Connect driver.

    Pass these (by ``.value``) inside ``db_kwargs``. Most can also be expressed
    inline in the ``sc://`` connection string, for example
    ``sc://host:443/;token=...;user_id=...``.

    The option names mirror the native Go driver's keys exactly, so anything you
    set here is honored by the loaded shared library.
    """

    #: Bearer token used for authentication (sent as ``Authorization: Bearer``).
    #: Equivalent to the ``token=`` field of the connection string, and accepted
    #: as the standard ``password`` option too.
    TOKEN = "adbc.spark.token"
    #: Force TLS for the gRPC channel on/off, ``"true"`` or ``"false"``,
    #: overriding whatever the connection string implies.
    TLS_ENABLED = "adbc.spark.tls.enabled"
    #: The Spark user id associated with the remote session. Accepted as the
    #: standard ``username`` option too.
    USER_ID = "adbc.spark.user_id"
    #: The user-agent advertised to the server. Defaults to
    #: ``adbc-driver-spark/<version>``.
    USER_AGENT = "adbc.spark.user_agent"
    #: Pin the client to a specific server-side Spark Connect session id. When
    #: omitted the driver creates a fresh session.
    SESSION_ID = "adbc.spark.session_id"


def connect(
    uri: str = DEFAULT_URI,
    db_kwargs: typing.Optional[dict[str, str]] = None,
) -> adbc_driver_manager.AdbcDatabase:
    """Create a low level ADBC database handle for a Spark Connect server.

    Parameters
    ----------
    uri:
        A Spark Connect URI of the form ``sc://host:port/;k=v;...``. Defaults
        to :data:`DEFAULT_URI` (the local Spark Connect server).
    db_kwargs:
        Extra database options. Keys are ADBC option names; see
        :class:`DatabaseOptions` for the Spark-specific ones.

    Returns
    -------
    adbc_driver_manager.AdbcDatabase
        A database handle. Close it (or use it as a context manager) when done.
    """
    kwargs: dict[str, str] = {"uri": uri}
    if db_kwargs:
        kwargs.update(db_kwargs)
    # The shared library exports the standard ``AdbcDriverInit`` symbol (see the
    # Go ``c/init.go`` alias), so no explicit entrypoint is needed; the ADBC
    # driver manager resolves it by default. The driver-specific
    # ``AdbcDriverSparkInit`` symbol is also exported for callers that name it.
    return adbc_driver_manager.AdbcDatabase(
        driver=_driver_path(),
        **kwargs,
    )


@functools.lru_cache
def _driver_path() -> str:
    """Locate the bundled ``libadbc_driver_spark`` shared library.

    Search order: the wheel-bundled copy inside this package, then the active
    environment's ``lib``/``bin`` directories (covers conda and source builds),
    then fall back to the bare driver name so the ADBC driver manager can
    resolve it from ``(DY)LD_LIBRARY_PATH`` / ``PATH``.
    """
    import pathlib
    import sys
    from importlib import resources

    driver = "adbc_driver_spark"

    # 1. Wheel-bundled shared library next to this module. cibuildwheel ships
    #    the library named "lib<driver>.so" on every platform for consistency,
    #    but a local "go build" may produce a platform-native name, so we look
    #    for all of them.
    package_root = resources.files(driver)
    for filename in (
        f"lib{driver}.so",
        f"lib{driver}.dylib",
        f"{driver}.dll",
        f"lib{driver}.dll",
    ):
        candidate = package_root.joinpath(filename)
        if candidate.is_file():
            return str(candidate)

    # 2. Environment lib/bin directories (conda, "make install").
    prefix = pathlib.Path(sys.prefix)
    for filename in (f"lib{driver}.so", f"lib{driver}.dylib"):
        candidate = prefix / "lib" / filename
        if candidate.is_file():
            return str(candidate)
    candidate = prefix / "bin" / f"{driver}.dll"
    if candidate.is_file():
        return str(candidate)

    # 3. Let the driver manager search the loader path.
    return driver
