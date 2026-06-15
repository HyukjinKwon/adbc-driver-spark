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

"""DBAPI 2.0 (PEP 249) facade for the ADBC Spark Connect driver.

Example
-------
>>> import adbc_driver_spark.dbapi as dbapi
>>> with dbapi.connect("sc://localhost:15002") as conn:
...     with conn.cursor() as cur:
...         cur.execute("SELECT 1 AS id, 'hi' AS msg")
...         cur.fetchall()
[(1, 'hi')]
"""

import typing

import adbc_driver_manager
import adbc_driver_manager.dbapi

import adbc_driver_spark

__all__ = [
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]

# --------------------------------------------------------------------------
# Module globals required by PEP 249

apilevel = adbc_driver_manager.dbapi.apilevel
threadsafety = adbc_driver_manager.dbapi.threadsafety
#: Spark Connect uses positional ``?`` placeholders for parameter binding.
paramstyle = "qmark"

Warning = adbc_driver_manager.dbapi.Warning
Error = adbc_driver_manager.dbapi.Error
InterfaceError = adbc_driver_manager.dbapi.InterfaceError
DatabaseError = adbc_driver_manager.dbapi.DatabaseError
DataError = adbc_driver_manager.dbapi.DataError
OperationalError = adbc_driver_manager.dbapi.OperationalError
IntegrityError = adbc_driver_manager.dbapi.IntegrityError
InternalError = adbc_driver_manager.dbapi.InternalError
ProgrammingError = adbc_driver_manager.dbapi.ProgrammingError
NotSupportedError = adbc_driver_manager.dbapi.NotSupportedError

# --------------------------------------------------------------------------
# Type objects

Date = adbc_driver_manager.dbapi.Date
Time = adbc_driver_manager.dbapi.Time
Timestamp = adbc_driver_manager.dbapi.Timestamp
DateFromTicks = adbc_driver_manager.dbapi.DateFromTicks
TimeFromTicks = adbc_driver_manager.dbapi.TimeFromTicks
TimestampFromTicks = adbc_driver_manager.dbapi.TimestampFromTicks
STRING = adbc_driver_manager.dbapi.STRING
BINARY = adbc_driver_manager.dbapi.BINARY
NUMBER = adbc_driver_manager.dbapi.NUMBER
DATETIME = adbc_driver_manager.dbapi.DATETIME
ROWID = adbc_driver_manager.dbapi.ROWID


def connect(
    uri: str = adbc_driver_spark.DEFAULT_URI,
    db_kwargs: typing.Optional[dict[str, str]] = None,
    conn_kwargs: typing.Optional[dict[str, str]] = None,
    *,
    token: typing.Optional[str] = None,
    use_ssl: typing.Optional[bool] = None,
    autocommit: bool = True,
    **kwargs: typing.Any,
) -> "Connection":
    """Connect to a Spark Connect server and return a DBAPI 2.0 connection.

    Parameters
    ----------
    uri:
        Spark Connect URI (``sc://host:port/;k=v;...``). Defaults to the local
        Spark Connect server.
    db_kwargs:
        Extra database options (see :class:`adbc_driver_spark.DatabaseOptions`).
    conn_kwargs:
        Extra connection options (see
        :class:`adbc_driver_spark.ConnectionOptions`).
    token:
        Convenience shortcut for the bearer token. Equivalent to setting
        :attr:`adbc_driver_spark.DatabaseOptions.TOKEN`. Implies ``use_ssl``.
    use_ssl:
        Convenience shortcut to force TLS on or off.
    autocommit:
        Spark Connect has no multi-statement transactions, so autocommit is on
        by default. Passing ``autocommit=False`` is accepted for API symmetry
        but the driver reports transactions as not implemented.

    Returns
    -------
    Connection
        A DBAPI 2.0 connection. Use it as a context manager to ensure cleanup.
    """
    db_kwargs = dict(db_kwargs or {})
    if token is not None:
        db_kwargs[adbc_driver_spark.DatabaseOptions.TOKEN.value] = token
        # A bearer token over plaintext would leak credentials; default to TLS
        # unless the caller explicitly opts out.
        if use_ssl is None:
            use_ssl = True
    if use_ssl is not None:
        db_kwargs[adbc_driver_spark.DatabaseOptions.TLS_ENABLED.value] = (
            "true" if use_ssl else "false"
        )

    db = None
    conn = None
    try:
        db = adbc_driver_spark.connect(uri, db_kwargs=db_kwargs)
        conn = adbc_driver_manager.AdbcConnection(db, **(conn_kwargs or {}))
        return adbc_driver_manager.dbapi.Connection(
            db, conn, autocommit=autocommit, **kwargs
        )
    except Exception:
        if conn is not None:
            conn.close()
        if db is not None:
            db.close()
        raise


# --------------------------------------------------------------------------
# Re-exported classes

Connection = adbc_driver_manager.dbapi.Connection
Cursor = adbc_driver_manager.dbapi.Cursor
