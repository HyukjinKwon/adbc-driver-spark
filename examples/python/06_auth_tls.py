# SPDX-License-Identifier: Apache-2.0
"""Authentication and TLS: connect with a bearer token over a secure channel.

Remote Spark Connect endpoints (for example Databricks) authenticate with a
bearer token and require TLS. The DBAPI ``connect`` helper exposes ``token=``
and ``use_ssl=`` shortcuts for exactly this. Supplying ``token=`` turns on TLS
automatically (sending a bearer token over plaintext would leak it), so
``use_ssl=True`` is implied; it is shown below for clarity.

This example needs a real authenticated server, so the connecting code is
commented out. Fill in your host and token, then uncomment to run.

Prerequisites
-------------
- A Spark Connect endpoint reachable over TLS, plus a valid bearer token.
- pip install adbc-driver-spark

Run
---
    # export SPARK_HOST=...      e.g. dbc-XXXX.cloud.databricks.com
    # export SPARK_TOKEN=...     e.g. a Databricks personal access token
    python 06_auth_tls.py
"""

import os

import adbc_driver_spark.dbapi as dbapi

HOST = os.environ.get("SPARK_HOST", "spark.example.com")
TOKEN = os.environ.get("SPARK_TOKEN", "<your-bearer-token>")

# A TLS endpoint typically listens on 443. The path and any extra fields can
# also be embedded in the URI; here we keep the URI minimal and pass auth
# options through the dedicated keyword arguments.
URI = f"sc://{HOST}:443"

def run() -> None:
    # token= implies use_ssl=True (sending a bearer token over plaintext would
    # leak it). use_ssl=True is passed here only for clarity.
    with dbapi.connect(URI, token=TOKEN, use_ssl=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_user() AS user")
            print(cur.fetchall())


if TOKEN and TOKEN != "<your-bearer-token>":
    print("Connecting to:", URI)
    run()
else:
    print("Would connect to:", URI)
    print("Set SPARK_HOST and SPARK_TOKEN to connect to a real TLS endpoint.")

# Other equivalent ways to authenticate:
#
# 1. Databricks-style: the token and TLS flag can also live in the URI itself,
#    which is handy when the endpoint string is supplied by configuration.
#
# databricks_uri = (
#     f"sc://{HOST}:443/;token={TOKEN};use_ssl=true"
#     ";x-databricks-cluster-id=<cluster-id>"
# )
# with dbapi.connect(databricks_uri) as conn:
#     ...
#
# 2. Low-level option keys (same effect, no DBAPI shortcuts). Pass them through
#    db_kwargs using the names from adbc_driver_spark.DatabaseOptions.
#
# import adbc_driver_spark
# db_kwargs = {
#     adbc_driver_spark.DatabaseOptions.TOKEN.value: TOKEN,
#     adbc_driver_spark.DatabaseOptions.USE_SSL.value: "true",
# }
# with dbapi.connect(URI, db_kwargs=db_kwargs) as conn:
#     ...
