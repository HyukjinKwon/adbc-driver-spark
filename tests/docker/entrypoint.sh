#!/usr/bin/env bash
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
#
# Starts an Apache Spark Connect server in the foreground so the container
# stays alive and Docker health checks can probe the gRPC port.
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
PORT="${SPARK_CONNECT_PORT:-15002}"
PACKAGE="${SPARK_CONNECT_PACKAGE:-org.apache.spark:spark-connect_2.13:4.0.0}"

echo "[entrypoint] starting Spark Connect server on port ${PORT} (package ${PACKAGE})"

# SPARK_NO_DAEMONIZE=1 makes start-connect-server.sh run in the foreground.
export SPARK_NO_DAEMONIZE=1

exec "${SPARK_HOME}/sbin/start-connect-server.sh" \
    --packages "${PACKAGE}" \
    --conf "spark.connect.grpc.binding.port=${PORT}" \
    --conf "spark.jars.ivy=/tmp/.ivy2" \
    --conf "spark.sql.session.timeZone=UTC" \
    --conf "spark.ui.enabled=false" \
    "$@"
