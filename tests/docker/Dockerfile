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

# A self-contained Apache Spark Connect server used by the e2e test suite and
# by anyone who wants to run the driver against a real server locally.
#
#   docker build -t adbc-spark-connect tests/docker
#   docker run --rm -p 15002:15002 adbc-spark-connect
#   # then: sc://localhost:15002
#
# The image pre-resolves the Spark Connect server package at build time so the
# container starts fast and works even on a flaky network at run time.
ARG SPARK_VERSION=4.1.2
ARG SCALA_VERSION=2.13

FROM apache/spark:${SPARK_VERSION}

ARG SPARK_VERSION
ARG SCALA_VERSION
ENV SPARK_CONNECT_PACKAGE=org.apache.spark:spark-connect_${SCALA_VERSION}:${SPARK_VERSION}
ENV SPARK_CONNECT_PORT=15002
# Keep ivy/coursier caches in a writable location for the non-root `spark` user.
ENV HOME=/tmp
ENV SPARK_NO_DAEMONIZE=1

USER root
RUN set -eux; \
    mkdir -p /tmp/.ivy2 /tmp/.m2 /opt/spark/logs; \
    chown -R spark:spark /tmp/.ivy2 /tmp/.m2 /opt/spark/logs
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

USER spark

# Warm the ivy cache with the Spark Connect server jar so runtime startup does
# not depend on Maven Central being reachable. Best-effort: never fails the build.
RUN /opt/spark/bin/spark-submit \
        --packages "${SPARK_CONNECT_PACKAGE}" \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --class org.apache.spark.deploy.SparkSubmit \
        --version 2>/dev/null || true

EXPOSE 15002
HEALTHCHECK --interval=10s --timeout=5s --start-period=90s --retries=30 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/${SPARK_CONNECT_PORT:-15002}' || exit 1

ENTRYPOINT ["/opt/entrypoint.sh"]
