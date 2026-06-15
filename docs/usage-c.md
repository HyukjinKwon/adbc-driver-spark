<!-- SPDX-License-Identifier: Apache-2.0 -->
# Using from C and C++

C and C++ programs load `libadbc_driver_spark` through the ADBC driver manager
and drive it with the plain ADBC C API declared in `adbc.h`. Against
**Apache Spark Connect**, the library exports the standard `AdbcDriverInit`
entrypoint, so the driver manager `dlopen()`s the shared library and resolves
that symbol without you naming an `entrypoint` option.

## Prerequisites

- The shared library `libadbc_driver_spark.{so,dylib,dll}`, either downloaded
  from [Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases) or
  built from source (see [Installation](installation.md)).
- The ADBC driver manager (`libadbc_driver_manager`) and `adbc.h`. The header
  shipped with this project lives at `c/arrow-adbc/adbc.h`.
- A running Spark Connect server reachable at the `sc://` URI (default
  `sc://localhost:15002`).

## The example program

The runnable example lives at
[`examples/c/quickstart.c`](https://github.com/HyukjinKwon/adbc-driver-spark/blob/main/examples/c/quickstart.c).
It creates a database handle, points the driver manager at the Spark Connect
shared library through the `driver` option, sets the `uri`, opens a connection,
runs a query, and reads the Arrow result stream. To stay free of an Arrow C++ or
nanoarrow dependency it counts rows and batches; consume the returned
`ArrowArrayStream` with [nanoarrow](https://github.com/apache/arrow-nanoarrow)
or the Arrow C data interface to inspect individual columns.

```c
/* SPDX-License-Identifier: Apache-2.0 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <adbc.h>

/* Print the ADBC error (if any), release it, and return the status so callers
 * can `return check(...)` directly. */
static AdbcStatusCode check(const char* what, AdbcStatusCode status,
                            struct AdbcError* error) {
  if (status != ADBC_STATUS_OK) {
    fprintf(stderr, "%s failed (status %d): %s\n", what, (int)status,
            error->message ? error->message : "(no message)");
    if (error->release) {
      error->release(error);
    }
  }
  return status;
}

int main(void) {
  const char* driver_path = getenv("SPARK_DRIVER");
  const char* uri = getenv("SPARK_REMOTE");
  if (!driver_path) {
    fprintf(stderr, "set SPARK_DRIVER to the libadbc_driver_spark path\n");
    return EXIT_FAILURE;
  }
  if (!uri) {
    uri = "sc://localhost:15002";
  }

  /* Every Adbc* call takes an AdbcError that must be zero-initialized. */
  struct AdbcError error = {0};
  struct AdbcDatabase database = {0};
  struct AdbcConnection connection = {0};
  struct AdbcStatement statement = {0};
  struct ArrowArrayStream stream = {0};
  int rc = EXIT_FAILURE;

  /* 1. Create the database handle and configure it. Setting "driver" tells the
   *    driver manager which shared library to load; "uri" is the Spark Connect
   *    endpoint. */
  if (check("AdbcDatabaseNew", AdbcDatabaseNew(&database, &error), &error) != ADBC_STATUS_OK) {
    return EXIT_FAILURE;
  }
  if (check("set driver", AdbcDatabaseSetOption(&database, "driver", driver_path, &error),
            &error) != ADBC_STATUS_OK) {
    goto release_db;
  }
  if (check("set uri", AdbcDatabaseSetOption(&database, "uri", uri, &error), &error) !=
      ADBC_STATUS_OK) {
    goto release_db;
  }
  if (check("AdbcDatabaseInit", AdbcDatabaseInit(&database, &error), &error) !=
      ADBC_STATUS_OK) {
    goto release_db;
  }

  /* 2. Create and initialize a connection (one Spark Connect session). */
  if (check("AdbcConnectionNew", AdbcConnectionNew(&connection, &error), &error) !=
      ADBC_STATUS_OK) {
    goto release_db;
  }
  if (check("AdbcConnectionInit", AdbcConnectionInit(&connection, &database, &error),
            &error) != ADBC_STATUS_OK) {
    goto release_conn;
  }

  /* 3. Create a statement, set the SQL, and execute it. */
  if (check("AdbcStatementNew", AdbcStatementNew(&connection, &statement, &error),
            &error) != ADBC_STATUS_OK) {
    goto release_conn;
  }
  if (check("AdbcStatementSetSqlQuery",
            AdbcStatementSetSqlQuery(&statement,
                                     "SELECT id, id * id AS square FROM range(5)", &error),
            &error) != ADBC_STATUS_OK) {
    goto release_stmt;
  }

  int64_t rows_affected = -1;
  if (check("AdbcStatementExecuteQuery",
            AdbcStatementExecuteQuery(&statement, &stream, &rows_affected, &error),
            &error) != ADBC_STATUS_OK) {
    goto release_stmt;
  }

  /* 4. Read the result as an Arrow C stream. The driver returns native Arrow
   *    data; here we just count rows and batches to keep the example free of an
   *    Arrow C++/nanoarrow dependency. */
  struct ArrowSchema schema = {0};
  if (stream.get_schema(&stream, &schema) != 0) {
    fprintf(stderr, "get_schema failed: %s\n", stream.get_last_error(&stream));
    goto release_stream;
  }
  printf("result has %d column(s):", (int)schema.n_children);
  for (int64_t i = 0; i < schema.n_children; i++) {
    printf(" %s", schema.children[i]->name);
  }
  printf("\n");
  if (schema.release) {
    schema.release(&schema);
  }

  int64_t total_rows = 0;
  int batches = 0;
  for (;;) {
    struct ArrowArray array = {0};
    if (stream.get_next(&stream, &array) != 0) {
      fprintf(stderr, "get_next failed: %s\n", stream.get_last_error(&stream));
      goto release_stream;
    }
    /* A released/NULL array marks end of stream. */
    if (array.release == NULL) {
      break;
    }
    total_rows += array.length;
    batches++;
    array.release(&array);
  }
  printf("read %lld row(s) in %d batch(es)\n", (long long)total_rows, batches);
  rc = EXIT_SUCCESS;

release_stream:
  if (stream.release) {
    stream.release(&stream);
  }
release_stmt:
  AdbcStatementRelease(&statement, &error);
release_conn:
  AdbcConnectionRelease(&connection, &error);
release_db:
  AdbcDatabaseRelease(&database, &error);
  return rc;
}
```

## Building

Compile against the vendored ADBC headers and link the driver manager. From the
repository root:

```bash
cc examples/c/quickstart.c \
    -Ic/arrow-adbc \
    -ladbc_driver_manager \
    -o quickstart
```

Add `-I` and `-L` flags pointing at wherever the driver manager headers and
library live on your system if they are not on the default search path.

## Running

Point `SPARK_DRIVER` at the Spark Connect shared library. The copy bundled in
the Python wheel works well; the snippet below resolves it automatically:

```bash
# Resolve the bundled shared library (Linux .so / macOS .dylib / Windows .dll).
export SPARK_DRIVER=$(python -c \
  "import adbc_driver_spark, pathlib; \
   print(next(pathlib.Path(adbc_driver_spark.__file__).parent.glob('libadbc_driver_spark.*')))")

export SPARK_REMOTE=sc://localhost:15002
./quickstart
```

At run time the loader must also be able to find `libadbc_driver_manager` if it
is not on the default search path:

```bash
# Linux
LD_LIBRARY_PATH=/path/to/lib ./quickstart
# macOS
DYLD_LIBRARY_PATH=/path/to/lib ./quickstart
```

!!! tip
    The example passes the library's absolute path as the `driver` option
    (`SPARK_DRIVER`), for example `/opt/adbc/libadbc_driver_spark.so`. Using the
    absolute path instead of a bare name avoids loader-path issues.

!!! note
    Authentication and other settings are plain database options. To use a
    bearer token over TLS, add
    `AdbcDatabaseSetOption(&database, "adbc.spark.connect.token", "...", &error)`
    and `"adbc.spark.connect.use_ssl", "true"`. See the
    [Configuration Reference](configuration.md).
