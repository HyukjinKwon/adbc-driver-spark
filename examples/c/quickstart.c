/* SPDX-License-Identifier: Apache-2.0 */

/*
 * quickstart.c - connect to a Spark Connect server through the ADBC driver
 * manager, run a query, and read the Arrow result stream.
 *
 * The driver manager (libadbc_driver_manager) implements the AdbcDatabase /
 * AdbcConnection / AdbcStatement functions declared in adbc.h. You point it at
 * the Spark Connect driver shared library by setting the "driver" option; the
 * manager then dlopen()s it and resolves the default "AdbcDriverInit" symbol,
 * which the Spark driver exports. No "entrypoint" option is needed.
 *
 * Build (adjust paths to your install):
 *
 *   cc quickstart.c \
 *       -I../../c/arrow-adbc \
 *       -ladbc_driver_manager \
 *       -o quickstart
 *
 * The "driver" option below is the path to the Spark Connect shared library,
 * e.g. the one bundled in the Python wheel:
 *
 *   .../site-packages/adbc_driver_spark/libadbc_driver_spark.so   (Linux)
 *   .../site-packages/adbc_driver_spark/libadbc_driver_spark.dylib (macOS)
 *
 * Run:
 *
 *   SPARK_DRIVER=/path/to/libadbc_driver_spark.so \
 *   SPARK_REMOTE=sc://localhost:15002 \
 *       ./quickstart
 */

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
   *    Arrow C++/nanoarrow dependency. Use nanoarrow or the Arrow C data
   *    interface to inspect individual columns. */
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
