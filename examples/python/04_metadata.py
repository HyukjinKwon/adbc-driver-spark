# SPDX-License-Identifier: Apache-2.0
"""Catalog metadata: list catalogs, schemas, tables, columns, and a table schema.

The DBAPI ``Connection`` exposes the standard ADBC metadata helpers. They return
either a pyarrow object (RecordBatchReader / Schema) or a plain Python list,
following the ADBC standard layout. This example walks the catalog hierarchy and
also fetches the Arrow schema of a single table.

Prerequisites
-------------
- A Spark Connect server at sc://localhost:15002.
- pip install adbc-driver-spark pyarrow

Run
---
    python 04_metadata.py

The example creates a temporary view so it has something to introspect even on
an empty server. Adjust CATALOG / SCHEMA / TABLE below to point at your data.
"""

import adbc_driver_spark.dbapi as dbapi

CATALOG = "spark_catalog"
SCHEMA = "default"
TABLE = "example_metadata_view"

with dbapi.connect("sc://localhost:15002") as conn:
    # Create a temporary view so this example is self-contained.
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE OR REPLACE TEMPORARY VIEW {TABLE} AS "
            "SELECT id, CAST(id AS STRING) AS label FROM range(3)"
        )

    # adbc_get_table_types() returns the table types Spark exposes.
    print("table types:", conn.adbc_get_table_types())

    # adbc_get_table_schema() returns the pyarrow.Schema of one table. Temporary
    # views live in the session, so look them up without a catalog/schema.
    schema = conn.adbc_get_table_schema(TABLE)
    print(f"\nschema of {TABLE}:")
    for field in schema:
        print(f"  {field.name}: {field.type}")

    # adbc_get_objects() returns the standard ADBC nested structure as a
    # pyarrow.RecordBatchReader. depth controls how deep the traversal goes:
    # 'catalogs', 'db_schemas', 'tables', or 'all' (catalogs through columns).
    reader = conn.adbc_get_objects(depth="all", catalog_filter=CATALOG)
    objects = reader.read_all()

    print("\ncatalogs / schemas / tables / columns:")
    # The outer level is one row per catalog: columns catalog_name and
    # catalog_db_schemas (a list of schema structs).
    for batch in objects.to_pylist():
        catalog_name = batch["catalog_name"]
        for db_schema in batch["catalog_db_schemas"] or []:
            schema_name = db_schema["db_schema_name"]
            for tbl in db_schema["db_schema_tables"] or []:
                col_names = [c["column_name"] for c in tbl["table_columns"] or []]
                print(
                    f"  {catalog_name}.{schema_name}.{tbl['table_name']} "
                    f"({tbl['table_type']}) columns={col_names}"
                )
