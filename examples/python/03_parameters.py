# SPDX-License-Identifier: Apache-2.0
"""Parameter binding: positional placeholders in prepared statements.

The Spark Connect driver uses the "qmark" parameter style (see
``adbc_driver_spark.dbapi.paramstyle``): parameters are positional ``?``
placeholders, bound in order. Passing parameters to ``cur.execute`` prepares
the statement and binds the values before the server evaluates it, so user
values are sent as typed literals rather than spliced into the SQL text.

Note on named (``:name``) parameters
------------------------------------
Spark Connect SQL parameter binding through ADBC is positional only. There is
no ``:named`` placeholder support in this driver, so the named style is not
shown here as runnable code. If you need name-based readability, keep your
``?`` placeholders in a documented order, or build the values dict yourself and
pass the values as a positional sequence in that same order (see the helper at
the bottom of this file).

Prerequisites
-------------
- A Spark Connect server at sc://localhost:15002.
- pip install adbc-driver-spark

Run
---
    python 03_parameters.py
"""

import adbc_driver_spark.dbapi as dbapi

with dbapi.connect("sc://localhost:15002") as conn:
    with conn.cursor() as cur:
        # Confirm the parameter style this driver expects.
        print("paramstyle:", dbapi.paramstyle)  # -> qmark

        # A single positional parameter. The value 3 is bound to the ? marker.
        cur.execute("SELECT id FROM range(10) WHERE id > ?", (3,))
        print("ids > 3:", [row[0] for row in cur.fetchall()])

        # Multiple positional parameters are bound left to right.
        cur.execute(
            "SELECT ? AS greeting, ? AS answer",
            ("hello", 42),
        )
        print("scalars:", cur.fetchall())

        # Parameters carry their type, so this is safe against SQL injection:
        # the string below is treated as a value, never as SQL.
        cur.execute("SELECT ? AS note", ("Robert'); DROP TABLE students;--",))
        print("literal value:", cur.fetchall())


# Optional readability helper: emulate named parameters by mapping names to
# their positions yourself, then handing execute() a positional tuple. This
# stays entirely within the supported positional (qmark) parameter style.
def execute_named(cur, sql_with_names, params):
    """Run SQL written with {name} fields using a params dict.

    Each {name} is rewritten to a positional ? in first-seen order, and the
    matching values are passed positionally. This is a convenience wrapper,
    not a driver feature.
    """
    order = []

    def replace(match):
        order.append(match.group(1))
        return "?"

    import re

    rewritten = re.sub(r"\{(\w+)\}", replace, sql_with_names)
    values = tuple(params[name] for name in order)
    cur.execute(rewritten, values)


if __name__ == "__main__":
    with dbapi.connect("sc://localhost:15002") as conn:
        with conn.cursor() as cur:
            execute_named(
                cur,
                "SELECT id FROM range(100) WHERE id BETWEEN {lo} AND {hi}",
                {"lo": 10, "hi": 13},
            )
            print("named-style range:", [row[0] for row in cur.fetchall()])
