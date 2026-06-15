---
name: Bug report
about: Report something that is not working as expected
title: "[Bug] "
labels: bug
---

## Description

A clear description of the bug.

## Reproduction

```python
# Minimal code that reproduces the issue. Redact tokens and hostnames.
import adbc_driver_spark.dbapi as dbapi
with dbapi.connect("sc://HOST:PORT") as conn:
    ...
```

## Expected behavior

What you expected to happen.

## Actual behavior

What actually happened, including the full error message and traceback.

## Environment

- adbc-driver-spark version:
- Language and runtime (Python / Go / C / R version):
- Spark version and server type (open source Spark Connect, Databricks, other):
- Operating system and architecture:

## Logs

Any relevant client or Spark Connect server logs.
