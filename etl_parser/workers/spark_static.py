"""SparkStaticWorker: PySpark DataFrame chain tracing (design spec section 8.3).

The design spec describes ``SparkStaticWorker`` as sharing the DataFrame tracker with
``PythonWorker`` and handling ``select``, ``withColumn``, ``withColumnRenamed``, ``drop``,
``filter``, ``where``, ``join``, ``groupBy().agg``, ``alias``, ``union``, and
``F.col``/``F.expr``/``F.lit`` expressions, with ``F.expr``/``selectExpr`` strings routed to
sqlglot under the ``spark`` dialect. That behavior lives in
:class:`etl_parser.workers.python.PythonWorker` (see section 8.2), which recognizes PySpark
call patterns as part of one shared tracker. ``SparkStaticWorker`` is the PySpark entry
point onto it: it pins the language instead of detecting it, so a Spark file that never
imports ``pyspark`` (a Databricks notebook or a Glue script with an injected session) is
still reported as ``pyspark``/``spark`` and stamps ``parser="spark_static"`` on its edges.
"""

from etl_parser.workers.python import PythonWorker


class SparkStaticWorker(PythonWorker):
    """A :class:`~etl_parser.workers.python.PythonWorker` pinned to PySpark.

    Attributes:
        language (str): Always ``"pyspark"``, so edges carry ``parser="spark_static"`` and
            jobs carry ``engine="spark"`` without depending on the file's imports.
    """

    language = "pyspark"
