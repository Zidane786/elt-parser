"""SparkStaticWorker: PySpark DataFrame chain tracing (design spec section 8.3).

The design spec describes ``SparkStaticWorker`` as sharing the DataFrame tracker with
``PythonWorker`` and handling ``select``, ``withColumn``, ``withColumnRenamed``, ``drop``,
``filter``, ``where``, ``join``, ``groupBy().agg``, ``alias``, ``union``, and
``F.col``/``F.expr``/``F.lit`` expressions, with ``F.expr``/``selectExpr`` strings routed to
sqlglot under the ``spark`` dialect. In this implementation that behavior lives entirely in
:class:`etl_parser.workers.python.PythonWorker` (see section 8.2), which already recognizes
PySpark call patterns and sets ``engine="spark"`` when ``pyspark`` appears in the source text.
``SparkStaticWorker`` is therefore a re-export: an alias to ``PythonWorker`` so callers that
expect a distinct PySpark entry point get the same analyzer.
"""

from etl_parser.workers.python import PythonWorker

SparkStaticWorker = PythonWorker
