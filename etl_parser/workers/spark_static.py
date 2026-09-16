"""PySpark uses the shared frame interpreter and SQL expression worker."""

from etl_parser.workers.python import PythonWorker

SparkStaticWorker = PythonWorker
