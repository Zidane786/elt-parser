"""Airflow schedules and task order from AST; DAG modules are never imported.

Implements design section 8.4. A DAG file is parsed statically with ``ast`` and walked
once, in source order, by ``_DagVisitor``: ``DAG(...)`` constructors, ``with DAG(...)``
blocks and ``@dag``-decorated functions become ``Schedule`` entries (one per task plus
one for the DAG itself); operator instantiations, ``@task``-decorated calls and
``.partial(...).expand(...)`` mappings become tasks mapped to jobs (delegating SQL
operators to ``SqlWorker`` and Python callables to ``PythonWorker``); ``>>``/``<<``/
``chain``/``cross_downstream``/``set_upstream``/``set_downstream``/TaskFlow arguments
become ``declared_upstream``/``declared_downstream``.

Tasks bind to a DAG by their ``dag=`` keyword first, then the enclosing ``with`` block or
``@dag`` function, then the most recently constructed module-level DAG. Variables are
bound at assignment time, so ``t = one; t >> two; t = three; two >> t`` reads as written.
``for`` loops and comprehensions over literal iterables are unrolled; anything
data-dependent (unknown iterables, ``.expand()``, non-cron schedule text) becomes an
``Unresolved`` item of kind ``dynamic_schedule`` instead of a guess. ``TaskGroup`` blocks
prefix their members' task ids and a group variable stands for all of its members in
dependency expressions.

Entry points: ``AirflowWorker.analyze_source`` (a parsed ``SourceFile``) and
``AirflowWorker.analyze_file`` (a path, wraps it in a ``SourceFile``).
"""

from __future__ import annotations

import ast
import re
import shlex
from pathlib import Path

from etl_parser.models import Job, Schedule, Unresolved, WorkerResult
from etl_parser.scanner.strings import Folded, fold_string
from etl_parser.workers.base import CRON_PRESETS, normalize_cron, repo_relative, sql_line_offset
from etl_parser.workers.sql import SqlWorker

_SQL_OPERATORS = {"AthenaOperator", "PostgresOperator", "TrinoOperator", "SQLExecuteQueryOperator"}
_SQL_DIALECTS = {
    "AthenaOperator": "trino",
    "TrinoOperator": "trino",
    "PostgresOperator": "postgres",
}
_MAX_UNROLL = 200


class AirflowWorker:
    """Parses Airflow DAG modules statically into ``Schedule``, task, and job edges.

    Attributes:
        index: ``ScanIndex`` used to resolve script paths (``BashOperator``/
            ``SparkSubmitOperator``/``.sql`` operator arguments) and imported callables
            (``PythonOperator``) to the ``SourceFile`` that defines their job.
        sql: ``SqlWorker`` used to analyze SQL operator queries (``AthenaOperator``,
            ``PostgresOperator``, ``TrinoOperator``, ``SQLExecuteQueryOperator``).
        bindings: Caller-supplied values folded into every string resolution, for example
            ``connection:<conn_id>`` dialect hints used by ``SQLExecuteQueryOperator``.
    """

    def __init__(self, index, sql_worker=None, *, bindings=None):
        """Create a worker for one repo scan.

        Args:
            index: ``ScanIndex`` for resolving scripts and Python imports.
            sql_worker: ``SqlWorker`` to reuse for SQL operators, or ``None`` to create a
                fresh one with no schema provider.
            bindings: Extra name/value bindings folded alongside module-level constants,
                such as ``connection:<conn_id>`` dialect hints.
        """
        self.index = index
        self.sql = sql_worker or SqlWorker()
        self.bindings = bindings or {}

    def analyze_source(self, source):
        """Parse one DAG module's AST into schedules, tasks, jobs, and task dependencies.

        The module is never imported or executed; see the module docstring for the
        binding rules. This method never raises on user code: a syntax error yields a
        single ``unsupported_syntax`` item, and any unexpected failure while walking the
        module is recorded the same way while everything collected so far is kept.

        Args:
            source: The DAG file as a ``SourceFile`` (path, text, and derived job id).

        Returns:
            A ``WorkerResult`` whose ``schedules`` holds one entry per task plus one for
            each DAG, ``task_jobs`` maps each task schedule id to its resolved job id (or
            ``None``), and ``jobs``/``column_edges``/``table_edges``/``join_conditions``/
            ``unresolved`` include everything produced by any SQL or Python job a task
            resolved to.
        """
        result = WorkerResult()
        try:
            tree = ast.parse(source.text)
        except (SyntaxError, ValueError) as exc:
            result.unresolved.append(
                Unresolved(
                    kind="unsupported_syntax",
                    source_file=source.path,
                    line=getattr(exc, "lineno", None),
                    reason=str(exc),
                )
            )
            return result
        visitor = _DagVisitor(self, source, tree, result)
        try:
            visitor.run()
        except Exception as exc:  # Never raise on user code; keep what was collected.
            result.unresolved.append(
                Unresolved(
                    kind="unsupported_syntax",
                    source_file=source.path,
                    reason=f"DAG analysis aborted: {type(exc).__name__}: {exc}"[:500],
                )
            )
        return result

    def analyze_file(self, path):
        """Analyze a DAG file given as a filesystem path.

        Args:
            path: Path to the ``.py`` DAG file. A path outside ``self.index.root`` is
                recorded under its absolute path instead of raising.

        Returns:
            The ``WorkerResult`` from ``analyze_source`` for this file's contents.
        """
        from etl_parser.scanner.repo import SourceFile

        rel = repo_relative(Path(path), self.index.root).replace("\\", "/")
        return self.analyze_source(SourceFile(rel, Path(path).read_text(), ".py"))


class _DagVisitor:
    """Single-pass, binding-aware walk of one DAG module (see module docstring).

    Attributes:
        worker: The owning ``AirflowWorker`` (index, SQL worker, bindings).
        source: The ``SourceFile`` being analysed.
        tree: Its parsed module.
        result: The ``WorkerResult`` being filled.
        env: Folded module constants plus loop variables bound during unrolling.
        aliases: ``from x import a as b`` alias -> original name.
        dags: DAG variable name -> its ``Schedule`` (for ``dag=`` and ``with dag_var:``).
        dag_stack: Enclosing ``with DAG(...)``/``@dag`` contexts, innermost last.
        last_dag: Most recently constructed module-level DAG, the fallback binding.
        groups: Open ``TaskGroup`` contexts as ``(group_id, members)`` pairs.
        symbols: Variable name -> task schedule ids it currently refers to.
        decorated: ``@task``-decorated function name -> its definition.
    """

    def __init__(self, worker: AirflowWorker, source, tree: ast.Module, result: WorkerResult):
        """Prepare the environment (module constants, import aliases) for the walk.

        Args:
            worker: The owning ``AirflowWorker``.
            source: The ``SourceFile`` being analysed.
            tree: Its parsed module.
            result: The ``WorkerResult`` to fill.
        """
        self.worker = worker
        self.source = source
        self.tree = tree
        self.result = result
        self.env: dict = dict(worker.bindings)
        for node in tree.body:
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if isinstance(target, ast.Name) and target.id not in worker.bindings:
                        self.env[target.id] = fold_string(node.value, self.env)
        self.aliases: dict[str, str] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    self.aliases[alias.asname or alias.name] = alias.name
        self.dags: dict[str, Schedule] = {}
        self.dag_stack: list[Schedule] = []
        self.last_dag: Schedule | None = None
        self.groups: list[tuple[str, list[str]]] = []
        self.symbols: dict[str, list[str]] = {}
        self.decorated: dict[str, ast.AST] = {}

    def run(self) -> None:
        """Walk the module body."""
        self.visit(self.tree.body)

    # ----------------------------------------------------------------- helpers
    def name(self, node: ast.AST) -> str:
        """Return an unqualified, alias-resolved name for a call target or reference.

        Args:
            node: The AST expression to name, typically a ``Call.func``.

        Returns:
            The last dotted segment of ``node`` (``"DAG"`` for ``airflow.DAG``), resolved
            through import aliases when it was imported under another name.
        """
        text = ast.unparse(node).split(".")[-1]
        return self.aliases.get(text, text)

    def root_name(self, node: ast.AST) -> str | None:
        """Return the alias-resolved leftmost name of a dotted expression.

        Args:
            node: A ``Name`` or ``Attribute`` chain such as ``task.virtualenv``.

        Returns:
            The resolved root name (``"task"``), or ``None`` for other expressions.
        """
        while isinstance(node, ast.Attribute):
            node = node.value
        return self.aliases.get(node.id, node.id) if isinstance(node, ast.Name) else None

    def value(self, node: ast.AST | None) -> str | None:
        """Fold ``node`` to a plain string only when fully and unambiguously resolved.

        Args:
            node: The AST expression to fold, or ``None``.

        Returns:
            The folded text, or ``None`` when ``node`` is ``None`` or the fold is
            incomplete or relies on an assumed (defaulted) value.
        """
        if node is None:
            return None
        folded = fold_string(node, self.env)
        return folded.text if folded.complete and not folded.assumptions else None

    def issue(self, node: ast.AST, reason: str, kind: str = "dynamic_schedule") -> None:
        """Record an ``Unresolved`` item pointing at ``node``.

        Args:
            node: AST node the issue is about, used for its line and unparsed text.
            reason: Human-readable explanation of what could not be resolved.
            kind: ``Unresolved.kind`` to record.
        """
        try:
            expression = ast.unparse(node)[:500]
        except Exception:  # pragma: no cover - defensive against exotic nodes
            expression = type(node).__name__
        self.result.unresolved.append(
            Unresolved(
                kind=kind,
                source_file=self.source.path,
                line=getattr(node, "lineno", None),
                expression=expression,
                reason=reason,
            )
        )

    def dag_schedule(self, call: ast.Call, fallback: str) -> Schedule:
        """Build and register the ``Schedule`` for a ``DAG(...)`` call or ``@dag`` decorator.

        Reads ``dag_id``, ``schedule``/``schedule_interval`` (normalised to cron via
        ``normalize_cron``), ``catchup``, ``tags``, ``start_date`` (and its timezone
        keyword when given as a call), and ``default_args["owner"]``. Schedule text that
        is neither a preset, a ``timedelta``, nor a valid five-field cron (for example a
        six-field cron or a timetable object) is kept as ``interval_text`` with
        ``cron=None`` and reported as a ``dynamic_schedule`` item.

        Args:
            call: The ``DAG(...)`` call node (synthetic for a bare ``@dag`` decorator).
            fallback: ``dag_id`` to use when it cannot be resolved from ``call``.

        Returns:
            The new ``Schedule``, stored in ``result.schedules`` under
            ``f"airflow.{source.job_id}.{dag_id}"``.
        """
        kw = {k.arg: k.value for k in call.keywords if k.arg}
        dag_id = self.value(kw.get("dag_id") or (call.args[0] if call.args else None)) or fallback
        interval = kw.get("schedule", kw.get("schedule_interval"))
        text = self.value(interval)
        if isinstance(interval, ast.Constant) and interval.value is None:
            text = None
        elif interval is not None and text is None:
            text = ast.unparse(interval)
        cron = normalize_cron(text)
        if (
            text is not None
            and cron is None
            and text.strip().lower() not in CRON_PRESETS
            and "timedelta(" not in text
        ):
            self.issue(interval, "DAG schedule is not a preset or five-field cron")
        ident = f"airflow.{self.source.job_id}.{dag_id}"
        schedule = Schedule(
            id=ident,
            orchestrator="airflow",
            dag_id=dag_id,
            cron=cron,
            interval_text=text,
            source_file=self.source.path,
            line=call.lineno,
        )
        for key in ("catchup", "tags"):
            if key in kw:
                try:
                    setattr(schedule, key, ast.literal_eval(kw[key]))
                except (ValueError, TypeError, SyntaxError):
                    self.issue(kw[key], f"Cannot resolve {key}")
        if "start_date" in kw:
            schedule.start_date = ast.unparse(kw["start_date"])
            if isinstance(kw["start_date"], ast.Call):
                zone = next(
                    (k.value for k in kw["start_date"].keywords if k.arg in {"tz", "tzinfo"}),
                    None,
                )
                schedule.timezone = self.value(zone)
        defaults = kw.get("default_args")
        if isinstance(defaults, ast.Name):
            defaults = next(
                (
                    n.value
                    for n in self.tree.body
                    if isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == defaults.id for t in n.targets)
                ),
                None,
            )
        if isinstance(defaults, ast.Dict):
            schedule.owner = next(
                (
                    self.value(v)
                    for k, v in zip(defaults.keys, defaults.values, strict=True)
                    if isinstance(k, ast.Constant) and k.value == "owner"
                ),
                None,
            )
        self.result.schedules[ident] = schedule
        return schedule

    def current_dag(self, call: ast.Call | None) -> Schedule | None:
        """Resolve the DAG a task call belongs to.

        Args:
            call: The task's call node, whose ``dag=`` keyword wins when it names a known
                DAG variable.

        Returns:
            The ``dag=`` DAG, else the innermost enclosing ``with DAG``/``@dag`` context,
            else the most recently constructed module-level DAG, else ``None``.
        """
        if call is not None:
            dag_kw = next((k.value for k in call.keywords if k.arg == "dag"), None)
            if isinstance(dag_kw, ast.Name) and dag_kw.id in self.dags:
                return self.dags[dag_kw.id]
        if self.dag_stack:
            return self.dag_stack[-1]
        return self.last_dag

    def is_task_decorator(self, decorator: ast.AST) -> bool:
        """Whether ``decorator`` is ``@task``, ``@task(...)`` or a ``@task.<flavour>`` variant.

        Args:
            decorator: One entry of a function's ``decorator_list``.

        Returns:
            ``True`` for ``task``, ``task.python``, ``task.virtualenv``, ``task.branch``,
            ... with or without call arguments.
        """
        func = decorator.func if isinstance(decorator, ast.Call) else decorator
        return self.root_name(func) == "task"

    def is_task_call(self, call: ast.Call) -> bool:
        """Whether a call instantiates a task: an ``*Operator``/``*Sensor``, a ``@task``
        function, or any callable given a ``task_id=`` keyword (custom operators).

        Args:
            call: The call node.

        Returns:
            ``True`` when the call should be registered as a task.
        """
        if isinstance(call.func, ast.Attribute) and call.func.attr in {"partial", "expand"}:
            return False
        callee = self.name(call.func)
        return (
            callee.endswith(("Operator", "Sensor"))
            or callee in self.decorated
            or any(k.arg == "task_id" for k in call.keywords)
        )

    # ------------------------------------------------------------------- tasks
    def task(self, call: ast.Call, dag: Schedule, function: str | None = None, mapped=False):
        """Register one task's ``Schedule`` and resolve its operator to a job.

        Copies ``dag``'s schedule to a task-scoped id ``f"{dag.id}.{group.}{task_id}"``
        (TaskGroup prefixes included), adds the task to every open group, then maps the
        operator per design section 8.4 step 2 through ``resolve_job``. A dynamically
        mapped task (``.partial(...).expand(...)``) is registered with no job and a
        ``dynamic_schedule`` item.

        Args:
            call: The operator instantiation call (or the ``.partial(...)`` call for a
                mapped task).
            dag: The DAG the task belongs to.
            function: Name of the ``@task``-decorated function when this task comes from a
                TaskFlow call rather than an ``Operator(...)`` call.
            mapped: Whether the task is dynamically mapped with ``.expand()``.

        Returns:
            The task's schedule id, or ``None`` when ``task_id`` could not be resolved (an
            ``Unresolved`` item is recorded and no schedule is created).
        """
        kw = {k.arg: k.value for k in call.keywords if k.arg}
        task_id = self.value(kw.get("task_id")) or function
        if not task_id:
            folded = fold_string(kw.get("task_id"), self.env) if "task_id" in kw else None
            self.result.unresolved.append(
                Unresolved(
                    kind="dynamic_schedule",
                    source_file=self.source.path,
                    line=call.lineno,
                    expression=ast.unparse(call)[:500],
                    reason="Task ID is dynamic",
                    partial_text=folded.text if folded else None,
                    symbols=sorted(folded.placeholders) if folded else [],
                )
            )
            return None
        prefix = "".join(f"{group}." for group, _ in self.groups)
        ident = f"{dag.id}.{prefix}{task_id}"
        schedule = dag.model_copy(
            deep=True, update={"id": ident, "task_id": f"{prefix}{task_id}", "line": call.lineno}
        )
        if "owner" in kw:
            schedule.owner = self.value(kw["owner"])
        self.result.schedules[ident] = schedule
        self.result.task_jobs[ident] = None
        for _, members in self.groups:
            members.append(ident)
        if mapped:
            self.issue(call, "Dynamically mapped task (.expand) is resolved at runtime")
            return ident
        self.result.task_jobs[ident] = self.resolve_job(call, kw, ident, schedule, function)
        return ident

    def resolve_job(self, call, kw, ident, schedule, function) -> str | None:
        """Map an operator call to the job it runs (design section 8.4 step 2).

        Args:
            call: The operator instantiation call.
            kw: Its keyword arguments by name.
            ident: The task's schedule id (used as the job id for SQL/TaskFlow tasks).
            schedule: The task's ``Schedule`` (cross-DAG sensors update it in place).
            function: ``@task`` function name for TaskFlow tasks, else ``None``.

        Returns:
            The resolved job id, or ``None`` with an ``Unresolved`` item recorded.
        """
        operator = self.name(call.func)
        if operator in {"BashOperator", "SparkSubmitOperator"}:
            command = self.value(kw.get("bash_command") or kw.get("application"))
            paths: list[str] = []
            if command:
                try:
                    paths = [p for p in shlex.split(command) if p.endswith((".py", ".sql"))]
                except ValueError:
                    paths = []
                paths += re.findall(r"\$\(cat\s+([^\s)]+\.sql)\)", command)
            matches = [m for m in (self.worker.index.script(p) for p in paths) if m]
            if len(matches) == 1:
                return matches[0].job_id
            self.issue(call, "Task script could not be resolved unambiguously", "external_job")
            return None
        if operator in {"PythonOperator", "PythonVirtualenvOperator"} or function:
            return self.resolve_python(call, kw, ident, function)
        if operator in _SQL_OPERATORS:
            return self.resolve_sql(call, kw, ident, operator)
        if operator == "ExternalTaskSensor":
            external_dag = self.value(kw.get("external_dag_id"))
            external_task = self.value(kw.get("external_task_id"))
            if external_dag:
                schedule.declared_upstream = [
                    f"external-dag://{external_dag}/{external_task or ''}"
                ]
            else:
                self.issue(call, "External DAG identifier is dynamic", "external_job")
        elif operator == "TriggerDagRunOperator":
            external_dag = self.value(kw.get("trigger_dag_id"))
            if external_dag:
                schedule.declared_downstream = [f"external-dag://{external_dag}/"]
            else:
                self.issue(call, "Triggered DAG identifier is dynamic", "external_job")
        elif operator in {"GlueJobOperator", "EmrAddStepsOperator"}:
            self.issue(call, "External task/job requires explicit mapping", "external_job")
        return None

    def resolve_python(self, call, kw, ident, function) -> str | None:
        """Resolve a ``PythonOperator`` callable or ``@task`` function to a Python job.

        Args:
            call: The operator/TaskFlow call node.
            kw: Its keyword arguments by name.
            ident: The task's schedule id, used as the job id override.
            function: ``@task`` function name, or ``None`` to read ``python_callable``.

        Returns:
            ``ident`` when the callable was found and analysed by ``PythonWorker``, else
            ``None`` with an ``unresolved_import`` item recorded.
        """
        from etl_parser.workers.python import PythonWorker

        callable_source = self.source
        callable_node = kw.get("python_callable")
        function_name = function or (
            callable_node.id if isinstance(callable_node, ast.Name) else None
        )
        if isinstance(callable_node, ast.Name):
            for imported in ast.walk(self.tree):
                if not isinstance(imported, ast.ImportFrom):
                    continue
                if not any((a.asname or a.name) == callable_node.id for a in imported.names):
                    continue
                module = self.worker.index.resolve_module(
                    imported.module or "", self.source, imported.level
                )
                if module is None:
                    self.issue(
                        call, "Python callable import could not be resolved", "unresolved_import"
                    )
                    return None
                callable_source = module
                function_name = next(
                    a.name for a in imported.names if (a.asname or a.name) == callable_node.id
                )
        if not function_name:
            self.issue(call, "Python callable cannot be identified", "unresolved_import")
            return None
        self.result.extend(
            PythonWorker(
                self.worker.sql, self.worker.index, bindings=self.worker.bindings
            ).analyze_source(callable_source, entry_function=function_name, job_id_override=ident)
        )
        return ident

    def resolve_sql(self, call, kw, ident, operator) -> str | None:
        """Analyse a SQL operator's ``query``/``sql`` argument(s) as a SQL job.

        Accepts one string or a list of strings, each analysed as its own statement
        batch with lines anchored to the string constant (``sql_line_offset``); a value
        ending in ``.sql`` is resolved through ``index.script`` and the file's own text
        and path are analysed instead.

        Args:
            call: The operator call node.
            kw: Its keyword arguments by name.
            ident: The task's schedule id, used as the job id.
            operator: Alias-resolved operator class name, selecting the dialect.

        Returns:
            ``ident`` when at least one SQL item was analysed (a ``Job`` is appended to the
            result), else ``None``.
        """
        dialect = _SQL_DIALECTS.get(operator)
        if operator == "SQLExecuteQueryOperator":
            connection = self.value(kw.get("conn_id"))
            dialect = self.worker.bindings.get(f"connection:{connection}")
        if dialect is None:
            self.issue(
                call,
                "Generic SQL operator requires a connection dialect binding",
                "unsupported_syntax",
            )
            return None
        engine = "athena" if dialect in {"trino", "presto"} else dialect
        sql_node = kw.get("query") or kw.get("sql")
        items = list(sql_node.elts) if isinstance(sql_node, (ast.List, ast.Tuple)) else [sql_node]
        inputs: set[str] = set()
        outputs: set[str] = set()
        analysed = False
        for item in items:
            query = self.value(item)
            if query is None:
                self.issue(item or call, "Operator SQL depends on runtime values", "dynamic_sql")
                continue
            if query.strip().lower().endswith(".sql") and "\n" not in query.strip():
                script = self.worker.index.script(query.strip())
                if script is None:
                    self.issue(
                        item, f"SQL file {query.strip()!r} is not in the scan", "unresolved_import"
                    )
                    continue
                text, source_file, offset = script.text, script.path, 0
            else:
                text, source_file, offset = query, self.source.path, sql_line_offset(item)
            analysis = self.worker.sql.analyze(
                text,
                dialect=dialect,
                engine=engine,
                job_id=ident,
                source_file=source_file,
                line_offset=offset,
            )
            self.result.extend(analysis.result)
            inputs |= analysis.inputs
            outputs |= analysis.outputs
            analysed = True
        if not analysed:
            return None
        self.result.jobs.append(
            Job(
                id=ident,
                name=self.result.schedules[ident].task_id,
                source_file=self.source.path,
                language="sql",
                engine=engine,
                dialect=dialect,
                inputs=sorted(inputs),
                outputs=sorted(outputs),
                schedule_id=ident,
            )
        )
        return ident

    # ------------------------------------------------------------------- walk
    def visit(self, body) -> None:
        """Walk a block of statements in source order, binding names as they are assigned.

        Recurses into ``with``/``if``/``while``/``try``/function bodies, unrolls ``for``
        loops and comprehensions over literal iterables, registers tasks, and applies
        dependency expressions where they appear so a rebound variable refers to whatever
        it named at that point (finding 16).

        Args:
            body: A list of AST statements.
        """
        for node in body:
            if isinstance(node, ast.With):
                self.visit_with(node)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.visit_function(node)
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                target = node.targets[0] if isinstance(node, ast.Assign) else node.target
                self.visit_assign(target, node.value)
            elif isinstance(node, ast.Expr):
                self.visit_expression(node.value)
            elif isinstance(node, ast.For):
                self.visit_loop(node)
            elif isinstance(node, ast.While):
                self.visit(node.body)
                self.visit(node.orelse)
            elif isinstance(node, ast.If):
                self.visit(node.body)
                self.visit(node.orelse)
            elif isinstance(node, ast.Try):
                self.visit(node.body)
                for handler in node.handlers:
                    self.visit(handler.body)
                self.visit(node.orelse)
                self.visit(node.finalbody)
            elif isinstance(node, ast.Return) and node.value is not None:
                self.visit_expression(node.value)

    def visit_with(self, node: ast.With) -> None:
        """Enter ``with DAG(...)``, ``with dag_var``, and ``with TaskGroup(...)`` blocks.

        Args:
            node: The ``with`` statement.
        """
        pushed_dags = 0
        pushed_groups: list[tuple[str, list[str], ast.AST | None]] = []
        for item in node.items:
            expression = item.context_expr
            if isinstance(expression, ast.Call) and self.name(expression.func) == "DAG":
                dag = self.dag_schedule(expression, Path(self.source.path).stem)
                if isinstance(item.optional_vars, ast.Name):
                    self.dags[item.optional_vars.id] = dag
                self.dag_stack.append(dag)
                pushed_dags += 1
            elif isinstance(expression, ast.Name) and expression.id in self.dags:
                self.dag_stack.append(self.dags[expression.id])
                pushed_dags += 1
            elif isinstance(expression, ast.Call) and self.name(expression.func) == "TaskGroup":
                kw = {k.arg: k.value for k in expression.keywords if k.arg}
                group_id = self.value(
                    kw.get("group_id") or (expression.args[0] if expression.args else None)
                )
                if group_id is None:
                    self.issue(expression, "TaskGroup id cannot be resolved statically")
                    group_id = "group"
                members: list[str] = []
                self.groups.append((group_id, members))
                pushed_groups.append((group_id, members, item.optional_vars))
        self.visit(node.body)
        for _group_id, members, variable in reversed(pushed_groups):
            self.groups.pop()
            if isinstance(variable, ast.Name):
                self.symbols[variable.id] = list(members)
        del self.dag_stack[len(self.dag_stack) - pushed_dags :]

    def visit_function(self, node) -> None:
        """Handle a function definition: ``@dag``, ``@task``/``@task.*``, or a plain body.

        Args:
            node: The (async) function definition.
        """
        decorators = [(d.func if isinstance(d, ast.Call) else d, d) for d in node.decorator_list]
        dag_decorator = next((d for f, d in decorators if self.name(f) == "dag"), None)
        if dag_decorator is not None:
            if not isinstance(dag_decorator, ast.Call):
                dag_decorator = ast.copy_location(
                    ast.Call(func=dag_decorator, args=[], keywords=[]), node
                )
            dag = self.dag_schedule(dag_decorator, node.name)
            self.dag_stack.append(dag)
            self.visit(node.body)
            self.dag_stack.pop()
        elif any(self.is_task_decorator(d) for _, d in decorators):
            self.decorated[node.name] = node
        else:
            self.visit(node.body)

    def visit_assign(self, target, value) -> None:
        """Bind an assignment's target at the point it is written.

        Args:
            target: The assignment target (only ``Name`` targets bind symbols).
            value: The assigned expression: a ``DAG(...)`` call, a task call, a
                comprehension of task calls, or an expression naming existing tasks.
        """
        variable = target.id if isinstance(target, ast.Name) else None
        if isinstance(value, ast.Call) and self.name(value.func) == "DAG":
            dag = self.dag_schedule(value, variable or "dag")
            if variable:
                self.dags[variable] = dag
            self.last_dag = dag
            return
        ids = self.register(value)
        if variable is not None:
            if ids:
                self.symbols[variable] = ids
            elif variable in self.symbols:
                # Rebinding to something that is not a task clears the old meaning.
                del self.symbols[variable]

    def visit_expression(self, value) -> None:
        """Handle a bare expression statement: a task call or a dependency expression.

        Args:
            value: The expression node.
        """
        if isinstance(value, ast.BinOp) and isinstance(value.op, (ast.RShift, ast.LShift)):
            self.binop(value)
        elif isinstance(value, ast.Call) and not self.dependency_call(value):
            self.register(value)
        elif isinstance(value, (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.List, ast.Tuple)):
            self.register(value)

    def register(self, value) -> list[str]:
        """Register every task an expression creates and return their schedule ids.

        Handles operator/TaskFlow calls, ``.partial(...).expand(...)`` mappings,
        comprehensions over literal iterables, and list/tuple literals of those. An
        expression that only names existing tasks resolves through ``refs``.

        Args:
            value: The expression to register.

        Returns:
            The schedule ids created (or referenced) by ``value``, in order.
        """
        if isinstance(value, ast.Call):
            mapped = self.mapped_call(value)
            if mapped is not None:
                dag = self.current_dag(mapped)
                return [i for i in [self.task(mapped, dag, mapped=True)] if i] if dag else []
            if self.is_task_call(value):
                dag = self.current_dag(value)
                if dag is None:
                    return []
                function = (
                    self.name(value.func) if self.name(value.func) in self.decorated else None
                )
                ident = self.task(value, dag, function)
                if ident is not None:
                    for argument in value.args:
                        self.connect(self.refs(argument), [ident])
                return [i for i in [ident] if i]
            # A call that creates no task (a helper, ``os.getenv``, ...) binds nothing.
            return []
        if isinstance(value, (ast.ListComp, ast.SetComp, ast.GeneratorExp)):
            return self.unroll_comprehension(value)
        if isinstance(value, (ast.List, ast.Tuple, ast.Set)):
            return [i for element in value.elts for i in self.register(element)]
        return self.refs(value)

    def mapped_call(self, call: ast.Call) -> ast.Call | None:
        """Return the ``.partial(...)`` call of a ``Operator.partial(...).expand(...)`` chain.

        Args:
            call: The call node to inspect.

        Returns:
            The ``.partial(...)`` call carrying ``task_id``, or ``None`` when ``call`` is
            not a dynamically mapped task.
        """
        if not (
            isinstance(call.func, ast.Attribute) and call.func.attr in {"expand", "expand_kwargs"}
        ):
            return None
        inner = call.func.value
        if (
            isinstance(inner, ast.Call)
            and isinstance(inner.func, ast.Attribute)
            and inner.func.attr == "partial"
        ):
            return inner
        return None

    # ---------------------------------------------------------------- unrolling
    def literal_elements(self, node) -> list[ast.expr] | None:
        """Return the elements of a literal iterable, or ``None`` when it is not literal.

        Args:
            node: The iterable expression of a ``for`` loop or comprehension.

        Returns:
            The element expressions of a list/tuple/set display, else ``None``.
        """
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            return list(node.elts)
        return None

    def bind_target(self, target, element) -> dict[str, Folded] | None:
        """Fold one loop element into the environment bindings its target introduces.

        Args:
            target: The loop target (``Name``, or a ``Tuple``/``List`` of targets).
            element: The element expression the target is bound to.

        Returns:
            Name-to-``Folded`` bindings, or ``None`` when the element (or the shape of a
            tuple target) cannot be resolved statically.
        """
        if isinstance(target, ast.Name):
            folded = fold_string(element, self.env)
            return {target.id: folded} if folded.complete and not folded.assumptions else None
        if isinstance(target, (ast.Tuple, ast.List)):
            if not isinstance(element, (ast.Tuple, ast.List)):
                return None
            if len(target.elts) != len(element.elts):
                return None
            bindings: dict[str, Folded] = {}
            for sub_target, sub_element in zip(target.elts, element.elts, strict=True):
                sub = self.bind_target(sub_target, sub_element)
                if sub is None:
                    return None
                bindings.update(sub)
            return bindings
        return None

    def with_bindings(self, bindings: dict[str, Folded], action):
        """Run ``action`` with ``bindings`` applied to the folding environment.

        Args:
            bindings: Loop-variable bindings to apply.
            action: Zero-argument callable run while they are in effect.

        Returns:
            Whatever ``action`` returns.
        """
        previous = {k: self.env[k] for k in bindings if k in self.env}
        self.env.update(bindings)
        try:
            return action()
        finally:
            for key in bindings:
                self.env.pop(key, None)
            self.env.update(previous)

    def visit_loop(self, node: ast.For) -> None:
        """Unroll a ``for`` loop over a literal iterable, or report it as dynamic.

        Args:
            node: The ``for`` statement.
        """
        elements = self.literal_elements(node.iter)
        bindings: list[dict[str, Folded]] | None = []
        if elements is None or len(elements) > _MAX_UNROLL:
            bindings = None
        else:
            for element in elements:
                bound = self.bind_target(node.target, element)
                if bound is None:
                    bindings = None
                    break
                bindings.append(bound)
        if bindings is None:
            self.report_dynamic_body(node)
            return
        for bound in bindings:
            self.with_bindings(bound, lambda: self.visit(node.body))
        self.visit(node.orelse)

    def unroll_comprehension(self, node) -> list[str]:
        """Register the tasks created by a comprehension over a literal iterable.

        Args:
            node: A ``ListComp``/``SetComp``/``GeneratorExp``.

        Returns:
            The schedule ids created, or an empty list when the comprehension cannot be
            unrolled (reported once as ``dynamic_schedule``).
        """
        if len(node.generators) != 1 or node.generators[0].ifs:
            self.report_dynamic_body(node)
            return []
        generator = node.generators[0]
        elements = self.literal_elements(generator.iter)
        if elements is None or len(elements) > _MAX_UNROLL:
            self.report_dynamic_body(node)
            return []
        created: list[str] = []
        for element in elements:
            bound = self.bind_target(generator.target, element)
            if bound is None:
                self.report_dynamic_body(node)
                return created
            created.extend(self.with_bindings(bound, lambda: self.register(node.elt)))
        return created

    def report_dynamic_body(self, node) -> None:
        """Report one ``dynamic_schedule`` for task calls under an unresolvable iterable.

        Args:
            node: The ``for`` loop or comprehension that could not be unrolled. Nothing is
                reported when it creates no tasks.
        """
        call = next(
            (n for n in ast.walk(node) if isinstance(n, ast.Call) and self.is_task_call(n)), None
        )
        if call is not None:
            self.issue(call, "Tasks built from a runtime iterable cannot be resolved statically")

    # -------------------------------------------------------------- dependencies
    def refs(self, node) -> list[str]:
        """Resolve a dependency operand to the task schedule ids it names.

        Args:
            node: An operand of ``>>``/``<<``, an element of a ``chain()`` argument, or a
                TaskFlow call argument.

        Returns:
            The schedule ids ``node`` refers to: a variable's currently bound tasks, a
            group variable's members, every element of a list/tuple, the result of a task
            call, or the rightmost operand of a nested ``>>``/``<<`` chain. Empty when
            ``node`` names nothing resolvable.
        """
        if isinstance(node, ast.Name):
            return list(self.symbols.get(node.id, []))
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            return [ref for element in node.elts for ref in self.refs(element)]
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
            return self.binop(node)
        if isinstance(node, (ast.Call, ast.ListComp, ast.SetComp, ast.GeneratorExp)):
            return self.register(node)
        return []

    def binop(self, node) -> list[str]:
        """Apply one ``>>``/``<<`` chain and return the ids of its rightmost operand.

        ``a >> b >> c`` parses as ``(a >> b) >> c``, so the left operand is resolved
        first and its rightmost ids feed the next link.

        Args:
            node: The ``BinOp`` (or a plain operand at the end of the recursion).

        Returns:
            The schedule ids of ``node``'s rightmost operand.
        """
        if not (isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift))):
            return self.refs(node)
        left = self.binop(node.left)
        right = self.binop(node.right)
        if isinstance(node.op, ast.RShift):
            self.connect(left, right)
        else:
            self.connect(right, left)
        return right

    def dependency_call(self, call: ast.Call) -> bool:
        """Apply ``chain``/``cross_downstream``/``set_upstream``/``set_downstream`` calls.

        Args:
            call: The call node.

        Returns:
            ``True`` when the call was a dependency expression (and was applied).
        """
        method = self.name(call.func)
        if method in {"chain", "cross_downstream"}:
            for left, right in zip(call.args, call.args[1:], strict=False):
                left_refs, right_refs = self.refs(left), self.refs(right)
                if (
                    method == "chain"
                    and isinstance(left, (ast.List, ast.Tuple))
                    and isinstance(right, (ast.List, ast.Tuple))
                ):
                    if len(left_refs) != len(right_refs):
                        self.issue(call, "chain() lists must have equal lengths")
                    else:
                        for a, b in zip(left_refs, right_refs, strict=True):
                            self.connect([a], [b])
                else:
                    self.connect(left_refs, right_refs)
            return True
        if (
            method in {"set_upstream", "set_downstream"}
            and call.args
            and isinstance(call.func, ast.Attribute)
        ):
            left, right = self.refs(call.func.value), self.refs(call.args[0])
            if method == "set_upstream":
                self.connect(right, left)
            else:
                self.connect(left, right)
            return True
        return False

    def connect(self, left, right) -> None:
        """Record every task in ``left`` as upstream of every task in ``right``.

        Args:
            left: Upstream task schedule ids.
            right: Downstream task schedule ids.
        """
        schedules = self.result.schedules
        for a in left:
            for b in right:
                if a == b or a not in schedules or b not in schedules:
                    continue
                schedules[b].declared_upstream = sorted(set(schedules[b].declared_upstream) | {a})
                schedules[a].declared_downstream = sorted(
                    set(schedules[a].declared_downstream) | {b}
                )
