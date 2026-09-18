"""Airflow schedules and task order from AST; DAG modules are never imported.

Implements design section 8.4. A DAG file is parsed statically with ``ast``: ``DAG(...)``
constructor calls and ``@dag``-decorated functions become ``Schedule`` entries (one per
task plus one for the DAG itself), operator instantiations become tasks mapped to jobs
(delegating SQL operators to ``SqlWorker`` and Python callables to ``PythonWorker``), and
``>>``/``<<``/``chain``/``set_upstream``/``set_downstream``/``TriggerDagRunOperator``/
``ExternalTaskSensor`` usage becomes each schedule's ``declared_upstream``/
``declared_downstream``. String and constant values (``dag_id``, cron text, operator
arguments, ...) are resolved through ``etl_parser.scanner.strings.fold_string`` rather than
by executing the module, so anything data-dependent becomes an ``Unresolved`` item instead
of a guess.

Entry points: ``AirflowWorker.analyze_source`` (a parsed ``SourceFile``) and
``AirflowWorker.analyze_file`` (a path, wraps it in a ``SourceFile``).
"""

from __future__ import annotations

import ast
import re
import shlex
from pathlib import Path

from etl_parser.models import Job, Schedule, Unresolved, WorkerResult
from etl_parser.scanner.strings import fold_string
from etl_parser.workers.base import normalize_cron
from etl_parser.workers.sql import SqlWorker


class AirflowWorker:
    """Parses Airflow DAG modules statically into ``Schedule``, task, and job edges.

    Attributes:
        index: ``ScanIndex`` used to resolve script paths (``BashOperator``/
            ``SparkSubmitOperator``) and imported callables (``PythonOperator``) to the
            ``SourceFile`` that defines their job.
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

        The module is never imported or executed. Module-level assignments are folded into
        an environment (``env``) up front so ``DAG(...)``/operator arguments referencing
        them resolve; a nested ``visit`` walks the module body tracking the enclosing
        ``with DAG(...):`` block or ``@dag``-decorated function to build each ``Schedule``
        and task; a second pass over every node connects tasks via ``>>``/``<<``/
        ``chain``/``cross_downstream``/``set_upstream``/``set_downstream`` and TaskFlow
        (``@task``) call arguments.

        Args:
            source: The DAG file as a ``SourceFile`` (path, text, and derived job id).

        Returns:
            A ``WorkerResult`` whose ``schedules`` holds one entry per task plus one for
            each DAG, ``task_jobs`` maps each schedule id to its resolved job id (or
            ``None``), and ``jobs``/``column_edges``/``table_edges``/``unresolved`` include
            everything produced by any SQL or Python job a task resolved to. When the file
            fails to parse, the result holds a single ``unsupported_syntax`` item and
            nothing else.
        """
        result = WorkerResult()
        try:
            tree = ast.parse(source.text)
        except SyntaxError as exc:
            result.unresolved.append(
                Unresolved(
                    kind="unsupported_syntax",
                    source_file=source.path,
                    line=exc.lineno,
                    reason=str(exc),
                )
            )
            return result
        env = dict(self.bindings)
        for node in tree.body:
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if isinstance(target, ast.Name) and target.id not in self.bindings:
                        env[target.id] = fold_string(node.value, env)
        symbols = {}
        dag_ranges = []
        decorated = {}
        aliases = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    aliases[alias.asname or alias.name] = alias.name

        def name(node):
            """Return an unqualified, alias-resolved name for a call target or reference.

            Args:
                node: The AST expression to name, typically a ``Call.func``.

            Returns:
                The last dotted segment of ``node`` (e.g. ``"DAG"`` for ``airflow.DAG``),
                resolved through ``aliases`` when it was imported under another name.
            """
            text = ast.unparse(node).split(".")[-1]
            return aliases.get(text, text)

        def value(node):
            """Fold ``node`` to a plain string only when fully and unambiguously resolved.

            Args:
                node: The AST expression to fold, or ``None``.

            Returns:
                The folded text, or ``None`` when ``node`` is ``None`` or the fold is
                incomplete or relies on an assumed (defaulted) value.
            """
            if node is None:
                return None
            folded = fold_string(node, env)
            return folded.text if folded.complete and not folded.assumptions else None

        def issue(node, reason, kind="dynamic_schedule"):
            """Record an ``Unresolved`` item pointing at ``node``.

            Args:
                node: AST node the issue is about, used for its line number and unparsed
                    expression text.
                reason: Human-readable explanation of what could not be resolved.
                kind: ``Unresolved.kind`` to record.
            """
            result.unresolved.append(
                Unresolved(
                    kind=kind,
                    source_file=source.path,
                    line=node.lineno,
                    expression=ast.unparse(node),
                    reason=reason,
                )
            )

        def dag_schedule(call, fallback):
            """Build and register the ``Schedule`` for a ``DAG(...)`` call or ``@dag`` decorator.

            Reads ``dag_id``, ``schedule``/``schedule_interval`` (normalised to cron via
            ``normalize_cron``, with an unresolvable non-``timedelta`` value raising a
            ``dynamic_schedule`` issue), ``catchup``, ``tags``, ``start_date`` (and its
            timezone keyword when given as a call), and ``default_args["owner"]``
            (resolving a ``default_args`` variable back to its module-level assignment when
            needed).

            Args:
                call: The ``DAG(...)`` call node (constructed synthetically for a bare
                    ``@dag`` decorator with no call arguments).
                fallback: ``dag_id`` to use when it cannot be resolved from ``call``.

            Returns:
                The new ``Schedule``, already stored in ``result.schedules`` under its id
                ``f"airflow.{source.job_id}.{dag_id}"``.
            """
            kw = {k.arg: k.value for k in call.keywords if k.arg}
            dag_id = value(kw.get("dag_id") or (call.args[0] if call.args else None)) or fallback
            interval = kw.get("schedule", kw.get("schedule_interval"))
            text = value(interval)
            if isinstance(interval, ast.Constant) and interval.value is None:
                text = None
            elif interval is not None and text is None:
                text = ast.unparse(interval)
                if "timedelta(" not in text:
                    issue(interval, "DAG schedule cannot be resolved statically")
            ident = f"airflow.{source.job_id}.{dag_id}"
            schedule = Schedule(
                id=ident,
                orchestrator="airflow",
                dag_id=dag_id,
                cron=normalize_cron(text),
                interval_text=text,
                source_file=source.path,
                line=call.lineno,
            )
            for key in ("catchup", "tags"):
                if key in kw:
                    try:
                        setattr(schedule, key, ast.literal_eval(kw[key]))
                    except (ValueError, TypeError):
                        issue(kw[key], f"Cannot resolve {key}")
            if "start_date" in kw:
                schedule.start_date = ast.unparse(kw["start_date"])
                if isinstance(kw["start_date"], ast.Call):
                    zone = next(
                        (k.value for k in kw["start_date"].keywords if k.arg in {"tz", "tzinfo"}),
                        None,
                    )
                    schedule.timezone = value(zone)
            defaults = kw.get("default_args")
            if isinstance(defaults, ast.Name):
                defaults = next(
                    (
                        n.value
                        for n in tree.body
                        if isinstance(n, ast.Assign)
                        and any(isinstance(t, ast.Name) and t.id == defaults.id for t in n.targets)
                    ),
                    None,
                )
            if isinstance(defaults, ast.Dict):
                schedule.owner = next(
                    (
                        value(v)
                        for k, v in zip(defaults.keys, defaults.values, strict=True)
                        if isinstance(k, ast.Constant) and k.value == "owner"
                    ),
                    None,
                )
            result.schedules[ident] = schedule
            return schedule

        def task(call, dag, variable=None, function=None):
            """Register one task's ``Schedule`` and resolve its operator to a job.

            Copies ``dag``'s schedule to a task-scoped id ``f"{dag.id}.{task_id}"``, then
            maps the operator per design section 8.4 step 2: ``BashOperator``/
            ``SparkSubmitOperator`` resolve their script path through ``self.index.script``;
            ``PythonOperator``/``PythonVirtualenvOperator``/a ``@task``-decorated callable
            resolve their callable (following an import when needed) and delegate to
            ``PythonWorker``; ``AthenaOperator``/``PostgresOperator``/``TrinoOperator``/
            ``SQLExecuteQueryOperator`` delegate their SQL to ``self.sql``;
            ``ExternalTaskSensor``/``TriggerDagRunOperator`` record cross-DAG
            ``declared_upstream``/``declared_downstream``; ``GlueJobOperator``/
            ``EmrAddStepsOperator`` and anything unresolvable become an ``Unresolved`` item
            with no job. Registers the task id in ``symbols`` when assigned to a variable,
            so later ``>>``/``chain()`` expressions can reference it.

            Args:
                call: The operator instantiation call node.
                dag: The enclosing DAG's ``Schedule``, copied to build the task's schedule.
                variable: Name the call's result was assigned to, if any.
                function: Name of the ``@task``-decorated function being called, when this
                    task comes from a TaskFlow call rather than an ``Operator(...)`` call.

            Returns:
                The task's schedule id, or ``None`` when ``task_id`` could not be resolved
                (an ``Unresolved`` item is recorded and no schedule is created).
            """
            kw = {k.arg: k.value for k in call.keywords if k.arg}
            task_id = value(kw.get("task_id")) or function or variable
            if not task_id:
                issue(call, "Task ID is dynamic")
                return None
            ident = f"{dag.id}.{task_id}"
            schedule = dag.model_copy(
                deep=True, update={"id": ident, "task_id": task_id, "line": call.lineno}
            )
            if "owner" in kw:
                schedule.owner = value(kw["owner"])
            result.schedules[ident] = schedule
            result.task_jobs[ident] = None
            if variable:
                symbols[(dag.id, variable)] = [ident]
            operator = name(call.func)
            job = None
            if operator in {"BashOperator", "SparkSubmitOperator"}:
                command = value(kw.get("bash_command") or kw.get("application"))
                if command:
                    try:
                        paths = [
                            part for part in shlex.split(command) if part.endswith((".py", ".sql"))
                        ]
                    except ValueError:
                        paths = []
                    paths += re.findall(r"\$\(cat\s+([^\s)]+\.sql)\)", command)
                    matches = [self.index.script(path) for path in paths]
                    matches = [m for m in matches if m]
                    if len(matches) == 1:
                        job = matches[0].job_id
                if job is None:
                    issue(call, "Task script could not be resolved unambiguously", "external_job")
            elif operator in {"PythonOperator", "PythonVirtualenvOperator"} or function:
                from etl_parser.workers.python import PythonWorker

                job = ident
                callable_source = source
                callable_node = kw.get("python_callable")
                function_name = function or (
                    callable_node.id if isinstance(callable_node, ast.Name) else None
                )
                if isinstance(callable_node, ast.Name):
                    for imported in ast.walk(tree):
                        if isinstance(imported, ast.ImportFrom):
                            if any(
                                (a.asname or a.name) == callable_node.id for a in imported.names
                            ):
                                module = self.index.resolve_module(
                                    imported.module or "", source, imported.level
                                )
                                if module:
                                    callable_source = module
                                    function_name = next(
                                        a.name
                                        for a in imported.names
                                        if (a.asname or a.name) == callable_node.id
                                    )
                                else:
                                    job = None
                                    issue(
                                        call,
                                        "Python callable import could not be resolved",
                                        "unresolved_import",
                                    )
                if job and function_name:
                    result.extend(
                        PythonWorker(self.sql, self.index, bindings=self.bindings).analyze_source(
                            callable_source, entry_function=function_name, job_id_override=ident
                        )
                    )
                elif job:
                    job = None
                    issue(call, "Python callable cannot be identified", "unresolved_import")
            elif operator in {
                "AthenaOperator",
                "PostgresOperator",
                "TrinoOperator",
                "SQLExecuteQueryOperator",
            }:
                dialect = {
                    "AthenaOperator": "trino",
                    "TrinoOperator": "trino",
                    "PostgresOperator": "postgres",
                }.get(operator)
                if operator == "SQLExecuteQueryOperator":
                    connection = value(kw.get("conn_id"))
                    dialect = self.bindings.get(f"connection:{connection}")
                if dialect is None:
                    issue(
                        call,
                        "Generic SQL operator requires a connection dialect binding",
                        "unsupported_syntax",
                    )
                else:
                    sql_node = kw.get("query") or kw.get("sql")
                    query = value(sql_node)
                    if query is None:
                        issue(call, "Operator SQL depends on runtime values", "dynamic_sql")
                    else:
                        job = ident
                        engine = "athena" if dialect in {"trino", "presto"} else dialect
                        analyzed = self.sql.analyze(
                            query,
                            dialect=dialect,
                            engine=engine,
                            job_id=job,
                            source_file=source.path,
                            line_offset=call.lineno - 1,
                        )
                        result.extend(analyzed.result)
                        result.jobs.append(
                            Job(
                                id=job,
                                name=task_id,
                                source_file=source.path,
                                language="sql",
                                engine=engine,
                                dialect=dialect,
                                inputs=sorted(analyzed.inputs),
                                outputs=sorted(analyzed.outputs),
                                schedule_id=ident,
                            )
                        )
            elif operator == "ExternalTaskSensor":
                external_dag = value(kw.get("external_dag_id"))
                external_task = value(kw.get("external_task_id"))
                if external_dag:
                    schedule.declared_upstream = [
                        f"external-dag://{external_dag}/{external_task or ''}"
                    ]
                else:
                    issue(call, "External DAG identifier is dynamic", "external_job")
            elif operator == "TriggerDagRunOperator":
                external_dag = value(kw.get("trigger_dag_id"))
                if external_dag:
                    schedule.declared_downstream = [f"external-dag://{external_dag}/"]
                else:
                    issue(call, "Triggered DAG identifier is dynamic", "external_job")
            elif operator in {"GlueJobOperator", "EmrAddStepsOperator"}:
                issue(call, "External task/job requires explicit mapping", "external_job")
            result.task_jobs[ident] = job
            return ident

        def visit(body, dag=None):
            """Walk a block of statements, tracking the enclosing DAG to find tasks.

            Recurses into ``with DAG(...):`` blocks and ``@dag``-decorated function bodies
            (registering their line range in ``dag_ranges`` so later dependency edges can
            be attributed to the right DAG), records ``@task``-decorated functions in
            ``decorated`` without visiting them as tasks directly, and calls ``task`` for
            each recognised operator/sensor instantiation (as an assignment or a bare
            expression statement) found inside a DAG. Also recurses into ``if`` blocks.

            Args:
                body: A list of AST statements to walk (a module, function, or ``with``/
                    ``if`` body).
                dag: The enclosing DAG's ``Schedule``, or ``None`` outside any DAG.
            """
            for node in body:
                if isinstance(node, ast.With):
                    nested = dag
                    for item in node.items:
                        if (
                            isinstance(item.context_expr, ast.Call)
                            and name(item.context_expr.func) == "DAG"
                        ):
                            nested = dag_schedule(item.context_expr, Path(source.path).stem)
                            dag_ranges.append((node.lineno, node.end_lineno, nested.id))
                    visit(node.body, nested)
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    decorators = [
                        (d.func if isinstance(d, ast.Call) else d, d) for d in node.decorator_list
                    ]
                    if any(name(func) == "dag" for func, _ in decorators):
                        decorator = next(d for f, d in decorators if name(f) == "dag")
                        if not isinstance(decorator, ast.Call):
                            decorator = ast.copy_location(
                                ast.Call(func=decorator, args=[], keywords=[]), node
                            )
                        nested = dag_schedule(decorator, node.name)
                        dag_ranges.append((node.lineno, node.end_lineno, nested.id))
                        visit(node.body, nested)
                    elif any(name(func) == "task" for func, _ in decorators):
                        decorated[node.name] = node
                    else:
                        visit(node.body, dag)
                elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                    call = node.value
                    variable = node.targets[0].id if isinstance(node.targets[0], ast.Name) else None
                    if name(call.func) == "DAG":
                        dag = dag_schedule(call, variable or "dag")
                        dag_ranges.append((node.lineno, tree.body[-1].end_lineno, dag.id))
                    elif dag and (
                        name(call.func).endswith(("Operator", "Sensor"))
                        or name(call.func) in decorated
                    ):
                        task(
                            call,
                            dag,
                            variable,
                            name(call.func) if name(call.func) in decorated else None,
                        )
                elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and dag:
                    if name(node.value.func).endswith(("Operator", "Sensor")):
                        task(node.value, dag)
                elif isinstance(node, ast.If):
                    visit(node.body, dag)
                    visit(node.orelse, dag)

        visit(tree.body)

        def refs(node):
            """Resolve a dependency-operator operand to the task schedule ids it names.

            Args:
                node: The left- or right-hand operand of ``>>``/``<<``, or an element of a
                    ``chain()``/``set_upstream``/``set_downstream`` argument.

            Returns:
                Schedule ids ``node`` refers to: a variable's tasks (looked up in
                ``symbols`` under the current DAG), each element's refs for a list/tuple, or
                the right operand's refs for a chained ``BinOp`` (e.g. ``a >> b >> c``).
                Empty when ``node`` names nothing resolvable.
            """
            if isinstance(node, ast.Name):
                return symbols.get((current_dag, node.id), [])
            if isinstance(node, (ast.List, ast.Tuple)):
                return [ref for item in node.elts for ref in refs(item)]
            if isinstance(node, ast.BinOp):
                return refs(node.right)
            return []

        def connect(left, right):
            """Record every task in ``left`` as upstream of every task in ``right``.

            Args:
                left: Upstream task schedule ids.
                right: Downstream task schedule ids.
            """
            for a in left:
                for b in right:
                    result.schedules[b].declared_upstream = sorted(
                        set(result.schedules[b].declared_upstream) | {a}
                    )
                    result.schedules[a].declared_downstream = sorted(
                        set(result.schedules[a].declared_downstream) | {b}
                    )

        for node in ast.walk(tree):
            ranges = [r for r in dag_ranges if r[0] <= getattr(node, "lineno", -1) <= r[1]]
            current_dag = max(ranges)[2] if ranges else None
            if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
                left, right = refs(node.left), refs(node.right)
                connect(right, left) if isinstance(node.op, ast.LShift) else connect(left, right)
            elif isinstance(node, ast.Call):
                method = name(node.func)
                if method in {"chain", "cross_downstream"}:
                    for left, right in zip(node.args, node.args[1:], strict=False):
                        left_refs, right_refs = refs(left), refs(right)
                        if (
                            method == "chain"
                            and isinstance(left, (ast.List, ast.Tuple))
                            and isinstance(right, (ast.List, ast.Tuple))
                        ):
                            if len(left_refs) != len(right_refs):
                                issue(node, "chain() lists must have equal lengths")
                            else:
                                for a, b in zip(left_refs, right_refs, strict=True):
                                    connect([a], [b])
                        else:
                            connect(left_refs, right_refs)
                elif method in {"set_upstream", "set_downstream"} and node.args:
                    left, right = refs(node.func.value), refs(node.args[0])
                    connect(right, left) if method == "set_upstream" else connect(left, right)
                elif method in decorated:
                    # TaskFlow call arguments express implicit upstream dependencies.
                    for assignment in ast.walk(tree):
                        if isinstance(assignment, ast.Assign) and assignment.value is node:
                            target = refs(assignment.targets[0])
                            for arg in node.args:
                                connect(refs(arg), target)
        return result

    def analyze_file(self, path):
        """Analyze a DAG file given as a filesystem path.

        Args:
            path: Path to the ``.py`` DAG file, expected to lie under ``self.index.root``.

        Returns:
            The ``WorkerResult`` from ``analyze_source`` for this file's contents.

        Raises:
            ValueError: If ``path`` does not lie under ``self.index.root`` (from
                ``Path.relative_to``).
        """
        from etl_parser.scanner.repo import SourceFile

        return self.analyze_source(
            SourceFile(path.relative_to(self.index.root).as_posix(), path.read_text(), ".py")
        )
