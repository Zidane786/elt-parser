"""Product metadata with database ownership and schedule declarations."""

from pathlib import Path

import yaml

from etl_parser.identity import scheme_for_engine
from etl_parser.models import DeclaredDependency, Product, ProductDatabase, Schedule
from etl_parser.workers.base import normalize_cron


class ProductRegistry:
    """Loads ``product.yaml`` files and answers ownership and schedule queries.

    Populated once per scan (see ``etl_parser.pipeline.scan``) and consulted by the graph
    builder to attach ``product``/``layer`` to datasets and jobs, and to fold
    ``product.yaml``-declared schedules into the lineage document (spec section 6).
    """

    def __init__(self):
        """Create an empty registry with no products, databases, or schedules."""
        self.products: list[Product] = []
        self._databases = {}
        self._schedules = {}

    @classmethod
    def load(cls, path: Path | str):
        """Build a registry from one ``product.yaml`` file or a directory tree of them.

        Args:
            path: A single ``product.yaml`` file, or a directory searched recursively for
                files named ``product.yaml``.

        Returns:
            ProductRegistry: A registry populated from every file found, processed in
            sorted path order.

        Raises:
            ValueError: Propagated from :meth:`add_text` if a file is not a valid product
                document, declares a duplicate product code, or claims a database another
                product already owns.
        """
        path = Path(path)
        registry = cls()
        files = sorted(path.rglob("product.yaml")) if path.is_dir() else [path]
        for file in files:
            registry.add_text(file.read_text(), str(file))
        return registry

    def add_text(self, text: str, source_file: str):
        """Parse one ``product.yaml`` document's text and register it.

        Args:
            text: Raw YAML content of a ``product.yaml`` file.
            source_file: Path the text was read from, used for error messages and
                recorded as ``Product.source_file`` / ``Schedule.source_file``.

        Returns:
            Product: The parsed and registered product.

        Raises:
            ValueError: If ``text`` does not parse to a YAML mapping, if its ``code``
                duplicates an already-registered product, or if one of its databases is
                already owned by another registered product.
        """
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid product document: {source_file}")
        product = Product(
            code=data["code"],
            name=data["name"],
            domain=data.get("domain"),
            owners=data.get("owners", []),
            source_file=source_file,
            databases=[ProductDatabase(**db) for db in data.get("databases", [])],
            declared_dependencies=[
                DeclaredDependency(
                    product=d.get("product"),
                    code=d.get("code", d.get("product", "unknown")),
                    tables=d.get("tables", d.get("reads", [])),
                )
                for d in data.get("cross_product_dependencies", [])
            ],
            orchestrator_type=data.get("orchestrator", {}).get("type"),
            orchestrator_file=data.get("orchestrator", {}).get("file"),
        )
        if any(p.code == product.code for p in self.products):
            raise ValueError(f"Duplicate product code: {product.code}")
        for database in product.databases:
            if database.name in self._databases:
                raise ValueError(f"Database has multiple product owners: {database.name}")
        self.products.append(product)
        for database in product.databases:
            # One name can be declared for several engines (the same warehouse reached
            # through Athena and through a Postgres driver), so entries are kept per name.
            self._databases.setdefault(database.name, []).append(
                (product, database.layer, scheme_for_engine(database.type))
            )
        schedules = data.get("schedules", {})
        values: dict[str, object] = {}
        # Products spell per-script schedules either way; the meter fixture uses "scripts"
        # and its schedules were silently ignored while only "steps" was read (finding 31).
        for key in ("steps", "scripts"):
            entries = schedules.get(key) or {}
            values.update(
                {s["step"]: s["schedule"] for s in entries}
                if isinstance(entries, list)
                else dict(entries)
            )
        if schedules.get("primary") is not None:
            values["__primary__"] = schedules["primary"]
        for name, value in values.items():
            ident = f"product.{product.code}.{name}"
            self._schedules[ident] = Schedule(
                id=ident,
                orchestrator="product_yaml",
                interval_text=str(value),
                cron=normalize_cron(str(value)),
                source_file=source_file,
                task_id=None if name == "__primary__" else name,
            )
        return product

    def _owner(self, db, scheme=None):
        """Return the best ownership entry for a database name (finding 30).

        The declared engine *prefers* an entry, it does not veto ownership: a name match
        alone still owns the database, because losing true attribution (a warehouse reached
        through another driver) is worse than the mis-attribution strict matching avoids.
        Callers report the disagreement with :meth:`engine_matches`.

        Args:
            db: Database name (matches ``ProductDatabase.name``).
            scheme: Dataset id scheme the name was seen under (e.g. ``"glue"``), or
                ``None`` to take the first entry.

        Returns:
            tuple | None: The best ``(product, layer, scheme)`` entry: one declaring this
            exact scheme, else one declaring no engine at all, else the first entry
            declared for the name. ``None`` only when the name is unknown.
        """
        entries = self._databases.get(db)
        if not entries:
            return None
        if scheme is not None:
            for wanted in (scheme, None):
                for entry in entries:
                    if entry[2] == wanted:
                        return entry
        return entries[0]

    def engine_matches(self, db, scheme):
        """Report whether a database's declared engine agrees with an observed scheme.

        Args:
            db: Database name (matches ``ProductDatabase.name``).
            scheme: Dataset id scheme the name was seen under (e.g. ``"postgres"``).

        Returns:
            bool: True when the name is unknown, when it declares no engine, or when some
            declaration matches ``scheme``; False only when every declaration for this name
            names a different engine.
        """
        entries = self._databases.get(db)
        if not entries:
            return True
        return any(entry[2] is None or entry[2] == scheme for entry in entries)

    def product_for_database(self, db, scheme=None):
        """Return the product that owns a database, if any.

        Args:
            db: Database name (matches ``ProductDatabase.name``).
            scheme: Dataset id scheme the name was seen under, used to pick between
                several declarations of the same name.

        Returns:
            Product | None: The owning product, or ``None`` if no registered product
            declares this database.
        """
        value = self._owner(db, scheme)
        return value[0] if value else None

    def layer_for_database(self, db, scheme=None):
        """Return the declared layer for a database, if any.

        Args:
            db: Database name (matches ``ProductDatabase.name``).
            scheme: Dataset id scheme the name was seen under, used to pick between
                several declarations of the same name.

        Returns:
            str | None: The declared layer (e.g. ``"raw"``), or ``None`` if the database
            is not owned by a registered product or declares no layer.
        """
        value = self._owner(db, scheme)
        return value[1] if value else None

    def schedules(self):
        """Return all schedules declared across every registered product's ``product.yaml``.

        Returns:
            dict[str, Schedule]: A copy of the internal id-to-``Schedule`` mapping.
        """
        return dict(self._schedules)

    def resolve_dependencies(self):
        """Resolve each declared cross-product dependency's product name to its code.

        Mutates ``declared_dependencies`` on every registered product in place, replacing
        ``DeclaredDependency.code`` (which may hold a product *name* as written in YAML)
        with the matching product's ``code`` when one is found by name.
        """
        by_name = {p.name: p.code for p in self.products}
        for product in self.products:
            for dependency in product.declared_dependencies:
                dependency.code = by_name.get(dependency.code, dependency.code)
