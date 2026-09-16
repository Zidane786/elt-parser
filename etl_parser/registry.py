"""Product metadata with database ownership and schedule declarations."""

from pathlib import Path

import yaml

from etl_parser.models import DeclaredDependency, Product, ProductDatabase, Schedule
from etl_parser.workers.base import normalize_cron


class ProductRegistry:
    def __init__(self):
        self.products: list[Product] = []
        self._databases = {}
        self._schedules = {}

    @classmethod
    def load(cls, path: Path | str):
        path = Path(path)
        registry = cls()
        files = sorted(path.rglob("product.yaml")) if path.is_dir() else [path]
        for file in files:
            registry.add_text(file.read_text(), str(file))
        return registry

    def add_text(self, text: str, source_file: str):
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
            self._databases[database.name] = (product, database.layer)
        schedules = data.get("schedules", {})
        steps = schedules.get("steps", {})
        values = (
            {s["step"]: s["schedule"] for s in steps} if isinstance(steps, list) else dict(steps)
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

    def product_for_database(self, db):
        value = self._databases.get(db)
        return value[0] if value else None

    def layer_for_database(self, db):
        value = self._databases.get(db)
        return value[1] if value else None

    def schedules(self):
        return dict(self._schedules)

    def resolve_dependencies(self):
        by_name = {p.name: p.code for p in self.products}
        for product in self.products:
            for dependency in product.declared_dependencies:
                dependency.code = by_name.get(dependency.code, dependency.code)
