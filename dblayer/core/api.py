"""Methods shared by ``Database`` and ``Transaction``: introspection (built
on ``schema()``) and the named operations (built on ``_operation()``)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from ..backends.base import Capabilities
from ..errors import NotSupported, TableNotFound
from .operations import UNSET, OperationResult
from .schema import ColumnInfo, ForeignKey, Index, Schema, TableInfo

__all__ = ["IntrospectionMixin", "OperationsMixin"]


class IntrospectionMixin:
    """Needs ``schema()`` and ``capabilities``."""

    __slots__ = ()

    @property
    def capabilities(self) -> Capabilities:  # pragma: no cover - overridden
        raise NotImplementedError

    async def schema(self) -> Schema:  # pragma: no cover - overridden
        raise NotImplementedError

    async def table_names(self) -> list[str]:
        """Tables, without dblayer's hidden ``_dblayer_*`` tables."""
        return (await self.schema()).table_names()

    async def view_names(self) -> list[str]:
        return (await self.schema()).view_names()

    async def _info(self, table: str) -> TableInfo:
        info = (await self.schema()).get(table)
        if info is None:
            raise TableNotFound(f"no such table: {table}", code="NOT_FOUND")
        return info

    async def table_columns(self, table: str) -> list[ColumnInfo]:
        return list((await self._info(table)).columns)

    async def primary_keys(self, table: str) -> list[str]:
        return list((await self._info(table)).primary_keys)

    async def foreign_keys(self, table: str) -> list[ForeignKey]:
        return list((await self._info(table)).foreign_keys)

    async def indexes(self, table: str) -> list[Index]:
        return list((await self._info(table)).indexes)

    async def definition(self, name: str) -> str | None:
        """The ``CREATE`` statement for a table, view, index or trigger."""
        if not self.capabilities.table_definition_sql:
            raise NotSupported("this backend cannot supply CREATE statements")
        schema = await self.schema()
        try:
            return schema.definition(name)
        except KeyError:
            raise TableNotFound(
                f"no such table, view, index or trigger: {name}", code="NOT_FOUND"
            ) from None


class OperationsMixin:
    """The named operations. Each packs its arguments and calls
    ``_operation(name, args)``; ``Transaction`` dispatches to a native
    override or the default implementation, ``Database`` opens a
    transaction first. Arguments are documented on the default
    implementations in ``dblayer.core.operations``."""

    __slots__ = ()

    async def _operation(self, name: str, args: dict[str, Any]) -> OperationResult:  # pragma: no cover
        raise NotImplementedError

    async def create_table(
        self,
        name: str,
        columns: Mapping[str, str | type],
        *,
        pk: str | Sequence[str] | None = None,
        not_null: Iterable[str] = (),
        defaults: Mapping[str, Any] | None = None,
        foreign_keys: Iterable[Any] = (),
        if_not_exists: bool = False,
    ) -> OperationResult:
        return await self._operation(
            "create_table",
            dict(
                name=name,
                columns=columns,
                pk=pk,
                not_null=not_null,
                defaults=defaults,
                foreign_keys=foreign_keys,
                if_not_exists=if_not_exists,
            ),
        )

    async def drop_table(self, name: str, *, if_exists: bool = False) -> OperationResult:
        return await self._operation("drop_table", dict(name=name, if_exists=if_exists))

    async def rename_table(self, old: str, new: str) -> OperationResult:
        return await self._operation("rename_table", dict(old=old, new=new))

    async def add_column(self, table: str, name: str, type: str | type = "text") -> OperationResult:
        return await self._operation("add_column", dict(table=table, name=name, type=type))

    async def alter_table(
        self,
        table: str,
        *,
        rename: Mapping[str, str] | None = None,
        drop: Iterable[str] = (),
        types: Mapping[str, str | type] | None = None,
        pk: Any = UNSET,
    ) -> OperationResult:
        return await self._operation(
            "alter_table", dict(table=table, rename=rename, drop=drop, types=types, pk=pk)
        )

    async def create_index(
        self,
        table: str,
        columns: Sequence[str] | str,
        *,
        name: str | None = None,
        unique: bool = False,
        if_not_exists: bool = False,
    ) -> OperationResult:
        return await self._operation(
            "create_index",
            dict(table=table, columns=columns, name=name, unique=unique, if_not_exists=if_not_exists),
        )

    async def insert_rows(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        *,
        pk: str | Sequence[str] | None = None,
        alter: bool = False,
        ignore: bool = False,
        replace: bool = False,
        batch_size: int = 100,
    ) -> OperationResult:
        """On a transaction every row goes in that transaction and
        ``batch_size`` is ignored. ``Database.insert_rows`` commits every
        ``batch_size`` rows in its own short transaction."""
        return await self._operation(
            "insert_rows",
            dict(table=table, rows=rows, pk=pk, alter=alter, ignore=ignore, replace=replace),
        )

    async def upsert_rows(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        *,
        pk: str | Sequence[str] | None = None,
        alter: bool = False,
        batch_size: int = 100,
    ) -> OperationResult:
        return await self._operation(
            "upsert_rows", dict(table=table, rows=rows, pk=pk, alter=alter)
        )

    async def update_row(
        self, table: str, pk_values: Any, updates: Mapping[str, Any]
    ) -> OperationResult:
        return await self._operation(
            "update_row", dict(table=table, pk_values=pk_values, updates=updates)
        )

    async def delete_rows(self, table: str, pk_values_list: Iterable[Any]) -> OperationResult:
        return await self._operation(
            "delete_rows", dict(table=table, pk_values_list=list(pk_values_list))
        )
