"""Bound SQL generation without connection or transaction ownership."""

from collections.abc import Iterable, Sequence
from itertools import chain
import json
import sqlite3
from typing import Any, Optional, cast

import pypika
from pypika import functions, terms
from pypika.dialects import PostgreSQLQueryBuilder
from pypika.queries import QueryBuilder, Selectable


class _Value(terms.ValueWrapper):
    """Preserve expressions and NULL when the fork wraps UPDATE assignments."""

    def get_sql(
        self,
        quote_char: Optional[str] = None,
        secondary_quote_char: str = "'",
        parameter: Optional[terms.Parameter] = None,
        **kwargs: Any,
    ) -> str:
        if isinstance(self.value, terms.Term):
            return str(
                self.value.get_sql(
                    quote_char=quote_char,
                    secondary_quote_char=secondary_quote_char,
                    parameter=parameter,
                    **kwargs,
                )
            )
        if self.value is None:
            return "NULL"
        return str(
            super().get_sql(
                quote_char=quote_char,
                secondary_quote_char=secondary_quote_char,
                parameter=cast(terms.Parameter, parameter),
                **kwargs,
            )
        )


class Query(pypika.PostgreSQLQuery):
    """Use SQLite-compatible conflict/returning syntax and unwrapped UNION arms.

    SQLite accepts recursive CTEs with WITH, without the RECURSIVE keyword.
    Transaction commands and schema definitions remain owned by each database.
    """

    @classmethod
    def _builder(cls, **kwargs: Any) -> PostgreSQLQueryBuilder:
        return PostgreSQLQueryBuilder(
            wrap_set_operation_queries=False, wrapper_cls=_Value, **kwargs
        )


Table = pypika.Table
Order = pypika.Order
Case = pypika.Case

Value = Optional[str | int | float | bytes]


class _SchemaValue(terms.ValueWrapper):
    def get_sql(
        self,
        quote_char: Optional[str] = None,
        secondary_quote_char: str = "'",
        parameter: Optional[terms.Parameter] = None,
        **kwargs: Any,
    ) -> str:
        return str(
            super().get_sql(
                quote_char=quote_char,
                secondary_quote_char=secondary_quote_char,
                **kwargs,
            )
        )


def json_extract(value: terms.Term, path: str) -> terms.Function:
    """Keep schema JSON paths literal so SQLite expression indexes still match."""
    return terms.Function("json_extract", value, _SchemaValue(path))


def json_type(value: terms.Term, path: str) -> terms.Function:
    return terms.Function("json_type", value, _SchemaValue(path))


def json_set(value: terms.Term, *arguments: object) -> terms.Function:
    return terms.Function("json_set", value, *arguments)


def json_remove(value: terms.Term, path: str) -> terms.Function:
    return terms.Function("json_remove", value, path)


def json_value(value: str) -> terms.Function:
    return terms.Function("json", value)


def json_values(values: Sequence[str]) -> QueryBuilder:
    """Bind collections once, preserving empty-set and large-list behavior."""
    return Query.from_(
        cast(Selectable, terms.Function("json_each", json.dumps(list(values))))
    ).select(terms.Field("value"))


def compile(query: terms.Term) -> tuple[str, list[Value]]:
    parameter = pypika.QmarkParameter()
    statement = query.get_sql(parameter=parameter)
    return statement, parameter.get_parameters()


def execute(connection: sqlite3.Connection, query: terms.Term) -> sqlite3.Cursor:
    statement, parameters = compile(query)
    return connection.execute(statement, parameters)


def insert_many(
    connection: sqlite3.Connection,
    table: pypika.Table,
    rows: Iterable[Sequence[Value]],
    *,
    ignore: bool = False,
) -> None:
    """Stream inserts without changing transaction boundaries or variable limits."""
    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return
    query = Query.into(table).insert(*(pypika.Parameter("?") for _ in first))
    if ignore:
        query = query.on_conflict().do_nothing()
    statement, parameters = compile(query)
    assert not parameters
    connection.executemany(statement, chain((first,), iterator))
