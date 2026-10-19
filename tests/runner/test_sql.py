from contextlib import closing
import json
import sqlite3

import pytest

from vectorops.runner.runner import sql


@pytest.mark.parametrize("value", ["x' OR 1=1 --", "a\x00b", "?%_", "plain"])
def test_bound_values_and_repeated_compilation(value: str) -> None:
    table = sql.Table("items")
    query = sql.Query.from_(table).select(table.id).where(table.id == value)
    statement, parameters = sql.compile(query)
    assert value not in statement
    assert parameters == [value]
    assert sql.compile(query) == (statement, parameters)
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE items (id TEXT PRIMARY KEY)")
        sql.insert_many(connection, table, [(value,), ("other",)])
        assert sql.execute(connection, query).fetchall() == [(value,)]


def test_update_expressions_null_and_returning() -> None:
    table = sql.Table("items")
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute(
            "CREATE TABLE items (id TEXT PRIMARY KEY, counter INTEGER, payload TEXT, optional TEXT)"
        )
        sql.execute(connection, sql.Query.into(table).insert("one", 1, "{}", "present"))
        query = (
            sql.Query.update(table)
            .set(table.counter, table.counter + 1)
            .set(table.payload, sql.json_set(table.payload, "$.value", "x' OR 1=1 --"))
            .set(table.optional, None)
            .where(table.id == "one")
            .returning(table.id)
        )
        assert sql.execute(connection, query).fetchall() == [("one",)]
        row = sql.execute(
            connection,
            sql.Query.from_(table).select(table.counter, table.payload, table.optional),
        ).fetchone()
        assert row[0] == 2
        assert json.loads(row[1]) == {"value": "x' OR 1=1 --"}
        assert row[2] is None


def test_upsert_batch_and_transaction_rollback() -> None:
    table = sql.Table("items")
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT)")
        with connection:
            sql.insert_many(connection, table, [])
            sql.insert_many(connection, table, ((str(i), "old") for i in range(3)))
            sql.insert_many(connection, table, [("0", "ignored")], ignore=True)
            sql.execute(
                connection,
                sql.Query.into(table)
                .insert("1", "new")
                .on_conflict(table.id)
                .do_update(table.value),
            )
        assert sql.execute(
            connection, sql.Query.from_(table).select(table.value).orderby(table.id)
        ).fetchall() == [("old",), ("new",), ("old",)]
        with pytest.raises(sqlite3.IntegrityError):
            with connection:
                sql.insert_many(
                    connection, table, [("new", "first"), ("0", "duplicate")]
                )
        assert not sql.execute(
            connection, sql.Query.from_(table).select(table.id).where(table.id == "new")
        ).fetchall()


@pytest.mark.parametrize(
    "identifiers", [[], ["x' OR 1=1 --"], [str(i) for i in range(2000)]]
)
def test_json_collection_queries(identifiers: list[str]) -> None:
    table = sql.Table("items")
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE items (id TEXT PRIMARY KEY)")
        sql.insert_many(connection, table, [(item,) for item in identifiers])
        sql.insert_many(connection, table, [("outside",)])
        query = (
            sql.Query.from_(table)
            .select(table.id)
            .where(table.id.isin(sql.json_values(identifiers)))
        )
        assert len(sql.compile(query)[1]) == 1
        assert {row[0] for row in sql.execute(connection, query)} == set(identifiers)
        sql.execute(
            connection,
            sql.Query.from_(table)
            .delete()
            .where(table.id.isin(sql.json_values(identifiers))),
        )
        assert sql.execute(
            connection, sql.Query.from_(table).select(table.id)
        ).fetchall() == [("outside",)]


def test_json_filter_preserves_expression_index() -> None:
    table = sql.Table("items")
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE items (payload TEXT)")
        connection.execute(
            "CREATE INDEX item_status ON items(json_extract(payload, '$.status'))"
        )
        query = (
            sql.Query.from_(table)
            .select(table.payload)
            .where(sql.json_extract(table.payload, "$.status") == "pending")
        )
        statement, parameters = sql.compile(query)
        assert parameters == ["pending"]
        plan = connection.execute(
            "EXPLAIN QUERY PLAN " + statement, parameters
        ).fetchall()
        assert any("USING INDEX item_status" in row[3] for row in plan)


def test_recursive_cte_and_union_parameter_order() -> None:
    table = sql.Table("items")
    path = sql.Table("path")
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE items (id TEXT PRIMARY KEY, previous TEXT)")
        sql.insert_many(
            connection, table, [("root", None), ("leaf'", "root"), ("other", None)]
        )
        ancestors = (
            sql.Query.from_(table)
            .select(table.id, table.previous)
            .where(table.id == "leaf'")
            .union(
                sql.Query.from_(table)
                .join(path)
                .on(table.id == path.previous)
                .select(table.id, table.previous)
            )
        )
        query = (
            sql.Query.with_(ancestors, "path")
            .from_(table)
            .select(table.id)
            .where(table.id.isin(sql.Query.from_(path).select(path.id)))
            .where(table.id != "excluded")
        )
        assert sql.compile(query)[1] == ["leaf'", "excluded"]
        assert {row[0] for row in sql.execute(connection, query)} == {"root", "leaf'"}
