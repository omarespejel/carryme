"""Database helpers shared by storage backends."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine, Result, Row, make_url

DEFAULT_DATABASE_PING_TIMEOUT_SECONDS = 5.0


def normalize_database_url(database: str | Path) -> str:
    """Normalize a filesystem path or database URL into a SQLAlchemy URL."""

    raw = str(database)
    if "://" in raw:
        return raw
    if raw == ":memory:":
        return "sqlite:///:memory:"
    return f"sqlite:///{raw}"


def redact_database_url(database: str | Path) -> str:
    """Return a log-safe database target string."""

    url = make_url(normalize_database_url(database))
    if url.get_backend_name() == "sqlite":
        return url.database or ":memory:"

    drivername = url.drivername
    host = url.host or "localhost"
    database_name = url.database or ""
    return f"{drivername}://***@{host}/{database_name}".rstrip("/")


def _translate_dialect_sql(sql: str, backend_name: str) -> str:
    """Translate storage DDL/DML into the active backend dialect."""

    if backend_name == "postgresql":
        return sql.replace(
            "INTEGER PRIMARY KEY AUTOINCREMENT",
            "BIGSERIAL PRIMARY KEY",
        )
    return sql


def _prepare_sql(
    sql: str,
    params: Sequence[object],
    *,
    backend_name: str,
) -> tuple[str, dict[str, object]]:
    """Convert qmark-style SQL placeholders into SQLAlchemy bind params."""

    sql = _translate_dialect_sql(sql, backend_name)
    if not params:
        return sql, {}

    bind_params: dict[str, object] = {}
    pieces: list[str] = []
    placeholder_index = 0

    for character in sql:
        if character == "?":
            key = f"p{placeholder_index}"
            bind_params[key] = params[placeholder_index]
            pieces.append(f":{key}")
            placeholder_index += 1
            continue
        pieces.append(character)

    if placeholder_index != len(params):
        raise ValueError(
            "SQL placeholder count did not match parameter count "
            f"({placeholder_index} placeholders, {len(params)} parameters)"
        )
    return "".join(pieces), bind_params


def _run_with_timeout(
    operation: Callable[[], None],
    *,
    timeout_seconds: float,
) -> None:
    """Run one blocking operation with a bounded timeout."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    operation_error: Exception | None = None
    completed = threading.Event()

    def _run() -> None:
        nonlocal operation_error
        try:
            operation()
        except Exception as error:  # pragma: no cover - simple propagation path
            operation_error = error
        finally:
            completed.set()

    thread = threading.Thread(
        target=_run,
        name="carryme-db-ping",
        daemon=True,
    )
    thread.start()
    if not completed.wait(timeout_seconds):
        raise TimeoutError(f"Database ping timed out after {timeout_seconds:.1f}s")
    if operation_error is not None:
        raise operation_error


class DatabaseConnection:
    """Thin wrapper around a SQLAlchemy connection using qmark SQL bindings."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._backend_name = connection.engine.dialect.name

    def execute(
        self,
        sql: str,
        params: Sequence[object] | None = None,
    ) -> Result[tuple[object, ...]]:
        """Execute one SQL statement."""

        statement, bind_params = _prepare_sql(
            sql,
            params or (),
            backend_name=self._backend_name,
        )
        return self._connection.execute(text(statement), bind_params)

    def insert_returning_id(
        self,
        sql: str,
        params: Sequence[object] | None = None,
        *,
        id_column: str = "id",
    ) -> int:
        """Execute one insert statement and return the generated integer id."""

        statement = sql.rstrip().rstrip(";")
        if " RETURNING " not in statement.upper():
            statement = f"{statement} RETURNING {id_column}"
        row = self.execute(statement, params).fetchone()
        if row is None:
            raise RuntimeError("Insert statement did not return an id")
        return int(row[0])

    def fetchall(
        self,
        sql: str,
        params: Sequence[object] | None = None,
    ) -> list[Row[tuple[object, ...]]]:
        """Execute one statement and return all rows."""

        return list(self.execute(sql, params).fetchall())

    def fetchone(
        self,
        sql: str,
        params: Sequence[object] | None = None,
    ) -> Row[tuple[object, ...]] | None:
        """Execute one statement and return the first row, if any."""

        return self.execute(sql, params).fetchone()

    def table_columns(self, table_name: str) -> set[str]:
        """Return the column names currently defined for one table."""

        inspector = inspect(self._connection)
        return {str(column["name"]) for column in inspector.get_columns(table_name)}

    def primary_key_columns(self, table_name: str) -> list[str]:
        """Return the primary-key column names for one table in order."""

        inspector = inspect(self._connection)
        constraint = inspector.get_pk_constraint(table_name)
        columns = constraint.get("constrained_columns") or []
        return [str(column) for column in columns]

    def ping(self) -> None:
        """Execute a lightweight liveness query."""

        self.execute("SELECT 1")


class Database:
    """Database target wrapper supporting SQLite paths and SQLAlchemy URLs."""

    def __init__(self, database: str | Path) -> None:
        self.url = normalize_database_url(database)
        self.target = redact_database_url(self.url)
        self._ensure_sqlite_parent_directory()
        self._engine = self._build_engine()

    def _ensure_sqlite_parent_directory(self) -> None:
        parsed = make_url(self.url)
        if parsed.get_backend_name() != "sqlite":
            return
        database = parsed.database
        if database in (None, "", ":memory:"):
            return
        assert database is not None
        Path(database).parent.mkdir(parents=True, exist_ok=True)

    def _build_engine(self) -> Engine:
        parsed = make_url(self.url)
        connect_args: dict[str, object] = {}
        if parsed.get_backend_name() == "sqlite":
            connect_args["check_same_thread"] = False
        return create_engine(
            self.url,
            future=True,
            pool_pre_ping=True,
            connect_args=connect_args,
        )

    @contextmanager
    def begin(self) -> Iterator[DatabaseConnection]:
        """Open a transactional database connection."""

        with self._engine.begin() as connection:
            yield DatabaseConnection(connection)

    def ping(self) -> None:
        """Run a readiness query against the target database."""

        with self.begin() as connection:
            connection.ping()

    def ping_with_timeout(
        self,
        timeout_seconds: float = DEFAULT_DATABASE_PING_TIMEOUT_SECONDS,
    ) -> None:
        """Run a bounded readiness query against the target database."""

        _run_with_timeout(self.ping, timeout_seconds=timeout_seconds)
