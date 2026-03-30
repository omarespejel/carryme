.PHONY: sync lint format typecheck test check db-upgrade run-api run-worker worker-ready

sync:
	uv sync --all-packages --group dev

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run mypy $(find src apps packages tests -name '*.py' -type f)

test:
	uv run pytest

check: lint typecheck test

db-upgrade:
	uv run alembic upgrade head

run-api:
	uv run carryme-api

run-worker:
	uv run carryme-worker

worker-ready:
	uv run carryme-worker --ready
