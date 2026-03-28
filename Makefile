.PHONY: sync lint typecheck test check run-api run-worker

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

run-api:
	uv run carryme-api

run-worker:
	uv run carryme-worker
