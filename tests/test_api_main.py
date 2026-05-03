import subprocess
from collections.abc import Sequence

import pytest
from carryme_api import main as main_module


def test_run_alembic_upgrade_with_retries_succeeds_after_transient_failure() -> None:
    attempts: list[tuple[str, ...]] = []
    sleeps: list[float] = []

    def fake_run(command: Sequence[str]) -> subprocess.CompletedProcess[object]:
        attempts.append(tuple(command))
        return subprocess.CompletedProcess(
            command,
            returncode=0 if len(attempts) == 3 else 1,
        )

    main_module.run_alembic_upgrade_with_retries(
        run=fake_run,
        sleep=sleeps.append,
        max_attempts=3,
        retry_delay_seconds=0.5,
    )

    assert attempts == [main_module.ALEMBIC_UPGRADE_COMMAND] * 3
    assert sleeps == [0.5, 0.5]


def test_run_alembic_upgrade_with_retries_exits_after_retry_budget() -> None:
    attempts: list[tuple[str, ...]] = []

    def fake_run(command: Sequence[str]) -> subprocess.CompletedProcess[object]:
        attempts.append(tuple(command))
        return subprocess.CompletedProcess(command, returncode=7)

    with pytest.raises(SystemExit) as exc_info:
        main_module.run_alembic_upgrade_with_retries(
            run=fake_run,
            sleep=lambda delay: None,
            max_attempts=2,
            retry_delay_seconds=0,
        )

    assert exc_info.value.code == 7
    assert attempts == [main_module.ALEMBIC_UPGRADE_COMMAND] * 2


def test_migrate_and_start_runs_migrations_before_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        main_module,
        "run_alembic_upgrade_with_retries",
        lambda: calls.append("migrate"),
    )
    monkeypatch.setattr(main_module, "main", lambda: calls.append("main"))

    main_module.migrate_and_start()

    assert calls == ["migrate", "main"]


def test_migration_retry_settings_read_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(main_module.MIGRATION_MAX_ATTEMPTS_ENV, "9")
    monkeypatch.setenv(main_module.MIGRATION_RETRY_DELAY_SECONDS_ENV, "1.75")

    assert main_module.get_migration_max_attempts() == 9
    assert main_module.get_migration_retry_delay_seconds() == 1.75
