import os

import pybgworker.cli as cli


def test_cli_concurrency_overrides_env(monkeypatch):
    monkeypatch.setenv("PYBGWORKER_CONCURRENCY", "1")

    monkeypatch.setattr(cli, "run_worker", lambda: None)
    monkeypatch.setattr(cli.importlib, "import_module", lambda *_args, **_kwargs: None)

    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["pybgworker", "run", "--app", "example", "--concurrency", "3"],
    )

    cli.main()

    assert os.environ["PYBGWORKER_CONCURRENCY"] == "3"
