import argparse
import sys
import os
import importlib

from .worker import run_worker
from .inspect import inspect
from .retry import retry
from .purge import purge
from .cancel import cancel
from .failed import list_failed
from .stats import stats


def main():
    parser = argparse.ArgumentParser("pybgworker")

    parser.add_argument(
        "command",
        choices=["run", "inspect", "retry", "purge", "cancel", "failed", "stats"],
        help="worker control commands"
    )

    parser.add_argument(
        "task_id",
        nargs="?",
        help="task id for retry/cancel"
    )

    parser.add_argument(
        "--app",
        help="module containing task definitions (required for run)"
    )

    args = parser.parse_args()

    if args.command == "run":
        if not args.app:
            parser.error("--app is required for 'run'")

        sys.path.insert(0, os.getcwd())
        importlib.import_module(args.app)
        run_worker()

    elif args.command == "inspect":
        inspect()

    elif args.command == "retry":
        if not args.task_id:
            parser.error("retry requires task_id")
        retry(args.task_id)

    elif args.command == "purge":
        purge()

    elif args.command == "cancel":
        if not args.task_id:
            parser.error("cancel requires task_id")
        cancel(args.task_id)

    elif args.command == "failed":
        list_failed()

    elif args.command == "stats":
        stats()


if __name__ == "__main__":
    main()
