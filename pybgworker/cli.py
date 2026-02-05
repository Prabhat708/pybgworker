import argparse
import sys
import os
import importlib


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
    parser.add_argument(
        "--concurrency",
        type=int,
        help="number of tasks to run in parallel per worker"
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        help="days to retain finished tasks before cleanup"
    )
    parser.add_argument(
        "--cleanup-interval-hours",
        type=int,
        help="hours between automatic cleanup runs"
    )
    parser.add_argument(
        "--cleanup-interval-minutes",
        type=int,
        help="minutes between automatic cleanup runs"
    )

    args = parser.parse_args()

    if args.concurrency is not None:
        os.environ["PYBGWORKER_CONCURRENCY"] = str(args.concurrency)

    if args.retention_days is not None:
        os.environ["PYBGWORKER_RETENTION_DAYS"] = str(args.retention_days)

    if args.cleanup_interval_hours is not None:
        os.environ["PYBGWORKER_CLEANUP_INTERVAL_HOURS"] = str(
            args.cleanup_interval_hours
        )
    if args.cleanup_interval_minutes is not None:
        os.environ["PYBGWORKER_CLEANUP_INTERVAL_MINUTES"] = str(
            args.cleanup_interval_minutes
        )

    if args.command == "run":
        if not args.app:
            parser.error("--app is required for 'run'")

        sys.path.insert(0, os.getcwd())
        importlib.import_module(args.app)
        from .worker import run_worker
        run_worker()

    elif args.command == "inspect":
        from .inspect import inspect
        inspect()

    elif args.command == "retry":
        if not args.task_id:
            parser.error("retry requires task_id")
        from .retry import retry
        retry(args.task_id)

    elif args.command == "purge":
        from .purge import purge
        purge()

    elif args.command == "cancel":
        if not args.task_id:
            parser.error("cancel requires task_id")
        from .cancel import cancel
        cancel(args.task_id)

    elif args.command == "failed":
        from .failed import list_failed
        list_failed()

    elif args.command == "stats":
        from .stats import stats
        stats()


if __name__ == "__main__":
    main()
