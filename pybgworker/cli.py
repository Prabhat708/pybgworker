import argparse
import sys
import os
import importlib
from .worker import run_worker

def main():
    parser = argparse.ArgumentParser("pybgworker")
    parser.add_argument("command", choices=["run"])
    parser.add_argument("--app", required=True)
    args = parser.parse_args()

    sys.path.insert(0, os.getcwd())
    importlib.import_module(args.app)

    if args.command == "run":
        run_worker()

if __name__ == "__main__":
    main()
