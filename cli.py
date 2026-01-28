import argparse
from .worker import run_worker

def main():
    parser = argparse.ArgumentParser("pyworker")
    parser.add_argument("command", choices=["run"])
    args = parser.parse_args()

    if args.command == "run":
        run_worker()

if __name__ == "__main__":
    main()
