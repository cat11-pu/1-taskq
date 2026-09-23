"""CLI: python3 -m raftlite run sample/scenario.json [--json]"""

import argparse
import json
import sys

from .runner import build_report, format_text, run_scenario


def main(argv=None):
    parser = argparse.ArgumentParser(prog="raftlite")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run a scenario script")
    run.add_argument("scenario", help="path to scenario JSON")
    run.add_argument("--json", action="store_true", help="structured JSON output")
    args = parser.parse_args(argv)

    cluster, commands = run_scenario(args.scenario)
    report = build_report(cluster, commands)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(format_text(report))
    ok = report["committed"] == report["commands"] and report["leaders"] == 1
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
