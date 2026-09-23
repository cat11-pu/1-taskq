"""taskq 命令行入口。"""
from __future__ import annotations

import argparse
import json
import sys

from taskq import TaskQueue


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="taskq", description="最小任务队列")
    p.add_argument("--db", default="taskq.db", help="SQLite 文件路径")
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("enqueue", help="入队")
    e.add_argument("--queue", required=True)
    e.add_argument("--payload", required=True, help="JSON 字符串")

    d = sub.add_parser("dequeue", help="领取一条")
    d.add_argument("--queue", required=True)

    c = sub.add_parser("complete", help="标记完成")
    c.add_argument("--id", type=int, required=True)

    f = sub.add_parser("fail", help="标记失败")
    f.add_argument("--id", type=int, required=True)
    f.add_argument("--error", default="")

    s = sub.add_parser("stats", help="统计")
    s.add_argument("--queue", default=None)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    q = TaskQueue(args.db)
    if args.cmd == "enqueue":
        print(q.enqueue(args.queue, json.loads(args.payload)))
    elif args.cmd == "dequeue":
        print(json.dumps(q.dequeue(args.queue), ensure_ascii=False))
    elif args.cmd == "complete":
        print(q.complete(args.id))
    elif args.cmd == "fail":
        print(q.fail(args.id, args.error))
    elif args.cmd == "stats":
        print(json.dumps(q.stats(args.queue), ensure_ascii=False))
    q.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
