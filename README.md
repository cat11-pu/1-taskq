# taskq

SQLite 后端的最小任务队列。支持入队、按 FIFO 领取、完成、失败、统计。

## 用法

    python3 cli.py enqueue  --queue mail --payload '{"to": "a@b"}'
    python3 cli.py dequeue  --queue mail
    python3 cli.py complete --id 1
    python3 cli.py fail     --id 2 --error "smtp 550"
    python3 cli.py stats    --queue mail
    python3 -m unittest

# raftlite

最小 Raft 共识实现（纯 Python 标准库）：选主、心跳、日志复制与提交、崩溃安全持久化、确定性集群模拟。

## 用法

    python3 -m raftlite run sample/scenario.json          # 跑场景脚本，输出验收指标
    python3 -m raftlite run sample/scenario.json --json   # 结构化 JSON 输出
    python3 -m unittest discover -s tests -v

## 接口

- `Cluster(ids)`：确定性集群，`step(n)` 按固定轮次推进，`set_partition(groups)` / `heal()` 模拟分区与恢复，`summarize()` 输出轮次、各节点 term/state/日志长度与 leader。
- `Node.append(cmd)`：仅 leader 接受客户端命令，否则抛 `NotLeaderError`；`Node.committed()` 返回已提交命令列表。
- `Node.append_entries(term, leader, prev_log_index, prev_log_term, entries, leader_commit)`：跟随者校验 prev 一致性，冲突时截断并重同步；leader 收到多数派确认后推进 `commit_index`。
- `Node.save(path)` / `Node.load(path)`：term、voted_for、日志落盘；先写临时文件再 `os.replace` 原子替换，崩溃不产生半个文件。
