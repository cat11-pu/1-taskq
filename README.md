# taskq

SQLite 后端的最小任务队列。支持入队、按 FIFO 领取、完成、失败、统计。

## 用法

    python3 cli.py enqueue  --queue mail --payload '{"to": "a@b"}'
    python3 cli.py dequeue  --queue mail
    python3 cli.py complete --id 1
    python3 cli.py fail     --id 2 --error "smtp 550"
    python3 cli.py stats    --queue mail
    python3 -m unittest
