---
name: flask
description: "实时web dashboard展示bot运行状态(端口8080)"
---

# Flask性能监控仪表盘

> 实时web dashboard展示bot运行状态(端口8080)

**Trigger:** dashboard 仪表盘 监控 性能 flask web

## Interface

**Signature:** `start_dashboard(port=8080)`

**Input:** 端口号

**Output:** http://localhost:8080 实时监控页面

## Steps (reusable)

1. Flask路由/
2. stats_collector收集指标
3. JS定时刷新
4. /dashboard命令截图发TG

## Task-specific notes

- 消息处理速度/成功率/错误率/内存/运行时间
- 最近10条执行日志

## Template

```python
from flask import Flask
app = Flask(__name__)
@app.route('/')
def index(): return render_template('dashboard.html', stats=get_stats())
Thread(target=app.run, kwargs={'port':8080}, daemon=True).start()
```

## Key Decisions

- daemon线程不阻塞bot主进程
- 截图发TG而非打开浏览器

## Files Created

- `dashboard.py`

**Keywords:** dashboard, flask, 监控, 性能, web, 8080, 实时

---
*Auto-synthesized from skill `sk_seed_dashboard` on 2026-03-29 07:59*
