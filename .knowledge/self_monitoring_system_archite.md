# self-monitoring system architecture 操作手册

## 认证/配置
- 无需认证，均为系统库
- psutil: `pip install psutil`（跨平台进程/系统监控）
- subprocess: Python 内置，用于子进程管理
- signal: Python 内置，用于信号处理

## 核心API/接口

### subprocess 模块
```python
subprocess.Popen(args, stdin/stdout/stderr=PIPE, text=True)
# 创建子进程，返回 Popen 对象
popen.poll()  # 检查进程状态，None=运行中，返回code=已结束
popen.terminate() / popen.kill()  # 优雅终止/强制杀死
popen.communicate()  # 等待进程完成，获取输出
```

### psutil 模块
```python
psutil.Process(pid)  # 获取进程对象
process.status()  # 'running', 'sleeping', 'zombie'等
process.cpu_percent(interval=1)  # CPU占用率 0-100
process.memory_info().rss  # 内存占用（字节）
process.children()  # 子进程列表
process.terminate() / process.kill()  # 终止进程
psutil.virtual_memory()  # 系统内存统计
psutil.cpu_percent()  # 系统CPU占用率
```

### signal 模块
```python
signal.signal(signal.SIGTERM, handler_func)  # 注册信号处理器
signal.SIGTERM  # 优雅终止信号
signal.SIGKILL  # 强制杀死信号（无法捕获）
```

## 常见操作模式

### 1. 启动和监控子进程
```python
import subprocess
proc = subprocess.Popen(['python', 'worker.py'], stdout=subprocess.PIPE)
returncode = proc.poll()  # 定期检查状态
if returncode is None:  # 进程仍运行
    # 监控中...
```

### 2. 获取进程资源占用
```python
import psutil
p = psutil.Process(pid)
cpu = p.cpu_percent(interval=1)  # 最近1秒CPU
mem = p.memory_info().rss / (1024**2)  # MB
print(f"CPU: {cpu}%, Memory: {mem}MB")
```

### 3. 优雅关闭进程树
```python
import psutil
def kill_process_tree(pid):
    p = psutil.Process(pid)
    children = p.children(recursive=True)
    for child in children:
        try:
            child.terminate()
        except:
            pass
    p.terminate()
    psutil.wait_procs([p, *children], timeout=3)
```

### 4. 信号处理
```python
import signal
def cleanup(signum, frame):
    print("Shutting down...")
    exit(0)
signal.signal(signal.SIGTERM, cleanup)
```

### 5. 系统级监控
```python
import psutil
mem = psutil.virtual_memory()
print(f"内存: {mem.percent}% | {mem.available / (1024**3):.1f}GB可用")
cpu = psutil.cpu_percent(interval=1)
print(f"CPU: {cpu}%")
```

## 常见错误和处理

| 错误 | 原因 | 解决方案 |
|------|------|--------|
| `psutil.NoSuchProcess` | 进程已结束 | 先用 `poll()` 检查状态 |
| `PermissionError` | 无权限操作他人进程 | 检查进程owner，提升权限 |
| `subprocess.TimeoutExpired` | 进程超时未结束 | 设置 `timeout` 参数，or手动 `terminate()` |
| `OSError: [Errno 24] Too many open files` | 文件描述符泄漏 | 显式 `close()` pipe，使用上下文管理器 |
| 僵尸进程（Zombie） | 父进程未调用 `wait()` | 使用 `communicate()`或定期 `poll()` |

## 注意事项

- **跨平台差异**：Windows下无 `SIGKILL`，改用 `process.kill()`
- **资源泄漏**：subprocess pipe 必须显式关闭，否则文件描述符泄漏
- **信号安全**：signal handler 内只能调用 async-safe 函数（如 `os._exit()`），避免锁/网络IO
- **权限**：监控其他用户进程需要提升权限（Linux需sudo，Windows需Admin）
- **CPU采样间隔**：`cpu_percent(interval=1)` 首次调用会阻塞1秒初始化，后续快速
- **Windows进程树**：Windows下 `children()` 可能不完整，推荐使用WMI辅助
- **重复信号**：同一信号多次触发会丢弃（不队列化），设计时假设信号幂等