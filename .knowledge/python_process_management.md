# Python process management 操作手册

## 认证/配置
- 无需认证，标准库内置
- 必要导入：`subprocess`, `signal`, `os`, `psutil`
- 权限：需要操作系统级进程权限

## 核心API/接口

### subprocess.Popen
```python
Popen(args, stdin=None, stdout=None, stderr=None, shell=False, cwd=None)
# 返回: Popen对象，属性: pid, returncode, stdin, stdout, stderr
```

### subprocess.run (推荐)
```python
run(args, capture_output=False, timeout=None, shell=False, check=False)
# 返回: CompletedProcess(args, returncode, stdout, stderr)
```

### signal 信号处理
```python
signal.signal(signalnum, handler)  # 注册信号处理器
os.kill(pid, signal.SIGTERM)  # 发送信号
os.killpg(pgid, signal.SIGKILL)  # 杀死进程组
```

### psutil 进程监控
```python
psutil.Process(pid)  # 获取进程对象
process.terminate()  # 温和终止
process.kill()  # 强制杀死
process.cpu_percent(interval=0.1)  # CPU使用率
process.memory_info()  # 内存信息
```

## 常见操作模式

### 1. 执行命令并获取输出
```python
result = subprocess.run(['ls', '-la'], capture_output=True, text=True, check=True)
print(result.stdout)
```

### 2. 后台进程 + 等待完成
```python
proc = subprocess.Popen(['python', 'script.py'])
returncode = proc.wait()  # 阻塞直到完成
# 或: stdout, stderr = proc.communicate(timeout=30)
```

### 3. 进程超时处理
```python
try:
    result = subprocess.run(cmd, timeout=5, check=True)
except subprocess.TimeoutExpired:
    # 超时处理
    pass
```

### 4. 安全杀死进程
```python
proc = subprocess.Popen(cmd)
try:
    proc.wait(timeout=5)
except subprocess.TimeoutExpired:
    proc.terminate()  # 先温和终止
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()  # 再强制杀死
```

### 5. 进程监控与资源检查
```python
import psutil
p = psutil.Process(pid)
if p.is_running():
    print(f"CPU: {p.cpu_percent()}%, 内存: {p.memory_info().rss / 1024**2:.1f}MB")
```

## 常见错误和处理

| 错误 | 原因 | 解决方案 |
|------|------|--------|
| `FileNotFoundError` | 命令不存在 | 检查 `PATH` 或使用绝对路径 |
| `TimeoutExpired` | 进程超时 | 使用 `terminate()` 或 `kill()` 清理 |
| `PermissionError` | 权限不足 | 检查进程所有者，必要时提升权限 |
| 僵尸进程 | 未调用 `wait()/communicate()` | 必须等待子进程完成 |
| 管道阻塞 | stdout/stderr缓冲满 | 使用 `communicate()` 或 `PIPE` 读取 |

## 注意事项

- **shell=True风险**：避免用户输入直接拼接，易引入命令注入；若必须用shell，使用`shlex.quote()`转义
- **进程泄漏**：总是调用 `wait()` 或 `communicate()`，否则产生僵尸进程
- **跨平台信号**：`SIGTERM` / `SIGKILL` 在Windows不同；Windows推荐用 `terminate()` 而非信号
- **阻塞陷阱**：大量stdout/stderr会导致管道阻塞，使用 `communicate()` 或 `capture_output=True`
- **工作目录**：`cwd` 参数设置子进程工作目录，相对路径相对于当前目录
- **环境变量**：修改 `env` 参数时，需显式继承 `os.environ`（`env={**os.environ, 'KEY': 'val'}`）