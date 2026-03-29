# Python async programming 操作手册

## 认证/配置
- **asyncio 初始化**: `asyncio.run(main())` 或 `loop = asyncio.get_event_loop()`
- **事件循环**: 在异步函数中使用 `asyncio.get_running_loop()`
- **超时配置**: `asyncio.timeout(timeout_seconds)` 或 `asyncio.wait_for(coro, timeout)`

## 核心API/接口

### 协程创建与执行
```python
async def my_async_func(param):
    result = await some_async_operation()
    return result

# 执行
asyncio.run(my_async_func("value"))
```

### 并发执行
```python
# 多个协程并发
results = await asyncio.gather(coro1(), coro2(), coro3())

# 竞速 - 返回首个完成的
done, pending = await asyncio.wait([coro1(), coro2()], return_when=asyncio.FIRST_COMPLETED)

# 顺序等待
await asyncio.sleep(seconds)
```

### 任务创建
```python
task = asyncio.create_task(my_async_func())
result = await task
```

### HTTP 请求（aiohttp）
```python
async with aiohttp.ClientSession() as session:
    async with session.get(url) as resp:
        data = await resp.json()
```

## 常见操作模式

**1. 并发HTTP请求**
```python
async def fetch_multiple(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [session.get(url) for url in urls]
        responses = await asyncio.gather(*tasks)
        return [await r.json() for r in responses]
```

**2. 超时处理**
```python
try:
    result = await asyncio.wait_for(slow_operation(), timeout=5.0)
except asyncio.TimeoutError:
    print("Operation timed out")
```

**3. 后台任务**
```python
async def main():
    task = asyncio.create_task(background_worker())
    # 主程序继续
    await main_logic()
    await task  # 等待后台任务完成
```

**4. 事件循环内调用**
```python
loop = asyncio.get_event_loop()
future = loop.create_task(async_func())
```

**5. Telegram Bot（python-telegram-bot）**
```python
from telegram.ext import Application, CommandHandler

app = Application.builder().token("TOKEN").build()
app.add_handler(CommandHandler("start", start))
await app.run_polling()
```

## 常见错误和处理

| 错误 | 原因 | 解决方案 |
|------|------|--------|
| `RuntimeError: no running event loop` | 在非异步上下文调用await | 使用 `asyncio.run()` 或确保在async函数内 |
| `asyncio.TimeoutError` | 操作超时 | 使用 `asyncio.wait_for()` 设置timeout |
| `RuntimeError: gather() requires at least one argument` | gather()无参数 | 检查task列表是否为空 |
| `TypeError: object is not iterable` | await错误的对象 | 确保对象是协程或Future |
| `ConnectionError` | 网络连接失败 | 使用重试机制 + exponential backoff |

## 注意事项

- **事件循环唯一性**: 每个线程只能有一个运行中的事件循环
- **阻塞操作**: 避免在async函数中使用同步阻塞操作（如 `time.sleep()`），使用 `await asyncio.sleep()` 替代
- **异常处理**: `asyncio.gather()` 默认遇到异常会抛出，使用 `return_exceptions=True` 收集所有结果
- **Telegram API限速**: 30条消息/秒，使用 `asyncio.sleep()` 控制频率
- **WebSocket连接**: 需要 `aiohttp` 或 `websockets` 库，保持 `async with` 上下文
- **任务取消**: 使用 `task.cancel()` 然后 `await asyncio.CancelledError` 捕获
- **调试**: 启用 `asyncio.run(debug=True)` 检测未await的协程