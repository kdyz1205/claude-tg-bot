# Race conditions and concurrency 操作手册

## 认证/配置
- **Telegram Bot API**: Token-based auth via `https://api.telegram.org/bot{TOKEN}/method`
- **Anthropic API**: API key in `ANTHROPIC_API_KEY` environment variable
- **Chrome DevTools Protocol**: Local WebSocket connection (no auth, default: `ws://localhost:9222`)

## 核心API/接口

### Telegram Bot API (并发限制)
```python
# 单消息发送
POST /sendMessage
{"chat_id": 123, "text": "message"}

# 重要: 避免快速连续发送到同一chat_id
# Rate limit: ~30 msg/sec per bot globally
```

### Anthropic API (流式响应)
```python
from anthropic import Anthropic
client = Anthropic(api_key="sk-...")

# 使用流式避免超时
with client.messages.stream(
    model="claude-opus-4-6",
    max_tokens=1024,
    messages=[{"role": "user", "content": "..."}]
) as stream:
    for text in stream.text_stream:
        # 增量处理文本，避免内存堆积
        process(text)
```

### Chrome DevTools Protocol (并发命令)
```javascript
// 在同一连接上序列化命令，避免race condition
websocket.send(JSON.stringify({
  id: incrementing_counter,
  method: "Runtime.evaluate",
  params: {expression: "..."}
}));

// 通过ID匹配响应
on_message: match_by_id(response.id)
```

## 常见操作模式

**1. Telegram消息队列（防限速）**
```python
from asyncio import Queue
queue = Queue()
async def send_worker():
    while True:
        msg = await queue.get()
        await bot.send_message(msg['chat_id'], msg['text'])
        await asyncio.sleep(0.05)  # 20 msg/sec limit

# 发送端: await queue.put(msg)
```

**2. Anthropic并发请求（共享client）**
```python
# ✅ 正确: 单client实例，多任务共享
client = Anthropic()
tasks = [
    asyncio.create_task(client.messages.create(...))
    for _ in range(5)
]
results = await asyncio.gather(*tasks)

# ❌ 错误: 每个请求创建新client（资源泄漏）
```

**3. CDP命令序列化**
```python
class CDPSequencer:
    def __init__(self):
        self.counter = 0
        self.pending = {}
    
    def send(self, method, params):
        self.counter += 1
        msg = {"id": self.counter, "method": method, "params": params}
        self.pending[self.counter] = asyncio.Future()
        ws.send(json.dumps(msg))
        return self.pending[self.counter]
    
    def on_response(self, resp):
        future = self.pending.pop(resp['id'])
        future.set_result(resp.get('result'))
```

## 常见错误和处理

| 错误 | 原因 | 解决方案 |
|------|------|---------|
| `429 Too Many Requests` | Telegram限速 | 实现指数退避 + 消息队列 |
| `Connection timeout` | Anthropic超时 | 使用流式响应 + timeout参数 |
| `CDP: Unknown id` | 响应乱序 | 用ID严格匹配请求/响应对 |
| `Database locked` | SQLite并发写 | 使用WAL模式或单写线程 |
| `Websocket closed` | Chrome崩溃 | 检测断连 → 重新启动 + 重连 |

## 注意事项

- **Telegram**: 避免在同一`chat_id`快速发消息，用队列隔离
- **Anthropic**: Client可复用，但`model`和`max_tokens`必须显式指定
- **CDP**: WebSocket连接是单线程的，所有命令必须序列化；保持心跳(`Runtime.awaitPromise`)防断连
- **数据竞争**: 使用`asyncio.Lock()`保护共享state，避免`await`跨临界区
- **错误恢复**: 实现3层重试（瞬时错误→指数退避，连接错误→断路器，业务错误→死信队列）