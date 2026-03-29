# Telegram Bot API 操作手册

## 认证/配置
- **Bot Token**: 格式 `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`
- **获取方式**: 与 @BotFather 对话获取
- 必要配置: `TELEGRAM_BOT_TOKEN` 环境变量或直接传入

## 核心API/接口

### getUpdates (长轮询)
```python
from telegram import Update
from telegram.ext import Updater

updater = Updater(token='YOUR_BOT_TOKEN')
dispatcher = updater.dispatcher

# 长轮询获取更新
updater.start_polling()
```
**参数**: `offset`, `limit`, `timeout`  
**返回**: `Update[]` 对象数组

### sendMessage
```python
context.bot.send_message(
    chat_id=chat_id,
    text='Hello',
    parse_mode='HTML',  # 或 'Markdown'
    reply_markup=reply_markup  # 可选键盘
)
```
**必填**: `chat_id`, `text`  
**返回**: `Message` 对象

### editMessageText
```python
context.bot.edit_message_text(
    chat_id=chat_id,
    message_id=message_id,
    text='Updated'
)
```

### Webhook (推荐生产环境)
```python
updater.start_webhook(
    listen='0.0.0.0',
    port=8443,
    url_path='YOUR_TOKEN',
    cert='cert.pem',
    key='key.key'
)
```

## 常见操作模式

### 1. 回复消息
```python
def handle_message(update, context):
    update.message.reply_text('回复文本')
    # 或带键盘
    update.message.reply_text(
        '选择',
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton('按钮', callback_data='btn1')
        ]])
    )
```

### 2. 处理按钮回调
```python
def button_callback(update, context):
    query = update.callback_query
    query.answer()  # 关闭loading
    query.edit_message_text('已更新')

dispatcher.add_handler(CallbackQueryHandler(button_callback))
```

### 3. 上传/下载文件
```python
# 下载
file = context.bot.get_file(file_id)
file.download('local_path')

# 上传
with open('photo.jpg', 'rb') as f:
    context.bot.send_photo(chat_id, f)
```

### 4. 处理多种消息类型
```python
dispatcher.add_handler(MessageHandler(Filters.text, handle_text))
dispatcher.add_handler(MessageHandler(Filters.photo, handle_photo))
dispatcher.add_handler(MessageHandler(Filters.command, handle_command))
```

### 5. 错误处理
```python
def error_handler(update, context):
    logger.warning(f'Error: {context.error}')

dispatcher.add_error_handler(error_handler)
```

## 常见错误和处理

| 错误 | 原因 | 解决 |
|------|------|------|
| `Unauthorized` | Token 无效 | 检查 Token 格式和权限 |
| `Bad Request: chat_id_invalid` | chat_id 格式错误 | 确保是整数或有效的用户名 |
| `Too Many Requests` | 触发限速 | 添加延迟或队列机制 |
| `Message is not modified` | 编辑内容相同 | 检查新旧内容是否真的不同 |
| `Webhook url not accessible` | Webhook 地址无法访问 | 确保服务器公网IP、端口开放、证书有效 |

## 注意事项

- **限速**: Bot 限制 ~30 msg/sec；群组 ~40 msg/min
- **超时**: 长轮询默认 `timeout=30s`；Webhook 响应需要 5min 内返回
- **parse_mode**: `'HTML'` 支持 `<b>`, `<i>`, `<code>` 等标签
- **chat_id**: 私聊用 user_id；群组用 negative group_id
- **Webhook 优于轮询**: 更快、消耗资源少，生产环境必用
- **File API**: file_id 有时效性，及时下载；上传大文件用 `send_document` 而非 `send_photo`
- **错误重试**: 建议指数退避 + 日志记录