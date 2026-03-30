---
name: json
description: "分类记忆存储 + 自动摘要 + 按重要性排序清理"
---

# 结构化JSON记忆系统

> 分类记忆存储 + 自动摘要 + 按重要性排序清理

**Trigger:** 记忆 memory 记住 习惯 偏好 学习

## Interface

**Signature:** `save_memory(content, category, importance) / recall_memory(query)`

**Input:** 内容字符串 + 分类 + 重要性0-1

**Output:** 记忆ID 或 匹配记忆列表

## Steps (reusable)

1. 分类存储(JSON)
2. 关键词/语义检索
3. 重要性评分
4. 超限自动清理低分条目

## Task-specific notes

- /memory命令查看编辑
- 对话结束自动摘要

## Template

```python
# action_memory.json structure
{'entries': [{'id': ..., 'content': ..., 'category': ..., 'importance': 0-1, 'ts': ...}]}
```

## Key Decisions

- JSON比MD更易结构化查询
- 重要性分数支持自动清理

## Files Created

- `action_memory.json`

**Keywords:** 记忆, memory, 存储, recall, 摘要, action_memory, 分类

---
*Auto-synthesized from skill `sk_seed_memory_system` on 2026-03-29 07:59*
