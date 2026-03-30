---
name: auto-skill
description: "成功任务后自动提取可复用技能到JSON+MD格式"
---

# 技能自动合成

> 成功任务后自动提取可复用技能到JSON+MD格式

**Trigger:** 自动学习 自动保存技能 skill extraction 技能合成

## Interface

**Signature:** `maybe_extract_skill(user_message, response, score)`

**Input:** 用户消息 + bot回复 + 质量分数dict

**Output:** skill_id(str) 或 None

## Steps (reusable)

1. 检测是否值得提取(分数/完成信号/代码内容)
2. 调用haiku提炼函数签名
3. 保存JSON+MD
4. 更新index

## Task-specific notes

- heuristic阈值0.45
- MD写入skills/目录

## Template

```python
if _should_extract_skill(msg, resp, score):
    data = await _run_claude_raw(extract_prompt, model='haiku')
    skill = parse_and_save(data)
    synthesize_to_md(skill)
```

## Key Decisions

- 用haiku节省token
- 阈值0.45平衡质量与覆盖率
- MD格式兼容superpowers技能系统

## Files Created

- `.skill_library/skills/*.json`
- `skills/*/SKILL.md`

**Keywords:** skill, 技能, 自动学习, 结晶, 提取, 合成, 复用

---
*Auto-synthesized from skill `sk_seed_skill_synthesis` on 2026-03-29 07:59*
