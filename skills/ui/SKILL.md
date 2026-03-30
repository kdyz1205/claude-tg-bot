---
name: ui
description: "SOM截图标注 + fallback链点击UI元素"
---

# 视觉UI定位点击

> SOM截图标注 + fallback链点击UI元素

**Trigger:** 点击元素 UI自动化 截图定位 som click

## Interface

**Signature:** `ui_click_element(name_or_idx, fuzzy=True)`

**Input:** 元素名称(模糊匹配) 或 SOM标注序号

**Output:** click成功/失败

## Steps (reusable)

1. som_screenshot标注元素
2. fuzzy名称匹配
3. 依次尝试browser_click→ui_click→som_click→smartclick

## Task-specific notes

- 不区分大小写部分匹配
- Windows UI Automation树

## Template

```python
# fallback chain
for method in [browser_click, ui_click, som_click, smartclick]:
    try:
        if method(target): return True
    except: continue
return False
```

## Key Decisions

- fallback链保证可靠性
- SOM标注用数字序号便于LLM引用

**Keywords:** som, ui_click, 截图, 点击, 定位, 元素, fallback

---
*Auto-synthesized from skill `sk_seed_visual_ui` on 2026-03-29 07:59*
