"""
Teacher Manager Loop - 双Agent对话
Teacher讲解 -> Manager审查 -> loop直到满意
使用 claude_agent_sdk (CLI订阅，无需API key)
"""

import anyio
import json
import re
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage

MODEL = "claude-opus-4-6"


async def teacher_agent(topic: str, feedback: str = None) -> str:
    prompt = f"主题: {topic}"
    if feedback:
        prompt += f"\n\nManager反馈: {feedback}\n请根据反馈改进你的讲解。"

    result = ""
    async for msg in query(
        prompt=prompt,
        options=ClaudeAgentOptions(
            model=MODEL,
            system_prompt="你是专业老师(Teacher)。简洁清晰讲解知识点，用例子说明。",
            max_turns=1,
        )
    ):
        if isinstance(msg, ResultMessage):
            result = msg.result
    return result


async def manager_agent(teacher_output: str, topic: str) -> tuple[str, bool]:
    prompt = f"""主题: {topic}

Teacher回答:
{teacher_output}

审查Teacher的回答质量。回复纯JSON格式:
{{"satisfied": true/false, "feedback": "反馈内容"}}

satisfied=true表示内容质量好，loop结束。
satisfied=false表示需要改进，feedback说明改进方向。"""

    result = ""
    async for msg in query(
        prompt=prompt,
        options=ClaudeAgentOptions(
            model=MODEL,
            system_prompt="你是Manager。严格审查内容质量，只回复JSON，不要其他文字。",
            max_turns=1,
        )
    ):
        if isinstance(msg, ResultMessage):
            result = msg.result

    # 解析JSON
    match = re.search(r'\{.*\}', result, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group())
            return data.get("feedback", ""), data.get("satisfied", False)
        except json.JSONDecodeError:
            pass
    return result, False


async def run_loop(topic: str, max_rounds: int = 3):
    print(f"\n{'='*50}")
    print(f"Topic: {topic}")
    print(f"{'='*50}\n")

    feedback = None
    teacher_output = ""

    for round_num in range(1, max_rounds + 1):
        print(f"--- Round {round_num}/{max_rounds} ---")

        print("Teacher thinking...")
        teacher_output = await teacher_agent(topic, feedback)
        print(f"[Teacher]\n{teacher_output}\n")

        print("Manager reviewing...")
        feedback, satisfied = await manager_agent(teacher_output, topic)
        print(f"[Manager] satisfied={satisfied}, feedback={feedback}\n")

        if satisfied:
            print(f"Loop done in {round_num} round(s)")
            break
        if round_num < max_rounds:
            print("Continuing...\n")

    return teacher_output


async def main():
    topic = "用简单例子解释递归(recursion)"
    final = await run_loop(topic, max_rounds=3)
    print(f"\n=== Final Output ===\n{final}")

anyio.run(main)
