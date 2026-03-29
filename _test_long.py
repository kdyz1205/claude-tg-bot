import asyncio, claude_agent

async def test():
    # Simple test first
    msg = "你好，告诉我现在几点"
    print(f"Test 1: simple message ({len(msg)} chars)")
    resp, sid = await claude_agent._run_claude_cli(msg, chat_id=0, context=None, timeout=30)
    print(f"  Response: {resp[:200]}")
    print(f"  Session: {sid}")

asyncio.run(test())
