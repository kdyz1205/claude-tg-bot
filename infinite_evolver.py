"""
Infinite Evolver - 永续进化引擎
目标：持续自我进化，直到达到：自主操作 + 高度智能 + 赚钱能力

Phase 2+ 任务由AI动态生成，无限循环。
每轮完成 → 评估状态 → 生成下一批任务 → 继续进化
"""
import subprocess
import time
import os
import sys
import json
import logging
import re
import shutil
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID = os.getenv('AUTHORIZED_USER_ID')
BASE = Path(__file__).parent
STATE_FILE = BASE / "_infinite_evolver_state.json"
LOG_FILE = BASE / "_infinite_evolver.log"
LOCK_FILE = BASE / "_infinite_evolver.lock"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("infinite_evolver")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Phase 2 任务队列：自主 + 智能 + 赚钱 ────────────────────────────────────

PHASE2_TASKS = [
    {
        "id": "p2_1",
        "name": "自主市场监控",
        "goal": "autonomous_monitoring",
        "prompt": """【进化 Phase2-1: 自主市场监控】
无需用户触发，bot 自动监控加密市场，主动发出信号。

目标：
1. 创建 proactive_monitor.py：每5分钟检查 BTC/ETH/SOL 价格（用 OKX API）
2. 检测条件：价格突破24h高低点 → 自动发 TG 通知
3. 检测条件：价格变化 >3% 在1小时内 → 发警报
4. 整合到 run.py 作为后台线程启动
5. 加 /monitor on|off 命令控制开关

⚠️ 安全：不要杀进程，不要删关键文件，修改前先备份
完成后输出 ✅Phase2-1完成"""
    },
    {
        "id": "p2_2",
        "name": "智能交易信号生成",
        "goal": "trading_signals",
        "prompt": """【进化 Phase2-2: 智能交易信号生成】
基于技术分析自动生成买卖信号，发送给用户。

目标：
1. 创建 signal_engine.py：实现 RSI + MACD + MA均线 信号判断
2. 每15分钟扫描 TOP20 加密货币（OKX数据）
3. 检测到强烈买入/卖出信号 → 发送格式化 TG 消息
4. 消息格式：代币/方向/信号强度/建议入场价/止损位
5. 加 /signals 命令：查看今日所有信号汇总
6. 记录信号历史到 _signal_history.json，用于后续胜率统计

⚠️ 安全：只做分析不做实际交易，先问用户再执行任何资金操作
完成后输出 ✅Phase2-2完成"""
    },
    {
        "id": "p2_3",
        "name": "自主任务规划器",
        "goal": "autonomous_planning",
        "prompt": """【进化 Phase2-3: 自主任务规划器】
让 bot 能把复杂目标拆解成子任务并自主执行，无需用户每步指导。

目标：
1. 创建 task_planner.py：接收高层目标 → 用 Claude 分解为步骤 → 顺序执行
2. 支持长任务（>30分钟）自动分段，保存中间状态到 _plan_state.json
3. 每完成一个子任务 → TG 通知进度
4. 加 /plan "目标" 命令：触发自主规划执行
5. 失败子任务自动重试3次，全部失败才报告给用户
6. 规划历史记录到 _plan_history.json，学习成功模式

完成后输出 ✅Phase2-3完成"""
    },
    {
        "id": "p2_4",
        "name": "收益追踪与报告",
        "goal": "profit_tracking",
        "prompt": """【进化 Phase2-4: 收益追踪与报告】
追踪所有发出信号的表现，计算胜率，每日自动报告。

目标：
1. 创建 profit_tracker.py：读取 _signal_history.json，追踪每个信号后续价格
2. 计算：胜率 / 平均收益率 / 最佳信号 / 最差信号
3. 每天09:00自动发送日报到 TG：昨日信号表现 + 本周累计
4. 加 /report 命令：立即生成并发送报告
5. 用图表（matplotlib）可视化胜率趋势，发图片到 TG
6. 保存统计到 _performance_stats.json，供自我优化使用

完成后输出 ✅Phase2-4完成"""
    },
    {
        "id": "p2_5",
        "name": "自我优化信号策略",
        "goal": "self_optimize_strategy",
        "prompt": """【进化 Phase2-5: 自我优化信号策略】
基于历史胜率数据，自动调整信号参数，让策略越来越准。

目标：
1. 创建 strategy_optimizer.py：读取 _performance_stats.json
2. 分析哪些参数组合（RSI阈值/时间框架/代币）胜率最高
3. 每周自动调整 signal_engine.py 的参数到最优值
4. 记录每次调整到 _optimization_log.json
5. 如调整后胜率下降 → 自动回滚到上一版本
6. 加 /optimize 命令：手动触发优化

完成后输出 ✅Phase2-5完成"""
    },
    {
        "id": "p2_6",
        "name": "自然语言升级：多意图理解",
        "goal": "nlp_upgrade",
        "prompt": """【进化 Phase2-6: 多意图NLP升级】
让 bot 能理解复杂、含糊、多意图的消息，像真正的助手一样。

目标：
1. 升级 claude_agent.py：检测消息中的多个意图，并行执行
2. 支持上下文记忆：记住对话中提到的代币/项目，后续消息自动关联
3. 支持模糊指令：「帮我看看那个solana代币」→ 自动找最近分析过的代币
4. 加意图置信度：不确定时列出2-3个可能理解，让用户确认（但只在真正模糊时）
5. 加主动建议：分析完一个代币后，自动建议「要不要看看相关代币」
6. 测试10种复杂消息场景，记录准确率

完成后输出 ✅Phase2-6完成"""
    },
    {
        "id": "p2_7",
        "name": "完全自主操作模式",
        "goal": "full_autonomy",
        "prompt": """【进化 Phase2-7: 完全自主操作模式】
实现 bot 在用户不在线时自主运行，发现机会主动行动。

目标：
1. 创建 autonomous_mode.py：用户不活跃 >30分钟 → 进入自主模式
2. 自主模式做：市场扫描 + 代码自检 + 性能优化 + 进化任务
3. 用户回来时：发送自主期间做了什么的摘要报告
4. 加优先级队列：高价值信号 > 代码进化 > 常规检查
5. 每天深夜自动运行：压力测试自身代码 + 修复发现的bug
6. 最终目标：bot能独立运行7天不需要用户介入

完成后输出 ✅Phase2-7完成 - 接近自主运营目标！"""
    },
]

# ── Phase 3 会在 Phase 2 完成后由 AI 动态生成 ─────────────────────────────

PHASE3_SEED_PROMPT = """
你是一个正在进化的AI助手bot，已完成以下能力：
- 自主市场监控 + 交易信号生成
- 自主任务规划 + 执行
- 收益追踪 + 策略自优化
- 高级NLP + 完全自主操作模式

现在需要你生成 Phase 3 进化任务，推动实现：
1. 更强的赚钱能力（更准的信号、更多市场覆盖）
2. 更高的自主性（自我修复、自我学习、自我改进代码）
3. 更广的能力（多平台、更多工具、更复杂任务）

生成5个具体可执行的进化任务，JSON格式：
[
  {"id": "p3_1", "name": "任务名", "goal": "goal_key", "prompt": "详细实现指令..."},
  ...
]

只输出JSON，不要其他文字。
"""

SAFETY_RULES = """⚠️ 绝对安全规则（必须遵守）：
- 不要杀死任何 python 进程（bot.py、run.py）
- 不要删除 .env、.bot.pid、.bot.lock、_evolution_queue.json
- 修改代码前先用 python -m py_compile 验证语法
- 不要修改网络/防火墙设置

"""


def find_claude():
    """Find claude.cmd on Windows or claude on other platforms."""
    for c in [
        shutil.which("claude.cmd"),
        shutil.which("claude"),
        str(Path.home() / "AppData/Roaming/npm/claude.cmd"),
        str(Path.home() / "AppData/Local/Programs/claude/claude.cmd"),
        str(Path.home() / ".claude/local/claude.cmd"),
    ]:
        if c and Path(c).is_file():
            return c
    return "claude.cmd"  # fallback


def tg(text):
    if not TOKEN or not USER_ID:
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": USER_ID, "text": text[:4096]},
            timeout=10
        )
    except Exception as e:
        log.warning(f"TG失败: {e}")


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, IOError, OSError):
            pass
    return {
        "phase": 2,
        "task_index": 0,
        "completed_tasks": [],
        "failed_tasks": [],
        "total_runs": 0,
        "consecutive_failures": 0,
        "started": datetime.now().isoformat(),
        "last_update": datetime.now().isoformat(),
    }


def save_state(state):
    state["last_update"] = datetime.now().isoformat()
    tmp = str(STATE_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(STATE_FILE))
    except Exception as e:
        log.error(f"Save state error: {e}")
        try:
            os.unlink(tmp)
        except OSError:
            pass


def run_claude_task(prompt, timeout=600):
    """运行一个 claude 任务，返回 (success, output)"""
    full_prompt = SAFETY_RULES + prompt
    try:
        result = subprocess.run(
            [find_claude(), "-p", full_prompt,
             "--output-format", "text",
             "--dangerously-skip-permissions"],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(BASE),
            encoding="utf-8",
            errors="replace",
        )
        output = (result.stdout or "") + (result.stderr or "")
        log.info(f"Claude输出（前500字）: {output[:500]}")

        # Check for auth/credit errors
        output_lower = output.lower()
        credit_patterns = ["rate limit", "out of credits", "not logged in",
                           "quota exceeded", "429", "billing"]
        for pat in credit_patterns:
            if pat in output_lower:
                log.warning(f"Credit/auth error detected: {pat}")
                return False, output

        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        log.warning("任务超时 — 标记为失败")
        return False, "[TIMEOUT - task did not complete in time]"
    except FileNotFoundError:
        log.error("claude.cmd not found!")
        return False, "claude.cmd not found"
    except Exception as e:
        log.error(f"运行失败: {e}")
        return False, str(e)


def check_completion(output, task_id):
    """检测任务是否完成 — 需要实质性输出 + 完成标记"""
    if not output or len(output.strip()) < 50:
        return False  # Too short = no real work done

    # Check for error/failure indicators first
    output_lower = output.lower()
    fail_markers = ["error", "failed", "timeout", "not found", "exception",
                    "traceback", "rate limit", "429", "not logged in"]
    fail_count = sum(1 for m in fail_markers if m in output_lower)
    if fail_count >= 3:
        return False  # Likely failed, not completed

    # Must contain a specific completion marker (not just any word)
    specific_markers = [
        f"✅{task_id}完成",      # e.g. ✅p3_1完成
        f"✅ {task_id}完成",
        f"✅任务{task_id}完成",
        f"✅ 任务{task_id}完成",
        "task complete",
        "completed successfully",
        "all changes applied",
    ]
    for m in specific_markers:
        if m.lower() in output_lower:
            return True

    # Fallback: if output is substantial (real work was done) and contains generic ✅
    if len(output.strip()) > 500 and "✅" in output:
        return True

    return False


def generate_phase3_tasks(phase=3):
    """用 Claude 动态生成下一阶段任务 (IDs are phase-unique)"""
    log.info(f"生成 Phase {phase} 任务...")
    tg(f"🧬 Phase {phase-1} 全部完成！正在生成 Phase {phase} 任务...")

    seed_prompt = PHASE3_SEED_PROMPT + f"\n\n注意：当前是 Phase {phase}，任务ID必须用 p{phase}_1, p{phase}_2 ... 格式。"

    try:
        result = subprocess.run(
            [find_claude(), "-p", seed_prompt,
             "--output-format", "text",
             "--dangerously-skip-permissions"],
            capture_output=True, text=True, timeout=120,
            cwd=str(BASE), encoding="utf-8", errors="replace",
        )
        output = result.stdout or ""
        # 提取JSON
        match = re.search(r'\[.*\]', output, re.DOTALL)
        if match:
            tasks = json.loads(match.group())
            # Ensure IDs are unique to this phase
            for i, t in enumerate(tasks):
                t["id"] = f"p{phase}_{i+1}"
            log.info(f"生成了 {len(tasks)} 个 Phase {phase} 任务")
            return tasks
    except Exception as e:
        log.error(f"Phase {phase} 生成失败: {e}")

    # fallback: 硬编码任务 (IDs unique per phase)
    return [
        {
            "id": f"p{phase}_1",
            "name": "多交易所套利引擎",
            "goal": "multi_exchange",
            "prompt": f"【进化 Phase{phase}-1】整合 Binance + OKX + Bybit 数据源，生成跨交易所套利信号，检测价差 >0.5% 自动报警。完成后输出 ✅p{phase}_1完成"
        },
        {
            "id": f"p{phase}_2",
            "name": "代码自检与热修复",
            "goal": "code_review",
            "prompt": f"【进化 Phase{phase}-2】自动审查所有Python文件，发现bug/优化点自动修复，用 py_compile 验证。完成后输出 ✅p{phase}_2完成"
        },
        {
            "id": f"p{phase}_3",
            "name": "链上聪明钱跟踪强化",
            "goal": "onchain_analysis",
            "prompt": f"【进化 Phase{phase}-3】强化链上追踪系统，检测 >$100K 转移自动分析并发 TG 通知。完成后输出 ✅p{phase}_3完成"
        },
        {
            "id": f"p{phase}_4",
            "name": "策略遗传算法优化",
            "goal": "strategy_optimize",
            "prompt": f"【进化 Phase{phase}-4】用遗传算法优化信号策略参数（窗口、阈值），回测验证后自动部署最优参数。完成后输出 ✅p{phase}_4完成"
        },
        {
            "id": f"p{phase}_5",
            "name": "自进化技能合成引擎",
            "goal": "skill_synthesis",
            "prompt": f"【进化 Phase{phase}-5】强化技能合成引擎，自动分析任务模式，生成可复用技能模板。完成后输出 ✅p{phase}_5完成"
        },
    ]


def main():
    # Singleton lock (hold file open for process lifetime)
    lock_fh = None
    try:
        lock_fh = open(str(LOCK_FILE), "w")  # noqa: SIM115 — lock must stay open for process lifetime
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(lock_fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fh.write(str(os.getpid()))
        lock_fh.flush()
    except (OSError, IOError):
        if lock_fh:
            lock_fh.close()
        log.info("Infinite evolver 已在运行，退出")
        return

    try:
        log.info("🚀 Infinite Evolver 启动 - 永续进化直到自主+赚钱")
        tg("🚀 无限进化引擎启动！目标：自主操作 + 智能 + 赚钱\nPhase 2 开始，7个任务...")

        state = load_state()

        # 当前任务队列
        current_tasks = PHASE2_TASKS.copy()
        phase = state.get("phase", 2)
        consecutive_skips = 0  # Detect infinite skip loops

        while True:
            task_index = state.get("task_index", 0)

            # 判断当前phase完成
            if task_index >= len(current_tasks):
                log.info(f"Phase {phase} 完成！生成下一阶段任务...")
                tg(f"🎉 Phase {phase} 全部完成！自动生成 Phase {phase+1} 任务...")

                phase += 1
                state["phase"] = phase
                state["task_index"] = 0
                # Reset completed_tasks for new phase (old IDs don't apply)
                state["completed_tasks"] = []

                current_tasks = generate_phase3_tasks(phase=phase)

                # Safety: cap at Phase 10 to prevent infinite progression
                if phase > 10:
                    log.info("Reached Phase 10 limit. Stopping infinite evolver.")
                    tg("🏁 进化引擎达到 Phase 10 上限，停止循环。")
                    break

                save_state(state)
                time.sleep(120)  # 2 min between phases (not 30s)
                continue

            task = current_tasks[task_index]
            task_id = task["id"]

            # 已完成的跳过
            if task_id in state.get("completed_tasks", []):
                consecutive_skips += 1
                if consecutive_skips >= len(current_tasks):
                    # ALL tasks skipped = ID collision bug, force reset
                    log.error(f"⚠️ All {len(current_tasks)} tasks skipped (ID collision). Resetting completed_tasks.")
                    state["completed_tasks"] = []
                    consecutive_skips = 0
                    continue
                state["task_index"] = task_index + 1
                save_state(state)
                continue
            consecutive_skips = 0  # Reset on actual task execution

            log.info(f"▶ 执行 Phase{phase} 任务 {task_index+1}/{len(current_tasks)}: {task['name']}")
            tg(f"▶ 进化中: Phase{phase} [{task_index+1}/{len(current_tasks)}]\n📝 {task['name']}")

            state["total_runs"] = state.get("total_runs", 0) + 1
            save_state(state)

            # 执行任务
            task_start = time.time()
            success, output = run_claude_task(task["prompt"], timeout=900)
            task_elapsed = time.time() - task_start

            # Safety: if task "completed" in < 10 seconds, it's fake
            if task_elapsed < 10:
                log.warning(f"⚠️ 任务在{task_elapsed:.0f}秒内完成 — 可能是空执行，标记失败")
                success = False
                output = f"[SUSPICIOUS: completed in {task_elapsed:.0f}s, likely no real work]"

            completed = check_completion(output, task_id)

            if completed and success:
                log.info(f"✅ 任务完成: {task['name']}")
                tg(f"✅ Phase{phase}-{task_index+1} 完成: {task['name']}")

                completed_list = state.get("completed_tasks", []) + [task_id]
                state["completed_tasks"] = completed_list[-200:]  # keep only last 200
                state["task_index"] = task_index + 1
                state["consecutive_failures"] = 0
                save_state(state)

                time.sleep(60)  # 任务间休息1分钟

            else:
                failures = state.get("consecutive_failures", 0) + 1
                state["consecutive_failures"] = failures
                save_state(state)

                log.warning(f"⚠️ 任务失败 ({failures}次): {task['name']}")

                if failures >= 3:
                    log.error(f"❌ 任务连续失败3次，跳过: {task['name']}")
                    tg(f"⚠️ 跳过失败任务: {task['name']}（将在下轮重试）")
                    state["failed_tasks"] = (state.get("failed_tasks", []) + [task_id])[-200:]
                    state["task_index"] = task_index + 1
                    state["consecutive_failures"] = 0
                    save_state(state)
                else:
                    log.info(f"重试中（{failures}/3）...")
                    time.sleep(120)  # 失败后等2分钟重试

    except KeyboardInterrupt:
        log.info("手动停止")
        tg("⏹ 无限进化引擎已停止（手动）")
    except Exception as e:
        log.error(f"致命错误: {e}")
        tg(f"❌ 进化引擎错误: {e}")
    finally:
        if lock_fh:
            try:
                lock_fh.close()
            except OSError:
                pass
        try:
            LOCK_FILE.unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    main()
