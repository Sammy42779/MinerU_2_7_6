#!/usr/bin/env python3
"""
端到端验证脚本 — 模拟 MQ 消息调用 Server B 并监控 GPU Utilization

用法:
  # 模式 1: 通过 Server B 完整流水线 (需 IOBS 中已有 PDF)
  python test_e2e.py --mode pipeline --data-id <IOBS_DATA_ID> --file-name test.pdf

  # 模式 2: 直接调 Server A 接口 (绕过 IOBS, 用本地 PDF)
  python test_e2e.py --mode direct --pdf /path/to/test.pdf --server-a http://host:port

  # 模式 3: 通过 Server B, 但先上传本地 PDF 到 IOBS 获取 dataId
  python test_e2e.py --mode upload --pdf /path/to/test.pdf

  # 并发压测: 发送 N 个并发请求
  python test_e2e.py --mode direct --pdf /path/to/test.pdf --concurrency 3

  # GPU 监控独立运行 (仅监控, 不发请求)
  python test_e2e.py --mode monitor --duration 60
"""

import argparse
import asyncio
import base64
import json
import os
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

try:
    import aiohttp
except ImportError:
    print("请安装 aiohttp: pip install aiohttp")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("请安装 requests: pip install requests")
    sys.exit(1)


# ============================================================
# 配置
# ============================================================

@dataclass
class TestConfig:
    """测试配置"""
    # Server 地址
    server_b_url: str = "http://127.0.0.1:29475"
    server_a_url: str = "http://127.0.0.1:80"
    # 并发
    concurrency: int = 1
    # GPU 监控
    gpu_monitor_interval: float = 1.0  # 秒
    monitor_duration: int = 300  # 最大监控时长
    # 超时
    request_timeout: int = 600  # 10 分钟


# ============================================================
# GPU 监控
# ============================================================

@dataclass
class GPUSample:
    timestamp: float
    gpu_util: int  # %
    mem_used: int  # MiB
    mem_total: int  # MiB
    power_draw: float  # W
    temperature: int  # °C


class GPUMonitor:
    """
    后台 GPU 利用率监控器.

    通过 nvidia-smi 轮询采集 GPU 指标.
    """

    def __init__(self, interval: float = 1.0, gpu_id: int = 0):
        self._interval = interval
        self._gpu_id = gpu_id
        self._samples: List[GPUSample] = []
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @property
    def samples(self) -> List[GPUSample]:
        return list(self._samples)

    def _query_gpu(self) -> Optional[GPUSample]:
        """调用 nvidia-smi 获取一次 GPU 指标"""
        try:
            result = subprocess.run(
                [
                    "nvidia-smi",
                    f"--id={self._gpu_id}",
                    "--query-gpu=utilization.gpu,memory.used,memory.total,power.draw,temperature.gpu",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0:
                return None

            parts = result.stdout.strip().split(",")
            if len(parts) < 5:
                return None

            return GPUSample(
                timestamp=time.time(),
                gpu_util=int(parts[0].strip()),
                mem_used=int(parts[1].strip()),
                mem_total=int(parts[2].strip()),
                power_draw=float(parts[3].strip()),
                temperature=int(parts[4].strip()),
            )
        except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
            return None

    async def _monitor_loop(self):
        """后台采集循环"""
        loop = asyncio.get_running_loop()
        while self._running:
            sample = await loop.run_in_executor(None, self._query_gpu)
            if sample:
                self._samples.append(sample)
            await asyncio.sleep(self._interval)

    def start(self):
        if self._running:
            return
        self._running = True
        self._samples.clear()
        self._task = asyncio.ensure_future(self._monitor_loop())
        print("[GPU Monitor] 已启动")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        print(f"[GPU Monitor] 已停止, 共采集 {len(self._samples)} 个样本")

    def report(self) -> dict:
        """生成统计报告"""
        if not self._samples:
            return {"error": "无 GPU 采样数据 (nvidia-smi 不可用或未在 GPU 机器上运行)"}

        utils = [s.gpu_util for s in self._samples]
        mems = [s.mem_used for s in self._samples]
        powers = [s.power_draw for s in self._samples]
        temps = [s.temperature for s in self._samples]
        duration = self._samples[-1].timestamp - self._samples[0].timestamp

        # 计算 GPU > 0% 的时间占比 (有效利用率)
        active_count = sum(1 for u in utils if u > 0)
        active_ratio = active_count / len(utils) if utils else 0

        # 找到 GPU > 50% 的高利用率时段
        high_util_count = sum(1 for u in utils if u > 50)
        high_util_ratio = high_util_count / len(utils) if utils else 0

        return {
            "采样数": len(self._samples),
            "监控时长(s)": round(duration, 1),
            "GPU利用率(%)": {
                "平均": round(sum(utils) / len(utils), 1),
                "最大": max(utils),
                "最小": min(utils),
                "中位数": sorted(utils)[len(utils) // 2],
                "标准差": round(_std(utils), 1),
            },
            "GPU活跃占比(util>0%)": f"{active_ratio:.1%}",
            "GPU高负载占比(util>50%)": f"{high_util_ratio:.1%}",
            "显存使用(MiB)": {
                "平均": round(sum(mems) / len(mems)),
                "最大": max(mems),
                "总量": self._samples[0].mem_total,
                "峰值利用率": f"{max(mems)/self._samples[0].mem_total:.1%}",
            },
            "功耗(W)": {
                "平均": round(sum(powers) / len(powers), 1),
                "最大": round(max(powers), 1),
            },
            "温度(°C)": {
                "平均": round(sum(temps) / len(temps), 1),
                "最大": max(temps),
            },
        }


def _std(data: list) -> float:
    if len(data) < 2:
        return 0.0
    mean = sum(data) / len(data)
    return (sum((x - mean) ** 2 for x in data) / (len(data) - 1)) ** 0.5


# ============================================================
# 模式 1: 通过 Server B 完整流水线
# ============================================================

async def test_pipeline(
    config: TestConfig,
    data_id: str,
    file_name: str,
    concurrency: int = 1,
    gpu_monitor: Optional[GPUMonitor] = None,
):
    """
    模拟 MQ 消息调用 Server B 的 /mineru_parser_split 接口.
    """
    print(f"\n{'='*60}")
    print(f"[Pipeline 模式] 发送 {concurrency} 个并发请求到 Server B")
    print(f"  Server B: {config.server_b_url}")
    print(f"  dataId: {data_id}")
    print(f"  file_name: {file_name}")
    print(f"{'='*60}\n")

    if gpu_monitor:
        gpu_monitor.start()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(concurrency):
            req_id = f"test_{datetime.now().strftime('%H%M%S')}_{i:02d}_{uuid.uuid4().hex[:6]}"
            tasks.append(
                _call_server_b(session, config, req_id, data_id, file_name, i)
            )

        t_start = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_elapsed = round(time.time() - t_start, 1)

    if gpu_monitor:
        # 等待一小段时间以采集尾部 GPU 数据
        await asyncio.sleep(3)
        await gpu_monitor.stop()

    _print_results("Pipeline", results, total_elapsed, gpu_monitor)


async def _call_server_b(
    session: aiohttp.ClientSession,
    config: TestConfig,
    request_id: str,
    data_id: str,
    file_name: str,
    idx: int,
) -> dict:
    """发送单个请求到 Server B"""
    url = f"{config.server_b_url}/mineru_parser_split"
    payload = {
        "request_id": request_id,
        "dataId": data_id,
        "file_name": file_name,
        "is_split": 0,
    }

    print(f"[Task {idx}] 发送请求 {request_id}")
    t_start = time.time()

    try:
        async with session.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=config.request_timeout),
        ) as resp:
            elapsed = round(time.time() - t_start, 2)
            body = await resp.json()
            status = resp.status

            print(f"[Task {idx}] 响应: status={status}, elapsed={elapsed}s")
            return {
                "task_idx": idx,
                "request_id": request_id,
                "status": status,
                "elapsed": elapsed,
                "response": body,
            }
    except Exception as e:
        elapsed = round(time.time() - t_start, 2)
        print(f"[Task {idx}] 失败: {e} ({elapsed}s)")
        return {
            "task_idx": idx,
            "request_id": request_id,
            "status": -1,
            "elapsed": elapsed,
            "error": str(e),
        }


# ============================================================
# 模式 2: 直接调 Server A (绕过 IOBS + EAGW)
# ============================================================

async def test_direct(
    config: TestConfig,
    pdf_path: str,
    concurrency: int = 1,
    gpu_monitor: Optional[GPUMonitor] = None,
):
    """
    直接发 PDF bytes 到 Server A 的 /pingangpt/multimodal/dialog 接口.
    绕过 IOBS / EAGW, 纯测 GPU 解析能力.
    """
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    from chunk_manager import ChunkManager, ChunkConfig

    chunk_config = ChunkConfig(default_chunk_size=15, min_chunk_size=8)
    mgr = ChunkManager(chunk_config)
    total_pages = mgr.get_page_count_from_bytes(pdf_bytes)

    print(f"\n{'='*60}")
    print(f"[Direct 模式] 直接调用 Server A")
    print(f"  Server A: {config.server_a_url}")
    print(f"  PDF: {pdf_path}")
    print(f"  大小: {len(pdf_bytes):,} bytes, 页数: {total_pages}")
    print(f"  并发请求数: {concurrency}")
    print(f"{'='*60}\n")

    # 分块
    chunks = mgr.prepare_chunks(pdf_bytes)
    print(f"分块完成: {len(chunks)} 个 chunk\n")

    if gpu_monitor:
        gpu_monitor.start()

    async with aiohttp.ClientSession() as session:
        # 按并发度调度 chunk 请求
        semaphore = asyncio.Semaphore(concurrency)
        tasks = []
        for chunk in chunks:
            tasks.append(
                _call_server_a(session, config, chunk, semaphore)
            )

        t_start = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_elapsed = round(time.time() - t_start, 1)

    if gpu_monitor:
        await asyncio.sleep(3)
        await gpu_monitor.stop()

    _print_results("Direct", results, total_elapsed, gpu_monitor)

    # 额外: 验证 middle_json 完整性
    success_count = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
    total_pages_parsed = sum(
        r.get("pages_parsed", 0) for r in results if isinstance(r, dict) and r.get("success")
    )
    print(f"\n[完整性] 成功 chunk: {success_count}/{len(chunks)}, 总解析页数: {total_pages_parsed}/{total_pages}")


async def _call_server_a(
    session: aiohttp.ClientSession,
    config: TestConfig,
    chunk,  # ChunkInfo
    semaphore: asyncio.Semaphore,
) -> dict:
    """直接发送 chunk 到 Server A"""
    url = f"{config.server_a_url}/pingangpt/multimodal/dialog"
    pdf_b64 = base64.b64encode(chunk.pdf_bytes).decode("ascii")

    payload = {
        "request_id": f"test_chunk{chunk.chunk_idx}_{uuid.uuid4().hex[:6]}",
        "model_type": "vision",
        "messages": [
            {
                "files": [pdf_b64],
                "data": {"start_page_id": 0, "end_page_id": 99999},
            }
        ],
        "scene_id": 1503,
        "stream": False,
    }

    print(f"  [Chunk {chunk.chunk_idx}] pages {chunk.start_page}-{chunk.end_page}, "
          f"pdf_b64={len(pdf_b64):,} chars → 等待信号量...")

    async with semaphore:
        t_start = time.time()
        print(f"  [Chunk {chunk.chunk_idx}] 开始发送...")

        try:
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=config.request_timeout),
            ) as resp:
                elapsed = round(time.time() - t_start, 2)
                status = resp.status

                if status == 503:
                    print(f"  [Chunk {chunk.chunk_idx}] 503 - Server A 满载 ({elapsed}s)")
                    return {
                        "chunk_idx": chunk.chunk_idx,
                        "success": False,
                        "status": 503,
                        "elapsed": elapsed,
                        "error": "Server A at capacity",
                    }

                body = await resp.json()
                code = body.get("code", "")

                if code == "2000000":
                    info = body.get("info", {})
                    choices = info.get("choices", [{}])
                    inner = choices[0].get("message", {}) if choices else {}
                    mj = inner.get("middle_json", {})
                    images = inner.get("images", {})
                    pages_parsed = len(mj.get("pdf_info", []))

                    print(
                        f"  [Chunk {chunk.chunk_idx}] ✓ {pages_parsed} pages, "
                        f"{len(images)} images, {elapsed}s"
                    )
                    return {
                        "chunk_idx": chunk.chunk_idx,
                        "success": True,
                        "status": status,
                        "elapsed": elapsed,
                        "pages_parsed": pages_parsed,
                        "image_count": len(images),
                    }
                else:
                    msg = body.get("message", f"code={code}")
                    print(f"  [Chunk {chunk.chunk_idx}] ✗ {msg} ({elapsed}s)")
                    return {
                        "chunk_idx": chunk.chunk_idx,
                        "success": False,
                        "status": status,
                        "elapsed": elapsed,
                        "error": msg,
                    }

        except asyncio.TimeoutError:
            elapsed = round(time.time() - t_start, 2)
            print(f"  [Chunk {chunk.chunk_idx}] ✗ 超时 ({elapsed}s)")
            return {
                "chunk_idx": chunk.chunk_idx,
                "success": False,
                "elapsed": elapsed,
                "error": "timeout",
            }
        except Exception as e:
            elapsed = round(time.time() - t_start, 2)
            print(f"  [Chunk {chunk.chunk_idx}] ✗ {e} ({elapsed}s)")
            return {
                "chunk_idx": chunk.chunk_idx,
                "success": False,
                "elapsed": elapsed,
                "error": str(e),
            }


# ============================================================
# 模式 3: 上传 PDF 到 IOBS 后走 Server B
# ============================================================

async def test_upload_then_pipeline(
    config: TestConfig,
    pdf_path: str,
    concurrency: int = 1,
    gpu_monitor: Optional[GPUMonitor] = None,
):
    """先上传 PDF 到 IOBS, 获取 dataId, 再走 Server B 流水线"""
    from pipeline import upload_iobs_file

    print(f"[Upload] 上传 {pdf_path} 到 IOBS...")
    file_url, file_id = upload_iobs_file(pdf_path)

    if not file_id:
        print("[Upload] 上传失败, 无法获取 dataId")
        return

    print(f"[Upload] 上传成功: fileId={file_id}")

    file_name = os.path.basename(pdf_path)
    await test_pipeline(config, file_id, file_name, concurrency, gpu_monitor)


# ============================================================
# 模式 4: 纯 GPU 监控
# ============================================================

async def test_monitor_only(config: TestConfig, duration: int):
    print(f"\n[Monitor] 纯 GPU 监控模式, 持续 {duration}s")
    print(f"  采样间隔: {config.gpu_monitor_interval}s")
    print(f"  按 Ctrl+C 提前结束\n")

    monitor = GPUMonitor(interval=config.gpu_monitor_interval)
    monitor.start()

    try:
        for i in range(duration):
            await asyncio.sleep(1)
            if monitor.samples:
                s = monitor.samples[-1]
                bar = "█" * (s.gpu_util // 5) + "░" * (20 - s.gpu_util // 5)
                print(
                    f"\r  [{i+1:3d}s] GPU: {bar} {s.gpu_util:3d}% | "
                    f"Mem: {s.mem_used}/{s.mem_total} MiB | "
                    f"Power: {s.power_draw:.0f}W | Temp: {s.temperature}°C",
                    end="",
                    flush=True,
                )
    except KeyboardInterrupt:
        print("\n\n[Monitor] 用户中断")

    await monitor.stop()
    print()
    _print_gpu_report(monitor)


# ============================================================
# 辅助: Server A 健康检查
# ============================================================

async def check_server_health(url: str, name: str) -> bool:
    """检查服务是否可达"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{url}/health", timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    body = await resp.json()
                    print(f"  ✓ {name}: {url} — {body}")
                    return True
                else:
                    print(f"  ✗ {name}: {url} — HTTP {resp.status}")
                    return False
    except Exception as e:
        print(f"  ✗ {name}: {url} — {e}")
        return False


# ============================================================
# 输出
# ============================================================

def _print_results(mode: str, results: list, total_elapsed: float, gpu_monitor: Optional[GPUMonitor]):
    print(f"\n{'='*60}")
    print(f"[{mode} 结果汇总]")
    print(f"{'='*60}")
    print(f"总耗时: {total_elapsed}s")

    success = 0
    fail = 0
    elapsed_list = []

    for r in results:
        if isinstance(r, Exception):
            fail += 1
            print(f"  异常: {r}")
        elif isinstance(r, dict):
            if r.get("success") or r.get("status") in (200, 202):
                success += 1
            else:
                fail += 1
            elapsed_list.append(r.get("elapsed", 0))

    print(f"成功: {success}, 失败: {fail}")
    if elapsed_list:
        print(f"单请求耗时: avg={sum(elapsed_list)/len(elapsed_list):.1f}s, "
              f"min={min(elapsed_list):.1f}s, max={max(elapsed_list):.1f}s")

    if gpu_monitor:
        print()
        _print_gpu_report(gpu_monitor)


def _print_gpu_report(monitor: GPUMonitor):
    report = monitor.report()
    if "error" in report:
        print(f"[GPU 报告] {report['error']}")
        return

    print(f"{'─'*50}")
    print(f"[GPU 利用率报告]")
    print(f"{'─'*50}")

    for key, val in report.items():
        if isinstance(val, dict):
            print(f"  {key}:")
            for k2, v2 in val.items():
                print(f"    {k2}: {v2}")
        else:
            print(f"  {key}: {val}")

    # ASCII 时间线
    samples = monitor.samples
    if len(samples) > 2:
        print(f"\n  GPU 利用率时间线 (每行代表 ~1s):")
        print(f"  {'─'*42}")
        # 降采样到最多 40 行
        step = max(1, len(samples) // 40)
        for i in range(0, len(samples), step):
            s = samples[i]
            t_offset = round(s.timestamp - samples[0].timestamp, 0)
            bar_len = s.gpu_util // 5
            bar = "█" * bar_len + "░" * (20 - bar_len)
            print(f"  {t_offset:5.0f}s |{bar}| {s.gpu_util:3d}%  mem={s.mem_used}MiB")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="MinerU 服务端到端验证 + GPU Utilization 测试",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 直接调 Server A (最简单, 推荐先用此模式验证)
  python test_e2e.py --mode direct --pdf sample.pdf --server-a http://10.0.0.1:80 --concurrency 2

  # 通过 Server B 完整流水线
  python test_e2e.py --mode pipeline --data-id ABC123 --file-name test.pdf

  # 上传 PDF 后走 Server B
  python test_e2e.py --mode upload --pdf sample.pdf

  # 纯 GPU 监控
  python test_e2e.py --mode monitor --duration 120
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["pipeline", "direct", "upload", "monitor"],
        required=True,
        help="测试模式: pipeline=通过Server B, direct=直接调Server A, upload=上传后走Server B, monitor=纯GPU监控",
    )
    parser.add_argument("--pdf", help="本地 PDF 文件路径 (direct/upload 模式必填)")
    parser.add_argument("--data-id", help="IOBS dataId (pipeline 模式必填)")
    parser.add_argument("--file-name", help="文件名 (pipeline 模式必填)")
    parser.add_argument("--server-a", default="http://127.0.0.1:80", help="Server A 地址 (default: http://127.0.0.1:80)")
    parser.add_argument("--server-b", default="http://127.0.0.1:29475", help="Server B 地址 (default: http://127.0.0.1:29475)")
    parser.add_argument("--concurrency", type=int, default=2, help="并发请求数 (default: 2)")
    parser.add_argument("--gpu-id", type=int, default=0, help="监控的 GPU ID (default: 0)")
    parser.add_argument("--interval", type=float, default=1.0, help="GPU 采样间隔秒数 (default: 1.0)")
    parser.add_argument("--duration", type=int, default=300, help="monitor模式的监控时长秒数 (default: 300)")
    parser.add_argument("--no-gpu-monitor", action="store_true", help="禁用 GPU 监控")
    parser.add_argument("--timeout", type=int, default=600, help="请求超时秒数 (default: 600)")

    args = parser.parse_args()

    config = TestConfig(
        server_a_url=args.server_a.rstrip("/"),
        server_b_url=args.server_b.rstrip("/"),
        concurrency=args.concurrency,
        gpu_monitor_interval=args.interval,
        monitor_duration=args.duration,
        request_timeout=args.timeout,
    )

    gpu_monitor = None
    if not args.no_gpu_monitor and args.mode != "monitor":
        gpu_monitor = GPUMonitor(interval=config.gpu_monitor_interval, gpu_id=args.gpu_id)

    async def run():
        # 先检查服务健康状态
        print("\n[健康检查]")
        if args.mode in ("direct",):
            await check_server_health(config.server_a_url, "Server A")
        elif args.mode in ("pipeline", "upload"):
            await check_server_health(config.server_b_url, "Server B")
        print()

        if args.mode == "pipeline":
            if not args.data_id or not args.file_name:
                parser.error("pipeline 模式需要 --data-id 和 --file-name")
            await test_pipeline(config, args.data_id, args.file_name, args.concurrency, gpu_monitor)

        elif args.mode == "direct":
            if not args.pdf:
                parser.error("direct 模式需要 --pdf")
            if not os.path.isfile(args.pdf):
                parser.error(f"PDF 文件不存在: {args.pdf}")
            await test_direct(config, args.pdf, args.concurrency, gpu_monitor)

        elif args.mode == "upload":
            if not args.pdf:
                parser.error("upload 模式需要 --pdf")
            if not os.path.isfile(args.pdf):
                parser.error(f"PDF 文件不存在: {args.pdf}")
            await test_upload_then_pipeline(config, args.pdf, args.concurrency, gpu_monitor)

        elif args.mode == "monitor":
            await test_monitor_only(config, args.duration)

    asyncio.run(run())


if __name__ == "__main__":
    main()
