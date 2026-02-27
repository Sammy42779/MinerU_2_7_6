#!/usr/bin/env python3
"""
test_eagw_opt.py — EAGW 网关 / Server A 接口验证脚本（新方案）

与 test_eagw.py 的主要区别:
  1. 所有认证参数从 config 文件读取，不硬编码
  2. 请求格式使用新方案: files=[pdf_b64], data={start_page_id, end_page_id}
     （旧方案为 files=[1], data={dataId, file_name}）
  3. 新增 --mode direct: 绕过 EAGW，直接打 Server A（用于本地调试）
  4. 新增异步并发模式，可评估 Server A GPU 吞吐
  5. 完整解析响应 middle_json / images，打印关键指标

用法:
  # 通过 EAGW 网关（使用 stg 配置）
  python test_eagw_opt.py --mode gateway --pdf sample.pdf

  # 通过 EAGW 网关（使用 prd 配置）
  ENV=prd python test_eagw_opt.py --mode gateway --pdf sample.pdf

  # 绕过 EAGW，直接打 Server A（本地调试）
  python test_eagw_opt.py --mode direct --pdf sample.pdf --server-a http://10.0.0.1:80

  # 并发压测（发 3 个 chunk，每 chunk 15 页）
  python test_eagw_opt.py --mode gateway --pdf sample.pdf --concurrency 3

  # 保存 middle_json 结果到本地
  python test_eagw_opt.py --mode gateway --pdf sample.pdf --output-dir ./output
"""

import argparse
import asyncio
import base64
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

# ---- 可选依赖检查 ----
try:
    import aiohttp
except ImportError:
    print("请安装 aiohttp: pip install aiohttp")
    sys.exit(1)

# ---- 签名函数（从 sign_cache 复用，避免重复造轮子） ----
# 注意: 运行时需要在 serverB/ 目录或 sys.path 包含该目录
_SERVERB_DIR = Path(__file__).parent
if str(_SERVERB_DIR) not in sys.path:
    sys.path.insert(0, str(_SERVERB_DIR))

try:
    from sign_cache import _get_gpt_sign, _get_gateway_sign
except ImportError:
    print("无法导入 sign_cache，请确认脚本在 serverB/ 目录下运行")
    sys.exit(1)

# ---- 配置读取 ----
try:
    from configs.config import Config
except ImportError:
    print("无法导入 configs.config，请确认脚本在 serverB/ 目录下运行")
    sys.exit(1)


# ============================================================
# 健康检查 — 确保接口可达
# ============================================================

async def check_connectivity(url: str, name: str, health_path: Optional[str] = None) -> bool:
    """
    检查目标服务是否可达.

    - direct 模式: GET {server_a_url}/health（Server A 有专用健康检查端点）
    - gateway 模式: 向 EAGW URL 发送 HEAD 请求，仅验证 TCP + HTTP 可达性，
      不校验业务响应（网关无标准 /health 端点）
    """
    check_url = f"{url.rstrip('/')}{health_path}" if health_path else url
    print(f"[健康检查] {name}  →  {check_url}")
    t0 = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            method = session.get if health_path else session.head
            async with method(
                check_url,
                timeout=aiohttp.ClientTimeout(total=8),
                allow_redirects=True,
            ) as resp:
                elapsed = round(time.time() - t0, 2)
                if health_path:
                    # Server A /health 返回 200 + body
                    if resp.status == 200:
                        body = await resp.text()
                        print(f"  ✓ {name} 健康  HTTP {resp.status}  ({elapsed}s)  body={body[:80]}")
                        return True
                    else:
                        print(f"  ✗ {name} 不健康  HTTP {resp.status}  ({elapsed}s)")
                        return False
                else:
                    # EAGW HEAD 只要 TCP 握手成功即视为可达（4xx/5xx 也算网络通）
                    reachable = resp.status < 600
                    mark = "✓" if reachable else "✗"
                    print(f"  {mark} {name} {'可达' if reachable else '不可达'}  HTTP {resp.status}  ({elapsed}s)")
                    return reachable
    except aiohttp.ClientConnectorError as e:
        elapsed = round(time.time() - t0, 2)
        print(f"  ✗ {name} 连接失败  ({elapsed}s): {e}")
        return False
    except asyncio.TimeoutError:
        elapsed = round(time.time() - t0, 2)
        print(f"  ✗ {name} 连接超时  ({elapsed}s)")
        return False
    except Exception as e:
        elapsed = round(time.time() - t0, 2)
        print(f"  ✗ {name} 异常  ({elapsed}s): {e}")
        return False


# ============================================================
# 从 config 文件加载 EAGW 参数
# ============================================================

def load_eagw_config() -> dict:
    """
    从 config_stg.conf 或 config_prd.conf 读取 EAGW 认证参数.
    通过 ENV 环境变量切换（默认 stg）.
    """
    cfg = Config.config
    env = Config.env

    eagw = {
        "env": env,
        "app_key":         cfg.get("mineru_api", "app_key"),
        "app_secret":      cfg.get("mineru_api", "app_secret"),
        "rsa_private_key": cfg.get("mineru_api", "rsaPrivateKey"),
        "url_dialog":      cfg.get("mineru_api", "url_dialog"),
        "scene_id":        cfg.getint("mineru_api", "scene_id"),
        "open_api_code":   cfg.get("mineru_api", "openApiCode").strip(),
        "open_api_cred":   cfg.get("mineru_api", "openApiCredential"),
    }
    return eagw


# ============================================================
# 签名 — 新方案与旧方案使用相同的算法，此处复用 sign_cache 函数
# ============================================================

def build_signed_headers(eagw_cfg: dict) -> dict:
    """生成一次请求所需的 EAGW 签名 headers"""
    request_time = str(int(time.time()) * 1000)
    headers = {
        "Content-Type":      "application/json",
        "openApiCode":       eagw_cfg["open_api_code"],
        "openApiCredential": eagw_cfg["open_api_cred"],
        "openApiRequestTime": request_time,
        "openApiSignature":  _get_gateway_sign(eagw_cfg["rsa_private_key"], request_time),
        "gpt_app_key":       eagw_cfg["app_key"],
        "gpt_signature":     _get_gpt_sign(eagw_cfg["app_key"], eagw_cfg["app_secret"], request_time),
    }
    return headers


# ============================================================
# 请求体构造 — 新方案
# ============================================================

def build_request_body(
    pdf_b64: str,
    scene_id: int,
    request_id: Optional[str] = None,
    start_page: int = 0,
    end_page: int = 99999,
) -> dict:
    """
    构造新方案请求体.

    新方案:
        files = [<pdf_bytes_base64>]          # PDF 二进制 base64
        data  = {start_page_id, end_page_id}  # 页码范围

    旧方案(test_eagw.py):
        files = [1]                           # 占位符
        data  = {dataId, file_name}           # IOBS 引用
    """
    return {
        "request_id": request_id or str(uuid.uuid4()),
        "model_type": "vision",
        "messages": [
            {
                "files": [pdf_b64],
                "data": {
                    "start_page_id": start_page,
                    "end_page_id":   end_page,
                },
            }
        ],
        "scene_id": scene_id,
        "stream": False,
    }


# ============================================================
# 响应解析
# ============================================================

def parse_response(body: dict) -> dict:
    """
    统一解析 Server A / EAGW 响应.

    Server A 直连格式:
        {"code": "2000000", "info": {"choices": [{"message": {"middle_json": ..., "images": ...}}]}}

    EAGW 网关封装格式:
        {"resultCode": "0", "resultMsg": "success", "data": <同上 Server A 格式>}
    """
    # EAGW 封装层
    if "resultCode" in body:
        if body.get("resultCode") != "0":
            return {
                "success": False,
                "error": f"EAGW error: resultCode={body.get('resultCode')}, "
                         f"msg={body.get('resultMsg', '')}",
            }
        body = body.get("data", {})

    code = body.get("code", "")
    if code != "2000000":
        return {
            "success": False,
            "error": f"Server A error: code={code}, msg={body.get('message', '')}",
        }

    choices = body.get("info", {}).get("choices", [{}])
    inner = choices[0].get("message", {}) if choices else {}
    middle_json = inner.get("middle_json", {})
    images = inner.get("images", {})

    return {
        "success": True,
        "middle_json": middle_json,
        "images": images,
        "page_count": len(middle_json.get("pdf_info", [])),
        "image_count": len(images),
    }


# ============================================================
# 单次请求 — 异步
# ============================================================

async def call_once(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict,
    body: dict,
    timeout: int,
    task_idx: int,
) -> dict:
    """发送单次请求并返回结构化结果"""
    t0 = time.time()
    try:
        async with session.post(
            url,
            json=body,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            elapsed = round(time.time() - t0, 2)
            http_status = resp.status

            if http_status == 503:
                print(f"  [Task {task_idx}] 503 — Server A 满载 ({elapsed}s)")
                return {
                    "task_idx": task_idx,
                    "success": False,
                    "elapsed": elapsed,
                    "error": "Server A at capacity (503)",
                }

            resp_body = await resp.json(content_type=None)
            result = parse_response(resp_body)
            result["task_idx"] = task_idx
            result["elapsed"] = elapsed
            result["http_status"] = http_status

            if result["success"]:
                print(
                    f"  [Task {task_idx}] ✓  pages={result['page_count']}, "
                    f"images={result['image_count']}, elapsed={elapsed}s"
                )
            else:
                print(f"  [Task {task_idx}] ✗  {result['error']} ({elapsed}s)")

            return result

    except asyncio.TimeoutError:
        elapsed = round(time.time() - t0, 2)
        print(f"  [Task {task_idx}] ✗  超时 ({elapsed}s)")
        return {"task_idx": task_idx, "success": False, "elapsed": elapsed, "error": "timeout"}
    except Exception as e:
        elapsed = round(time.time() - t0, 2)
        print(f"  [Task {task_idx}] ✗  {e} ({elapsed}s)")
        return {"task_idx": task_idx, "success": False, "elapsed": elapsed, "error": str(e)}


# ============================================================
# 模式 1: 通过 EAGW 网关
# ============================================================

async def run_gateway_mode(
    pdf_path: str,
    concurrency: int,
    timeout: int,
    output_dir: Optional[str],
    chunk_size: int,
):
    """
    通过 EAGW 网关调用 Server A（新方案）.
    PDF 按 chunk_size 分页切割后并发发送.
    """
    eagw_cfg = load_eagw_config()

    print(f"\n{'='*60}")
    print(f"[Gateway 模式]  ENV={eagw_cfg['env']}")
    print(f"  EAGW URL:  {eagw_cfg['url_dialog']}")
    print(f"  scene_id:  {eagw_cfg['scene_id']}")
    print(f"  openApiCode: {eagw_cfg['open_api_code']}")
    print(f"  PDF:       {pdf_path}")
    print(f"  chunk_size: {chunk_size} pages/chunk")
    print(f"  concurrency: {concurrency}")
    print(f"{'='*60}\n")

    # ---- 健康检查 ----
    ok = await check_connectivity(eagw_cfg["url_dialog"], "EAGW Gateway")
    if not ok:
        print("[警告] EAGW 网关不可达，请确认网络与地址后继续。按 Ctrl+C 取消，或等待 3s 继续...")
        await asyncio.sleep(3)
    print()

    await _run(
        url=eagw_cfg["url_dialog"],
        scene_id=eagw_cfg["scene_id"],
        eagw_cfg=eagw_cfg,
        pdf_path=pdf_path,
        concurrency=concurrency,
        timeout=timeout,
        output_dir=output_dir,
        chunk_size=chunk_size,
    )


# ============================================================
# 模式 2: 绕过 EAGW，直连 Server A
# ============================================================

async def run_direct_mode(
    server_a_url: str,
    pdf_path: str,
    concurrency: int,
    timeout: int,
    output_dir: Optional[str],
    chunk_size: int,
):
    """
    直接打 Server A，不经过 EAGW 签名（本地调试用）.
    此模式不构造 EAGW headers，直接以 JSON body 发数据.
    """
    # scene_id 仍从 config 读取，保持一致
    cfg = Config.config
    scene_id = cfg.getint("mineru_api", "scene_id")

    print(f"\n{'='*60}")
    print(f"[Direct 模式]  直连 Server A (无 EAGW 签名)")
    print(f"  Server A URL: {server_a_url}/pingangpt/multimodal/dialog")
    print(f"  scene_id:     {scene_id}")
    print(f"  PDF:          {pdf_path}")
    print(f"  chunk_size:   {chunk_size} pages/chunk")
    print(f"  concurrency:  {concurrency}")
    print(f"{'='*60}\n")

    # ---- 健康检查 ----
    ok = await check_connectivity(server_a_url.rstrip("/"), "Server A", health_path="/health")
    if not ok:
        print("[警告] Server A 不可达，请确认服务已启动。按 Ctrl+C 取消，或等待 3s 继续...")
        await asyncio.sleep(3)
    print()

    url = f"{server_a_url.rstrip('/')}/pingangpt/multimodal/dialog"
    await _run(
        url=url,
        scene_id=scene_id,
        eagw_cfg=None,           # 不生成签名 headers
        pdf_path=pdf_path,
        concurrency=concurrency,
        timeout=timeout,
        output_dir=output_dir,
        chunk_size=chunk_size,
    )


# ============================================================
# 公共执行逻辑 — 分块 + 并发调度
# ============================================================

async def _run(
    url: str,
    scene_id: int,
    eagw_cfg: Optional[dict],
    pdf_path: str,
    concurrency: int,
    timeout: int,
    output_dir: Optional[str],
    chunk_size: int,
):
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    # 分块: 按 chunk_size 分页（用 pypdfium2 读页数 + 切割）
    chunks = _split_pdf(pdf_bytes, chunk_size)
    print(f"PDF 总大小: {len(pdf_bytes):,} bytes, 分成 {len(chunks)} 个 chunk\n")

    semaphore = asyncio.Semaphore(concurrency)
    tasks = []

    async def _bounded_call(idx: int, chunk_bytes: bytes, start_p: int, end_p: int):
        async with semaphore:
            b64 = base64.b64encode(chunk_bytes).decode("ascii")
            body = build_request_body(b64, scene_id, start_page=start_p, end_page=end_p)
            # 每次请求独立生成一份新签名（签名含时间戳，不能复用）
            headers = build_signed_headers(eagw_cfg) if eagw_cfg else {"Content-Type": "application/json"}
            async with aiohttp.ClientSession() as session:
                return await call_once(session, url, headers, body, timeout, idx)

    for idx, (chunk_bytes, start_p, end_p) in enumerate(chunks):
        tasks.append(_bounded_call(idx, chunk_bytes, start_p, end_p))

    t_start = time.time()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    total_elapsed = round(time.time() - t_start, 1)

    _print_summary(results, total_elapsed)

    if output_dir:
        _save_results(output_dir, results)


# ============================================================
# PDF 分块工具
# ============================================================

def _split_pdf(pdf_bytes: bytes, chunk_size: int) -> List[tuple]:
    """
    将 PDF 按 chunk_size 分页切割，返回 [(chunk_bytes, start_page, end_page), ...]

    依赖 pypdfium2（Server A 也依赖此库，确保已安装）.
    若 pypdfium2 不可用则整份 PDF 作为一个 chunk.
    """
    try:
        import pypdfium2 as pdfium
        from mineru.cli.common import convert_pdf_bytes_to_bytes_by_pypdfium2
    except ImportError:
        print("  [警告] pypdfium2 不可用，整份 PDF 作为单 chunk 发送")
        return [(pdf_bytes, 0, 99999)]

    try:
        doc = pdfium.PdfDocument(pdf_bytes)
        total_pages = len(doc)
        doc.close()
    except Exception as e:
        print(f"  [警告] 读取 PDF 页数失败: {e}，整份作为单 chunk")
        return [(pdf_bytes, 0, 99999)]

    chunks = []
    for start in range(0, total_pages, chunk_size):
        end = min(start + chunk_size - 1, total_pages - 1)
        try:
            sliced = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, start, end)
        except Exception:
            sliced = pdf_bytes  # 切割失败则用原始
        chunks.append((sliced, start, end))

    return chunks if chunks else [(pdf_bytes, 0, 99999)]


# ============================================================
# 结果输出
# ============================================================

def _print_summary(results: list, total_elapsed: float):
    print(f"\n{'='*60}")
    print(f"[汇总]  总耗时: {total_elapsed}s")
    print(f"{'='*60}")

    success, fail = 0, 0
    elapsed_list = []
    total_pages = 0
    total_images = 0

    for r in results:
        if isinstance(r, Exception):
            fail += 1
            print(f"  异常: {r}")
            continue
        if r.get("success"):
            success += 1
            total_pages  += r.get("page_count", 0)
            total_images += r.get("image_count", 0)
        else:
            fail += 1
        elapsed_list.append(r.get("elapsed", 0))

    print(f"  成功: {success}  失败: {fail}")
    if elapsed_list:
        avg = sum(elapsed_list) / len(elapsed_list)
        print(f"  单 chunk 耗时 — avg={avg:.1f}s  min={min(elapsed_list):.1f}s  max={max(elapsed_list):.1f}s")
    print(f"  累计解析页数: {total_pages}  累计裁图: {total_images}")


def _save_results(output_dir: str, results: list):
    """将每个成功 chunk 的 middle_json / images 存到本地"""
    os.makedirs(output_dir, exist_ok=True)
    saved = 0
    for r in results:
        if not isinstance(r, dict) or not r.get("success"):
            continue
        idx = r.get("task_idx", 0)

        mj = r.get("middle_json")
        if mj:
            path = os.path.join(output_dir, f"chunk{idx:02d}_middle.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(mj, f, ensure_ascii=False, indent=2)
            print(f"  → middle_json 已保存: {path}")

        images: Dict[str, str] = r.get("images", {})
        for img_key, img_data_uri in images.items():
            # img_key 格式: "images/xxx.jpg"
            img_name = os.path.basename(img_key)
            img_dir  = os.path.join(output_dir, f"chunk{idx:02d}_images")
            os.makedirs(img_dir, exist_ok=True)
            img_path = os.path.join(img_dir, img_name)
            # data URI: "data:image/jpeg;base64,<b64>"
            if "," in img_data_uri:
                raw_b64 = img_data_uri.split(",", 1)[1]
            else:
                raw_b64 = img_data_uri
            with open(img_path, "wb") as f:
                f.write(base64.b64decode(raw_b64))
        if images:
            print(f"  → {len(images)} 张裁图已保存: {img_dir}")

        saved += 1

    print(f"\n共保存 {saved} 个 chunk 结果至: {output_dir}")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="EAGW 网关 / Server A 接口验证（新方案：pdf_b64 替代 dataId）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # STG 环境，通过 EAGW 网关
  python test_eagw_opt.py --mode gateway --pdf sample.pdf

  # PRD 环境，通过 EAGW 网关
  ENV=prd python test_eagw_opt.py --mode gateway --pdf sample.pdf

  # 直连 Server A（绕过 EAGW，本地调试）
  python test_eagw_opt.py --mode direct --pdf sample.pdf --server-a http://10.0.0.1:80

  # 3 并发压测
  python test_eagw_opt.py --mode gateway --pdf sample.pdf --concurrency 3

  # 保存结果
  python test_eagw_opt.py --mode gateway --pdf sample.pdf --output-dir ./output
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["gateway", "direct"],
        required=True,
        help=(
            "gateway = 通过 EAGW 网关调用（使用 config 中的 url_dialog + 签名）; "
            "direct  = 绕过 EAGW 直连 Server A（本地调试，需 --server-a）"
        ),
    )
    parser.add_argument("--pdf", required=True, help="本地 PDF 文件路径")
    parser.add_argument(
        "--server-a",
        default="http://127.0.0.1:80",
        help="Server A 地址（仅 direct 模式使用，default: http://127.0.0.1:80）",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="并发 chunk 请求数 (default: 1)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=15,
        help="每个 chunk 的最大页数 (default: 15，与 Server B 默认值一致)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=295,
        help="单次请求超时秒数 (default: 295)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="保存 middle_json / 裁剪图片的目录（不指定则不保存）",
    )

    args = parser.parse_args()

    if not os.path.isfile(args.pdf):
        parser.error(f"PDF 文件不存在: {args.pdf}")

    if args.mode == "gateway":
        asyncio.run(
            run_gateway_mode(
                pdf_path=args.pdf,
                concurrency=args.concurrency,
                timeout=args.timeout,
                output_dir=args.output_dir,
                chunk_size=args.chunk_size,
            )
        )
    elif args.mode == "direct":
        asyncio.run(
            run_direct_mode(
                server_a_url=args.server_a,
                pdf_path=args.pdf,
                concurrency=args.concurrency,
                timeout=args.timeout,
                output_dir=args.output_dir,
                chunk_size=args.chunk_size,
            )
        )


if __name__ == "__main__":
    main()
