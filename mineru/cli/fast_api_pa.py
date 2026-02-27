"""
Server A — 纯 GPU 解析引擎
端点: POST /pingangpt/multimodal/dialog
职责: 接收 PDF bytes → VLM 两阶段推理 → 返回 middle_json + base64 裁剪图片
不做: IOBS 上传/下载、Markdown 生成、content_list 转换
"""

import asyncio
import base64
import glob
import io
import os
import sys
import time
import uuid
import tempfile
import shutil

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel, Field
from typing import List, Optional, Any

# ---------- 日志 ----------
log_level = os.getenv("MINERU_LOG_LEVEL", "INFO").upper()
logger.remove()
logger.add(sys.stderr, level=log_level)

# ---------- 核心依赖 ----------
from mineru.cli.common import (
    prepare_env,
    convert_pdf_bytes_to_bytes_by_pypdfium2,
)
from mineru.backend.vlm.vlm_analyze import aio_doc_analyze as aio_vlm_doc_analyze
from mineru.utils.engine_utils import get_vlm_engine
from mineru.data.data_reader_writer import FileBasedDataWriter
from mineru.version import __version__

os.environ["TOKENIZERS_PARALLELISM"] = "false"

# ============================================================
# 并发控制
# ============================================================
_request_semaphore: Optional[asyncio.Semaphore] = None


def create_app() -> FastAPI:
    app = FastAPI(
        title="MinerU Parser - Server A",
        openapi_url="/openapi.json",
        docs_url="/docs",
        redoc_url=None,
    )

    global _request_semaphore
    try:
        max_concurrent = int(os.getenv("MINERU_API_MAX_CONCURRENT_REQUESTS", "3"))
    except ValueError:
        max_concurrent = 3

    if max_concurrent > 0:
        _request_semaphore = asyncio.Semaphore(max_concurrent)
        logger.info(f"Server A concurrency limited to {max_concurrent}")

    app.add_middleware(GZipMiddleware, minimum_size=1000)
    return app


app = create_app()


# ============================================================
# 启动预热 — 提前加载 VLM 模型到 GPU 显存
# ============================================================
@app.on_event("startup")
async def warmup_model():
    """在服务启动时用一个最小空白 PDF 驱动 VLM 推理，
    触发 ModelSingleton 加载模型权重到 VRAM，
    避免第一个真实请求承受 30-120 秒的模型加载延迟。
    设置 MINERU_SKIP_WARMUP=1 可跳过。
    """
    if os.getenv("MINERU_SKIP_WARMUP", "0") == "1":
        logger.info("[warmup] skipped (MINERU_SKIP_WARMUP=1)")
        return

    logger.info("[warmup] starting model warmup ...")
    t0 = time.time()

    try:
        # ---- 生成 1 页空白 PDF (pypdfium2) ----
        import pypdfium2 as pdfium

        pdf_doc = pdfium.PdfDocument.new()
        pdf_doc.new_page(width=612, height=792)          # Letter size
        buf = io.BytesIO()
        pdf_doc.save(buf)
        pdf_doc.close()
        warmup_pdf_bytes = buf.getvalue()

        # ---- 准备临时目录 ----
        work_dir = os.path.join(tempfile.gettempdir(), "mineru_warmup")
        os.makedirs(work_dir, exist_ok=True)

        try:
            local_image_dir, _ = prepare_env(work_dir, "warmup", "vlm")
            image_writer = FileBasedDataWriter(local_image_dir)

            backend = os.getenv("MINERU_VLM_BACKEND", "vllm-async-engine")
            if backend == "auto-engine":
                backend = get_vlm_engine(inference_engine="auto", is_async=True)

            # ---- 执行推理，触发模型加载 ----
            os.environ.setdefault("MINERU_VLM_FORMULA_ENABLE", "True")
            os.environ.setdefault("MINERU_VLM_TABLE_ENABLE", "True")

            await aio_vlm_doc_analyze(
                warmup_pdf_bytes, image_writer=image_writer, backend=backend
            )

            elapsed = round(time.time() - t0, 2)
            logger.info(f"[warmup] model loaded and ready  ({elapsed}s)")
        finally:
            _cleanup(work_dir)

    except Exception as e:
        elapsed = round(time.time() - t0, 2)
        logger.error(f"[warmup] failed after {elapsed}s: {e}")
        logger.info("[warmup] model will load on first request instead")


# ============================================================
# 请求/响应模型
# ============================================================
class MessageData(BaseModel):
    start_page_id: int = 0
    end_page_id: int = 99999


class MessageItem(BaseModel):
    files: List[str] = Field(
        ..., description="PDF bytes Base64 编码列表（通常只有一个元素）"
    )
    data: MessageData = MessageData()


class DialogRequest(BaseModel):
    request_id: str = ""
    model_type: str = "vision"
    messages: List[MessageItem] = []
    scene_id: int = 1503
    stream: bool = False


# ============================================================
# 工具函数
# ============================================================
def _encode_image_file(path: str) -> str:
    """读取图片文件并返回 data URI"""
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    return f"data:image/jpeg;base64,{b64}"


def _cleanup(path: str):
    """清理临时目录"""
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
    except Exception as e:
        logger.warning(f"cleanup failed: {path}, {e}")


# ============================================================
# 核心端点
# ============================================================
@app.post("/pingangpt/multimodal/dialog")
async def dialog(req: DialogRequest):
    """
    接收 PDF bytes (Base64), 执行 VLM 解析,
    返回 middle_json + base64 裁剪图片.
    """

    # ---------- 并发控制 ----------
    if _request_semaphore is not None:
        if _request_semaphore._value == 0:
            return JSONResponse(
                status_code=503,
                content={
                    "code": "2000503",
                    "message": "Server is at maximum capacity. Please try again later.",
                },
            )

    async def _do_parse():
        start_total = time.time()
        request_id = req.request_id or str(uuid.uuid4())

        # ---------- 参数校验 ----------
        if not req.messages or not req.messages[0].files:
            return JSONResponse(
                status_code=400,
                content={"code": "2000400", "message": "没有传入 PDF 文件数据"},
            )

        pdf_b64 = req.messages[0].files[0]
        msg_data = req.messages[0].data

        # ---------- 解码 PDF bytes ----------
        try:
            pdf_bytes = base64.b64decode(pdf_b64)
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={"code": "2000400", "message": f"PDF Base64 解码失败: {e}"},
            )

        if len(pdf_bytes) < 100:
            return JSONResponse(
                status_code=400,
                content={"code": "2000400", "message": "PDF 文件过小或为空"},
            )

        # ---------- 按页截取（B 已切割, 此处兼容性保留） ----------
        try:
            pdf_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(
                pdf_bytes, msg_data.start_page_id, msg_data.end_page_id
            )
        except Exception as e:
            logger.warning(f"page slice failed, using original bytes: {e}")

        # ---------- 准备临时目录 ----------
        work_dir = os.path.join(tempfile.gettempdir(), f"mineru_pa_{request_id}")
        os.makedirs(work_dir, exist_ok=True)

        try:
            pdf_name = request_id
            local_image_dir, local_md_dir = prepare_env(work_dir, pdf_name, "vlm")
            image_writer = FileBasedDataWriter(local_image_dir)

            # ---------- 确定 VLM backend ----------
            backend = os.getenv("MINERU_VLM_BACKEND", "vllm-async-engine")
            if backend == "auto-engine":
                backend = get_vlm_engine(inference_engine="auto", is_async=True)
            logger.info(f"[{request_id}] VLM backend={backend}, pdf_size={len(pdf_bytes)}")

            # ---------- VLM 推理 ----------
            os.environ["MINERU_VLM_FORMULA_ENABLE"] = os.getenv("MINERU_VLM_FORMULA_ENABLE", "True")
            os.environ["MINERU_VLM_TABLE_ENABLE"] = os.getenv("MINERU_VLM_TABLE_ENABLE", "True")
            # os.environ["MINERU_TABLE_MERGE_ENABLE"] = False

            middle_json, _infer_result = await aio_vlm_doc_analyze(
                pdf_bytes, image_writer=image_writer, backend=backend
            )

            # ---------- 收集裁剪图片 base64 ----------
            images_map: dict[str, str] = {}
            images_dir = os.path.join(local_md_dir, "images")
            if os.path.isdir(images_dir):
                for img_path in glob.glob(os.path.join(glob.escape(images_dir), "*.jpg")):
                    rel_key = f"images/{os.path.basename(img_path)}"
                    images_map[rel_key] = _encode_image_file(img_path)

            elapsed = round(time.time() - start_total, 2)
            page_count = len(middle_json.get("pdf_info", []))
            logger.info(f"[{request_id}] done: {page_count} pages, {len(images_map)} images, {elapsed}s")

            return JSONResponse(
                status_code=200,
                content={
                    "code": "2000000",
                    "message": "Success",
                    "info": {
                        "id": "mineru-parser",
                        "choices": [
                            {
                                "message": {
                                    "middle_json": middle_json,
                                    "images": images_map,
                                }
                            }
                        ],
                    },
                },
            )

        except Exception as e:
            logger.exception(f"[{request_id}] parse failed: {e}")
            return JSONResponse(
                status_code=500,
                content={"code": "2000500", "message": str(e)},
            )
        finally:
            _cleanup(work_dir)

    # ---------- 信号量保护 ----------
    if _request_semaphore is not None:
        async with _request_semaphore:
            return await _do_parse()
    else:
        return await _do_parse()


# ============================================================
# 健康检查
# ============================================================
@app.get("/health")
async def health():
    # return {"status": "ok", "version": __version__}
    return True


# ============================================================
# 启动入口
# ============================================================
if __name__ == "__main__":
    PORT = int(os.getenv("MINERU_PA_PORT", "80"))
    uvicorn.run(
        "mineru.cli.fast_api_pa:app",
        host="0.0.0.0",
        port=PORT,
        reload=False,
    )
