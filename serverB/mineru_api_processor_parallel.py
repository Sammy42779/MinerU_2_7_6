"""
mineru_api_processor_parallel.py  
MinerU 分块 PDF 解析客户端 - 模块化版本  
支持大文件分段解析与智能合并，避免超时/不稳定问题  
可作为模块导入使用：
from mineru.cli.test_fast_api_func import parse_pdf_chunked, ChunkedParseConfig  

parser_config = ChunkedParseConfig(  
    api_url="http://your-server:8000/file_parse",  
    backend="vlm-vllm-async-engine",  
    pages_per_chunk=20  
)  
results = await parse_pdf_chunked("file.pdf", parser_config)  
"""
import asyncio  
import json  
import os  
import time  
from pathlib import Path  
from typing import List, Dict, Any, Optional, Tuple  
from dataclasses import dataclass  

from util import get_iobs_url, get_iobs_file  

import aiohttp  
from loguru import logger  
from configs.config import Config  

# 签名  
import base64  
import hmac  
# 获取token  
from urllib.parse import urlencode  
import binascii  
import requests  
from Crypto.Hash import SHA256  
from Crypto.PublicKey import RSA  
from Crypto.Signature import PKCS1_v1_5  

try:  
    import pymupdf as fitz  # PyMuPDF  
except ImportError:  
    try:  
        import fitz  # 旧版本导入方式  
    except ImportError:  
        fitz = None  
        logger.warning("PyMuPDF not found, will try pypdf")  

if fitz is None:  
    try:  
        from pypdf import PdfReader  
    except ImportError:  
        logger.error("Neither PyMuPDF nor pypdf installed. Install with: pip install pymupdf")  
        exit(1)


# 获取鉴权签名
def get_gpt_sign(app_key, app_secret, requestTime):
    '''
    根据应用appkey和secret生成签名
    '''
    # 构造参数字典
    params = {
        "openApiRequestTime": str(requestTime),
        "appKey": app_key,
        "appSecret": app_secret
    }
    # URL编码并转为小写
    query_string = urlencode(params).lower() 
    # HMAC-SHA1签名
    hmac_obj = hmac.new(app_secret.encode('utf-8'), query_string.encode('utf-8'), 'sha1')
    # Base64编码
    sign = base64.b64encode(hmac_obj.digest()).decode('utf-8')
    return sign

# 获取网关签名
def get_sign(rsaPrivateKey, requestTime):
    '''
    根据RSA私钥生成签名
    '''
    print('requestTime: ', requestTime)
    
    # 十六进制私钥转二进制
    binary_key = binascii.a2b_hex(rsaPrivateKey)
    # 创建RSA私钥对象
    pkcs8_private_key = RSA.import_key(binary_key)  

    h = SHA256.new(requestTime.encode('utf-8'))
    signer = PKCS1_v1_5.new(pkcs8_private_key)
    signature = signer.sign(h).hex().upper()  # openApiSignature的值
    return signature

# —————— 配置类 ——————
@dataclass
class ChunkedParseConfig:
    """分块解析配置"""
    # API 配置
    logger.info(f'env:{Config.env}')
    api_url = Config.config.get('mineru_api', 'url_dialog')
    logger.info(f'mineru api url: {api_url}')
    scene_id = int(Config.config.get('mineru_api', 'scene_id'))

    # 分块与并发配置
    pages_per_chunk: int = 10
    max_concurrent_requests: int = 2
    request_timeout: int = 295
    max_retries: int = 3
    retry_delay_base: int = 2

    # MinerU 后端配置
    backend: str = "vlm-vllm-async-engine"
    parse_method: str = "auto"
    lang: str = "ch"
    formula_enable: bool = True
    table_enable: bool = True

    # 返回内容配置
    return_md: bool = False
    return_middle_json: bool = True
    return_content_list: bool = False
    return_images: bool = False

    # 可选: session_id 用于分块请求归档
    session_id: Optional[str] = None

    # 大模型平台应用appkey
    app_key = Config.config.get('mineru_api', 'app_key')
    # 大模型平台应用appsecret
    app_secret = Config.config.get('mineru_api', 'app_secret')
    # requestTime = str(int(time.time() * 1000))  # 当前请求的时间（毫秒时间戳），就是OpenApiRequestTime的值
    # 科技网关平台RSA密钥
    rsaPrivateKey = Config.config.get('mineru_api', 'rsaPrivateKey')


## 工具函数
def get_pdf_page_count(pdf_path: str) -> int:  
    """获取 PDF 总页数"""  
    if fitz:  
        doc = fitz.open(pdf_path)  
        page_count = doc.page_count  
        doc.close()  
        return page_count  
    else:  
        reader = PdfReader(pdf_path)  
        return len(reader.pages)
    
def generate_chunks(total_pages: int, chunk_size: int) -> List[Tuple[int, int]]:  
    """生成分块范围列表 [(start, end), ...]"""  
    chunks = []  
    for start in range(0, total_pages, chunk_size):  
        end = min(start + chunk_size - 1, total_pages - 1)  
        chunks.append((start, end))  
    return chunks

def create_headers(app_key, app_secret, rsaPrivateKey):  
    """工厂函数生成请求头"""  
    current_time = str(int(time.time()) * 1000)  
    return {  
        "Content-Type": "application/json",  
        "openApiCode": "AP1035059",  
        "openApiCredential": Config.config.get('mineru_api', 'openApiCredential'),  
        "openApiRequestTime": current_time,  
        "openApiSignature": get_sign(rsaPrivateKey, current_time),  
        "gpt_app_key": app_key,  
        "gpt_signature": get_gpt_sign(app_key, app_secret, current_time),  
    }


async def parse_chunk(  
    session: aiohttp.ClientSession,  
    dataId: str,  
    file_name: str,  
    start_page: int,  
    end_page: int,  
    chunk_idx: int,  
    semaphore: asyncio.Semaphore,  
    parser_config: ChunkedParseConfig  
) -> Dict[str, Any]:  
    
    # 在每次请求前动态生成签名
    headers = create_headers(
        parser_config.app_key,
        parser_config.app_secret,
        parser_config.rsaPrivateKey
    )


    """解析单个分块，带重试机制"""
    pdf_name = os.path.splitext(os.path.basename(file_name))[0]
    res_msg = ""

    for attempt in range(parser_config.max_retries):
        async with semaphore:  # 使用信号量控制并发
            try:
                logger.info(f"[Chunk {chunk_idx}] 开始解析 页 {start_page}-{end_page} (尝试 {attempt + 1}/{parser_config.max_retries})")
                
                # 1. 准备调用API的请求数据
                plain_param_dialog = {
                    "request_id": "test_eagw",
                    "model_type": "vision",
                    "messages": [
                        {
                            "files": [1],
                            "data": {
                                "dataId": dataId,
                                "file_name": file_name,
                                "start_page_id": start_page,
                                "end_page_id": end_page
                            }
                        }
                    ],
                    "scene_id": parser_config.scene_id,
                    'stream': False
                }
                # 2. 发送异步POST请求
                start_time = time.time()
                timeout = aiohttp.ClientTimeout(total=parser_config.request_timeout)
                logger.info(f"---mineru-api-url:{parser_config.api_url}---")

                async with session.post(parser_config.api_url, json=plain_param_dialog, headers=headers, timeout=timeout) as response:
                    response_text = await response.text()
                    response_json = await response.json()
                    # 3. 处理服务器繁忙(503)情况，进行延迟重试
                    if response.status == 503:
                        wait_time = parser_config.retry_delay_base ** attempt  # 指数退避
                        res_msg = "服务器繁忙 (503)"
                        result = await response.json()
                        resultCode = result['resultCode']
                        resultMsg = result['resultMsg']
                        logger.info(f"resultCode: {resultCode}, resultMsg: {resultMsg}")
                        logger.warning(f"[Chunk {chunk_idx}] 服务器繁忙(503)，{wait_time}秒后重试... ")
                        await asyncio.sleep(wait_time)
                        continue  # 进入下一次重试循环
                        
                    response.raise_for_status()
                    result = await response.json()
                    resultCode = result['resultCode']
                    resultMsg = result['resultMsg']
                    logger.info(f"resultCode: {resultCode}, resultMsg: {resultMsg}")

                    if resultCode == '0':
                        logger.info(f'[Chunk {chunk_idx}] code=0, 调用成功')
                        elapsed = time.time() - start_time

                        logger.success(f"[Chunk {chunk_idx}] ✅ 完成 | 耗时: {elapsed:.1f}s | Pages: {start_page}-{end_page}")
                        middle_json_url = result['choices'][0]['message'][pdf_name]['middle_json']

                        return {
                            'code': 200,
                            'chunk_idx': chunk_idx,
                            'start_page': start_page,
                            'end_page': end_page,
                            'result_ios_url': middle_json_url, # 注意：变量名与键名不一致，原文为 result_iobs_url
                            'elapsed': elapsed,
                            'res_msg': "解析成功"
                        }
                    else:
                        logger.error(f"{resultMsg})")
                        return {
                            'code': 205,
                            'res_msg': resultMsg
                        }
                    
            except asyncio.TimeoutError:
                res_msg = f"[Chunk {chunk_idx}] 大模型平台接口调用超时 (>{parser_config.request_timeout}s)"
                logger.error(res_msg)
                if attempt < parser_config.max_retries - 1:
                    wait_time = parser_config.retry_delay_base ** attempt
                    await asyncio.sleep(wait_time)
                    continue  # 进行下一次重试

            except aiohttp.ClientError as e:
                res_msg = f"[Chunk {chunk_idx}] 大模型平台接口调用网络错误：{e}"
                logger.error(res_msg)
                if attempt < parser_config.max_retries - 1:
                    wait_time = parser_config.retry_delay_base ** attempt
                    await asyncio.sleep(wait_time)
                    continue  # 进行下一次重试

            except Exception as e:  # 捕获其他未预料的异常
                res_msg = f"[Chunk {chunk_idx}] 解析失败：{e}"
                logger.error(res_msg)
                if attempt < parser_config.max_retries - 1:
                    wait_time = parser_config.retry_delay_base ** attempt
                    await asyncio.sleep(wait_time)
                    continue  # 进行下一次重试

    return {
        'code': 205,
        'res_msg': res_msg
    }


def merge_middle_json(chunk_results: List[Dict[str, Any]], backend: str = "unknown") -> Dict[str, Any]:
    """合并 middle.json（偏移 page_idx）"""
    merged = {
        "pdf_info": [],
        "_backend": backend,
        "_version_name": "merged"
    }

    for chunk in sorted(chunk_results, key=lambda x: x['chunk_idx']):
        logger.info(f'chunk: {chunk["chunk_idx"]}')
        middle_json_str = chunk['result_jobs_url']
        logger.info(f'middle_json_str: {middle_json_str}')  # 这是iobs url
        
        if not middle_json_str:
            logger.warning(f'Chunk {chunk["chunk_idx"]} middle_json 为空，跳过')
            continue
        
        if not middle_json_str.strip():
            logger.warning(f'Chunk {chunk["chunk_idx"]} middle_json 只有空白字符，跳过')
            continue
        
        try:
            logger.info('开始读取json内容')
            file_name = 'tmp.json'
            local_mid_json_path = get_iobs_file(middle_json_str, file_name)
            file_path = local_mid_json_path
            # 打开并读取文件
            with open(file_path, 'r', encoding='utf-8') as file:
                json_str = file.read()
            middle_data = json.loads(json_str)
            logger.info(f'type(middle_data): {type(middle_data)}')
            
            page_offset = chunk['start_page']
            logger.info(f'page_offset: {page_offset}')
            
            # 偏移每个页面的 page_idx
            for page_info in middle_data.get('pdf_info', []):
                page_info['page_idx'] += page_offset

                # 如果有 page_no 字段（pipeline 后端）
                if 'page_no' in page_info:
                    page_info['page_no'] += page_offset

                merged['pdf_info'].append(page_info)
            
            # 更新后端信息（使用第一个chunk的）
            if not merged.get('_backend') or merged['_backend'] == "unknown":
                merged['_backend'] = middle_data.get('_backend', backend)
            merged['_version_name'] = middle_data.get('_version_name', 'unknown')
        
        except json.JSONDecodeError as e:
            logger.error(f'Chunk {chunk["chunk_idx"]} middle_json 解析失败：{e}')

    return merged


async def parse_pdf_chunked(
    dataId: str,
    file_name: str,
    parser_config: Optional[ChunkedParseConfig] = None
) -> Dict[str, Any]:
    """主函数：分块解析 PDF 并合并结果

    Args:
        pdf_path: PDF 文件路径
        parser_config: 解析配置，如果为 None 则使用默认配置

    Returns:
        包含解析结果的字典：
        - md_content: Markdown 文本（如果启用）
        - middle_json: 合并的 middle.json（如果启用）
        - content_list: 合并的 content_list（如果启用）
        - metadata: 元数据（总页数、耗时等）
    """
    if parser_config is None:
        parser_config = ChunkedParseConfig()

    logger.info(f"📄 开始解析 PDF：{file_name}")

    # 1. 获取总页数
    page_count_start = time.time()
    pdf_file_iobs_url = get_iobs_url(dataId)
    pdf_file_path_str = get_iobs_file(pdf_file_iobs_url, file_name)
    total_pages = get_pdf_page_count(pdf_file_path_str)
    page_count_time = time.time() - page_count_start
    logger.info(f"📊 PDF 总页数：{total_pages}，计算耗时：{page_count_time:.2f}s")

    # 2. 生成分块
    chunks = generate_chunks(total_pages, parser_config.pages_per_chunk)
    logger.info(f"📦 分块策略：{len(chunks)} 个块，每块 {parser_config.pages_per_chunk} 页，并发度 {parser_config.max_concurrent_requests}")

    # 3. 并发解析
    start_time = time.time()
    semaphore = asyncio.Semaphore(parser_config.max_concurrent_requests)

    async with aiohttp.ClientSession() as session:
        tasks = [
            parse_chunk(session, dataId, file_name, start, end, idx, semaphore, parser_config)
            for idx, (start, end) in enumerate(chunks)
        ]

        chunk_results = await asyncio.gather(*tasks)

    logger.info(f"---chunk_results: {chunk_results}---")

    for chunk in chunk_results:
        code = chunk.get('code', '')
        if code != 200:
            logger.info(f'【--解析失败，不合并结果--】')
            return {
                'error_code': 205,
                'res_msg': chunk.get('res_msg', '')
            }
        
    end_time = time.time()
    logger.info(f'···解析时长：{end_time - start_time}秒···')

    # 4. 合并结果
    logger.info("开始合并结果...")
    if parser_config.return_middle_json:
        merged_middle = merge_middle_json(chunk_results, backend=parser_config.backend)
        logger.info(f"middle.json 总页数：{len(merged_middle.get('pdf_info', []))}")

    # 统计信息
    total_time = sum(c['elapsed'] for c in chunk_results)
    max_time = max(c['elapsed'] for c in chunk_results)
    logger.success(f"解析完成！总耗时：{total_time:.1f}s，最长单段：{max_time:.1f}s")

    return merged_middle


# ============= 同步调用封装 =============
def parse_pdf_chunked_sync(dataId: str, file_name: str, **kwargs) -> Dict[str, Any]:
    """同步调用封装，用于非异步环境
    
    Args:
        pdf_path: PDF 文件路径
        **kwargs: 传递给 ChunkedParseConfig 的参数
        
    Returns:
        解析结果字典
        
    Example:
        results = parse_pdf_chunked_sync(
            "file.pdf",
            api_url="http://localhost:8000/file_parse",
            pages_per_chunk=20,
            backend="pipeline"
        )
    """
    parser_config = ChunkedParseConfig(**kwargs)
    return asyncio.run(parse_pdf_chunked(dataId, file_name, parser_config))