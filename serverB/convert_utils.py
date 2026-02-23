
import io
import os

import requests
import pypdfium2 as pdfium

from loguru import logger
from typing import List, Tuple


def convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, start_page_id=0, end_page_id=None):

    # 从字节数据加载PDF
    pdf = pdfium.PdfDocument(pdf_bytes)
    
    # 确定结束页
    end_page_id = end_page_id if end_page_id is not None and end_page_id >= 0 else len(pdf) - 1
    if end_page_id > len(pdf) - 1:
        logger.warning("end_page_id is out of range, use pdf docs length")
        end_page_id = len(pdf) - 1

    # 创建一个新的PDF文档
    output_pdf = pdfium.PdfDocument.new()

    # 选择要导入的页面索引
    page_indices = list(range(start_page_id, end_page_id + 1))

    # 从原PDF导入页面到新PDF
    output_pdf.import_pages(pdf, page_indices)

    # 将新PDF保存到内存缓冲区
    output_buffer = io.BytesIO()
    output_pdf.save(output_buffer)

    # 获取字节数值
    output_bytes = output_buffer.getvalue()

    pdf.close() # 关闭原PDF文档以释放资源
    output_pdf.close() # 关闭新PDF文档以释放资源

    return output_bytes


def prepare_pdf_bytes(pdf_bytes, start_page_id, end_page_id):
    """准备处理PDF字节数据"""
    new_pdf_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, start_page_id, end_page_id)
    return new_pdf_bytes





def normalize_bbox(bbox: List[float], page_size: Tuple[int, int]) -> List[float]:
    """
    归一化 bbox 坐标（保留 4 位小数）
    
    参数:
        bbox: [x0, y0, x1, y1] PDF 点坐标
        page_size: (page_width, page_height) 页面尺寸
    
    返回:
        [x0_norm, y0_norm, x1_norm, y1_norm] 归一化坐标（0-1 范围）
    """
    page_width, page_height = page_size
    x0, y0, x1, y1 = bbox

    return [
        round(x0 / page_width, 4),
        round(y0 / page_height, 4),
        round(x1 / page_width, 4),
        round(y1 / page_height, 4)
    ]


def bbox_to_tuple(bbox: List[float]) -> Tuple[float, float, float, float]:
    """将 bbox 转换为 tuple，用于字典 key"""
    return tuple(bbox)


def merge_spans_content(spans: List[dict]) -> str:
    """
    合并文本内容
    
    参数:
        spans: span 列表，每个包含 type 和 content
    
    返回:
        合并后的字符串
    """
    result = []
    for span in spans:
        content = span.get('content', '').strip()
        if content:
            result.append(content)

    # 用空格连接（保留原始格式）
    return ''.join(result)


def format_code_block(code_content: str, language: str = '') -> str:
    """
    使用三反引号包裹代码块
    
    参数:
        code_content: 代码内容
        language: 代码语言（可选）
    
    返回:
        格式化后的代码块
    """
    if not code_content.strip():
        return ''
    
    return f'```{language}\n{code_content}\n```'



def upload_iobs(file_path, iobs_url):

    with open(file_path, 'rb') as file:
        try:
            res = requests.post(iobs_url, files={'file': file}, timeout=20).json()
        except Exception as e:
            logger.warning(f'upload file to iobs fail: {e}. Try again.')
            try:
                res = requests.post(iobs_url, files={'file': file}, timeout=20).json()
            except Exception as e:
                logger.warning(f'upload file to iobs fail: {e}.')
                return "", ""
    
    file_url = res.get('data', {}).get('fileurl', '')
    file_id = res.get('data', {}).get('fileId', '')
    logger.info(f'upload file to iobs: {file_id}')
    
    return file_url, file_id