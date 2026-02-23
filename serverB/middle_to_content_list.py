"""
Middle JSON 转 Content List 模块

核心规则:
1. 只合并同一 bbox 内的内容
2. 不同 bbox 的元素必须独立
3. bbox 归一化（保留 4 位小数）
4. HTML 表格额外生成 Markdown 格式
5. 代码块使用三反引号包裹
"""

import json
from typing import List, Dict, Any, Tuple
from loguru import logger
import os

from convert_utils import normalize_bbox, bbox_to_tuple, merge_spans_content, format_code_block
from html_to_markdown import html_table_to_markdown
from util import write_res_to_json, upload_iobs

class MiddleToContentListConverter:
    """Middle JSON 到 Content List 的转换器"""
    
    def __init__(self, PIC_URL_TEMPLATE: str = ''):
        self.PIC_URL_TEMPLATE = PIC_URL_TEMPLATE
    
    
    def _upload_and_format_image(self, image_path: str) -> dict:
        """上传图片到 IOBS 并返回 URL 和 Markdown 格式
        
        参数:
            image_path: 相对图片路径（如 'images/xxx.jpg'）
        
        返回:
            包含 iobs_url 和 markdown_image 的字典
        """
        # 获取文件名作为 ID
        #
        file_id = image_path.split('/')[5].split('?')[0]  

        file_url = self.PIC_URL_TEMPLATE.format(file_id)

        # 生成markdown格式
        markdown_image = f"![]({file_url})"
        
        return {
            'image_iobs_url': file_url,
            'image_markdown': markdown_image,
            'image_file_id': file_id
        }

    def convert(self, middle_json: Dict[str, Any], group_by_page: bool = False) -> List[Any]:
        """
        转换 middle.json 为 content_list.json
        
        参数:
            middle_json: 完整的 middle.json 数据
            group_by_page: 是否按页面分组。False: 返回 List[Dict]；True: 返回 List[List[Dict]]
        
        返回:
            content_list: 内容列表（按页分组时为二维列表）
        """
        if group_by_page:
            # 按页分组：返回 List[List[Dict]]
            pages_content = []
        else:
            # 不分组：返回 List[Dict]
            content_list = []
        
        pdf_info = middle_json.get('pdf_info', [])
        
        for page_info in pdf_info:
            page_idx = page_info.get('page_idx', 0)
            page_size = tuple(page_info.get('page_size', [595, 842]))
            
            # 按页分组时，为每页创建独立的列表
            if group_by_page:
                page_content = []
            
            # 处理所有 para_blocks（不再合并不同 bbox 的内容）
            for block in page_info.get('para_blocks', []):
                content_item = self._process_block(block, page_size, page_idx)
                if content_item:
                    # LIST 类型返回列表，需要展开
                    if isinstance(content_item, list):
                        if group_by_page:
                            page_content.extend(content_item)
                        else:
                            content_list.extend(content_item)
                    else:
                        if group_by_page:
                            page_content.append(content_item)
                        else:
                            content_list.append(content_item)
            
            # 处理 discarded_blocks
            for block in page_info.get('discarded_blocks', []):
                content_item = self._process_block(block, page_size, page_idx)
                if content_item:
                    # LIST 类型返回列表，需要展开
                    if isinstance(content_item, list):
                        if group_by_page:
                            page_content.extend(content_item)
                        else:
                            content_list.extend(content_item)
                    else:
                        if group_by_page:
                            page_content.append(content_item)
                        else:
                            content_list.append(content_item)
            
            # 按页分组时，将当前页的内容添加到结果中
            if group_by_page:
                pages_content.append(page_content)
        
        return pages_content if group_by_page else content_list
    
    def _process_block(
        self, 
        block: Dict[str, Any], 
        page_size: Tuple[int, int],
        page_idx: int
    ) -> Dict[str, Any]:
        """
        处理单个 block
        
        关键规则：不再合并不同 bbox 的 caption/footnote/body
        """
        block_type = block.get('type')
        bbox = block.get('bbox')
        
        if not bbox:
            return None
        
        # 归一化 bbox
        norm_bbox = normalize_bbox(bbox, page_size)
        # with open('/data/RAG/MinerU/mineru/backend/vlm/content_process/debug_bbox.txt', 'a') as f:
        #     print(f'bbox:{bbox}, page_size : {page_size}, norm_bbox:{norm_bbox}')
        #     f.write(f'bbox:{bbox}, page_size : {page_size}, norm_bbox:{norm_bbox}\n')
        
        # 根据类型分发处理
        if block_type in ['text', 'title', 'phonetic', 'ref_text']:
            return self._process_text_block(block, norm_bbox, page_idx)
        
        elif block_type == 'interline_equation':
            return self._process_equation_block(block, norm_bbox, page_idx)
        
        elif block_type == 'image_body':
            return self._process_image_body(block, norm_bbox, page_idx)
        
        elif block_type == 'image_caption':
            return self._process_image_caption(block, norm_bbox, page_idx)
        
        elif block_type == 'image_footnote':
            return self._process_image_footnote(block, norm_bbox, page_idx)
        
        elif block_type == 'table_body':
            return self._process_table_body(block, norm_bbox, page_idx)
        
        elif block_type == 'table_caption':
            return self._process_table_caption(block, norm_bbox, page_idx)
        
        elif block_type == 'table_footnote':
            return self._process_table_footnote(block, norm_bbox, page_idx)
        
        elif block_type == 'image':
            # 处理通用 image 类型：遍历 blocks，为每个 sub_block 创建独立项
            image_items = []
            for sub_block in block.get('blocks', []):
                sub_type = sub_block.get('type')
                sub_bbox = sub_block.get('bbox')
                if not sub_bbox:
                    continue
                sub_norm_bbox = normalize_bbox(sub_bbox, page_size)
                
                if sub_type == 'image_body':
                    item = self._process_image_body(sub_block, sub_norm_bbox, page_idx)
                elif sub_type == 'image_caption':
                    item = self._process_image_caption(sub_block, sub_norm_bbox, page_idx)
                elif sub_type == 'image_footnote':
                    item = self._process_image_footnote(sub_block, sub_norm_bbox, page_idx)
                else:
                    continue
                
                if item:
                    image_items.append(item)
            
            return image_items if image_items else None
        
        elif block_type == 'table':
            # 处理通用 table 类型：遍历 blocks，为每个 sub_block 创建独立项
            table_items = []
            for sub_block in block.get('blocks', []):
                sub_type = sub_block.get('type')
                sub_bbox = sub_block.get('bbox')
                if not sub_bbox:
                    continue
                sub_norm_bbox = normalize_bbox(sub_bbox, page_size)
                
                if sub_type == 'table_body':
                    item = self._process_table_body(sub_block, sub_norm_bbox, page_idx)
                elif sub_type == 'table_caption':
                    item = self._process_table_caption(sub_block, sub_norm_bbox, page_idx)
                elif sub_type == 'table_footnote':
                    item = self._process_table_footnote(sub_block, sub_norm_bbox, page_idx)
                else:
                    continue
                
                if item:
                    table_items.append(item)
            
            return table_items if table_items else None
        
        elif block_type == 'code':
            # 处理通用 code 类型：遍历 blocks，为每个 sub_block 创建独立项
            # 检查 sub_type 以区分 code 和 algorithm
            para_sub_type = block.get('sub_type', 'code')  # 默认为 code
            is_algorithm = (para_sub_type == 'algorithm')
            
            code_items = []
            for sub_block in block.get('blocks', []):
                sub_type = sub_block.get('type')
                sub_bbox = sub_block.get('bbox')
                if not sub_bbox:
                    continue
                sub_norm_bbox = normalize_bbox(sub_bbox, page_size)
                
                if sub_type == 'code_body':
                    # 获取 guess_lang 从父 block
                    sub_block_with_lang = {**sub_block, 'guess_lang': block.get('guess_lang', '')}
                    item = self._process_code_body(sub_block_with_lang, sub_norm_bbox, page_idx, is_algorithm)
                elif sub_type == 'code_caption':
                    item = self._process_code_caption(sub_block, sub_norm_bbox, page_idx, is_algorithm)
                else:
                    # 未知子块类型，记录警告并透传
                    logger.warning(f"Unknown code sub_block type: {sub_type}, will create generic item")
                    item = {
                        'type': 'algorithm' if is_algorithm else 'code',
                        'sub_type': f'unknown_{sub_type}',
                        'bbox': sub_norm_bbox,
                        'page': page_idx,
                        'content': self._extract_text_content(sub_block)
                    }
                
                if item:
                    code_items.append(item)
            
            return code_items if code_items else None
        
        elif block_type == 'code_body':
            return self._process_code_body(block, norm_bbox, page_idx)
        
        elif block_type == 'code_caption':
            return self._process_code_caption(block, norm_bbox, page_idx)
        
        elif block_type == 'list':
            return self._process_list_block(block, page_size, page_idx)
        
        elif block_type in ['header', 'footer', 'page_number', 'aside_text', 'page_footnote']:
            return self._process_page_element(block, norm_bbox, page_idx)
        
        else:
            # 未知类型，记录日志
            logger.warning(f"Unknown block type: {block_type}")
            return None
    
    def _extract_text_content(self, block: Dict[str, Any]) -> str:
        """提取 block 中的文本内容（合并所有 lines 和 spans）
        
        注意：会根据 span 的 type 进行特殊处理：
        - inline_equation: 用 $ 包裹
        - text: 直接使用
        """
        content_parts = []
        
        for line in block.get('lines', []):
            for span in line.get('spans', []):
                span_type = span.get('type', 'text')
                text = span.get('content', '').strip()
                
                if not text:
                    continue
                
                # 根据 span 类型处理
                if span_type == 'inline_equation':
                    # 行内公式用单个 $ 包裹
                    content_parts.append(f'${text}$')
                else:
                    # 普通文本直接添加
                    content_parts.append(text)
        
        return ' '.join(content_parts)
    
    def _process_text_block(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int
    ) -> Dict[str, Any]:
        """处理文本块（text/title/phonetic/ref_text）"""
        block_type = block.get('type')
        text_content = self._extract_text_content(block)
        
        if not text_content:
            return None
        
        # 处理标题类型
        if block_type == 'title':
            level = self._get_title_level(block)
            
            if level == 0:
                # level = 0 时按普通文本处理
                content_item = {
                    'type': 'text',
                    'bbox': norm_bbox,
                    'page': page_idx,
                    'content': text_content
                }
            else:
                # level >= 1 时转换为 Markdown 标题
                markdown_title = f"{'#' * level} {text_content}"
                content_item = {
                    'type': 'title',
                    'bbox': norm_bbox,
                    'page': page_idx,
                    'content': markdown_title,
                    'level': level
                }
        else:
            # 普通文本处理
            content_item = {
                'type': 'text',
                'bbox': norm_bbox,
                'page': page_idx,
                'content': text_content
            }
        
        return content_item
    
    def _get_title_level(self, block: Dict[str, Any]) -> int:
        """获取标题层级（参考原始实现）"""
        title_level = block.get('level', 1)
        if title_level > 4:
            title_level = 4
        elif title_level < 1:
            title_level = 0
        return title_level
    

    def _process_equation_block(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int
    ) -> Dict[str, Any]:
        """处理行间公式"""
        # 提取 LaTeX 内容
        latex_content = self._extract_text_content(block)
        
        # 提取图片路径（如果有）
        image_path = None
        for line in block.get('lines', []):
            for span in line.get('spans', []):
                if span.get('image_path'):
                    image_path = span['image_path']
                    break
        
        # 上传图片并添加 IOBS URL
        if image_path:
            upload_result = self._upload_and_format_image(image_path)

        content_item = {
            'type': 'equation',
            'bbox': norm_bbox,
            'page': page_idx,
            'latex_content': latex_content,
            'format': 'latex',
            'content': upload_result['image_markdown']
        }
        content_item.update(upload_result)
        
        return content_item
    
    def _process_image_body(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int
    ) -> Dict[str, Any]:
        """处理图片主体（不再与 caption/footnote 合并）"""
        # 提取图片路径（从 lines[*]['spans'][*] 中提取）
        image_path = None
        for line in block.get('lines', []):
            for span in line.get('spans', []):
                if span.get('image_path'):
                    image_path = span['image_path']
                    break
            if image_path:
                break
        
        if not image_path:
            return None
        
        # 上传图片并获取 IOBS URL
        upload_result = self._upload_and_format_image(image_path)
        
        return {
            'type': 'image',
            'sub_type': 'body',
            'bbox': norm_bbox,
            'page': page_idx,
            'content': upload_result['image_markdown'],  # Markdown 格式的图片链接
            **upload_result  # 包含 iobs_url, markdown_image
        }

    def _process_image_caption(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int
    ) -> Dict[str, Any]:
        """处理图片标题（独立 block）"""
        caption_text = self._extract_text_content(block)
        
        if not caption_text:
            return None
        
        return {
            'type': 'image',
            'sub_type': 'caption',
            'bbox': norm_bbox,
            'page': page_idx,
            'content': caption_text
        }
    
    def _process_image_footnote(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int
    ) -> Dict[str, Any]:
        """处理图片脚注（独立 block）"""
        footnote_text = self._extract_text_content(block)
        
        if not footnote_text:
            return None
        
        return {
            'type': 'image',
            'sub_type': 'footnote',
            'bbox': norm_bbox,
            'page': page_idx,
            'content': footnote_text
        }
    
    def _process_table_body(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int
    ) -> Dict[str, Any]:
        """处理表格主体（包含 HTML 和 Markdown 格式）"""
        # 提取 HTML 内容（从 lines[*]['spans'][*] 中提取）
        html_content = None
        image_path = None
        
        for line in block.get('lines', []):
            for span in line.get('spans', []):
                if span.get('html'):
                    html_content = span['html']
                if span.get('image_path'):
                    image_path = span['image_path']
        
        if not html_content and not image_path:
            return None
        
        content_item = {
            'type': 'table',
            'sub_type': 'body',
            'bbox': norm_bbox,
            'page': page_idx
        }
        
        # 添加 HTML 格式
        if html_content:
            content_item['html'] = html_content
            
            # 转换为 Markdown 格式
            markdown_content = html_table_to_markdown(html_content)
            if markdown_content:
                content_item['markdown'] = markdown_content
        
        # 添加图片路径
        # 上传图片并添加 IOBS URL
        if image_path:
            upload_result = self._upload_and_format_image(image_path)
            content_item.update(upload_result)
        
        return content_item

    def _process_table_caption(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int
    ) -> Dict[str, Any]:
        """处理表格标题（独立 block）"""
        caption_text = self._extract_text_content(block)
        
        if not caption_text:
            return None
        
        return {
            'type': 'table',
            'sub_type': 'caption',
            'bbox': norm_bbox,
            'page': page_idx,
            'content': caption_text
        }
    
    def _process_table_footnote(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int
    ) -> Dict[str, Any]:
        """处理表格脚注（独立 block）"""
        footnote_text = self._extract_text_content(block)
        
        if not footnote_text:
            return None
        
        return {
            'type': 'table',
            'sub_type': 'footnote',
            'bbox': norm_bbox,
            'page': page_idx,
            'content': footnote_text
        }
    
    def _process_code_body(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int,
        is_algorithm: bool = False
    ) -> Dict[str, Any]:
        """处理代码主体（使用三反引号包裹）
        
        参数:
            is_algorithm: 是否为算法类型（而非普通代码）
        """
        code_content = self._extract_text_content(block)
        
        if not code_content:
            return None
        
        # 获取代码语言（从父级 block 获取）
        language = block.get('guess_lang', '')
        
        # 使用三反引号包裹
        formatted_code = format_code_block(code_content, language)
        
        return {
            'type': 'algorithm' if is_algorithm else 'code',
            'sub_type': 'body',
            'bbox': norm_bbox,
            'page': page_idx,
            'content': formatted_code,
            'language': language
        }
    
    def _process_code_caption(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int,
        is_algorithm: bool = False
    ) -> Dict[str, Any]:
        """处理代码标题（独立 block）
        
        参数:
            is_algorithm: 是否为算法类型（而非普通代码）
        """
        caption_text = self._extract_text_content(block)
        
        if not caption_text:
            return None
        
        return {
            'type': 'algorithm' if is_algorithm else 'code',
            'sub_type': 'caption',
            'bbox': norm_bbox,
            'page': page_idx,
            'content': caption_text
        }
    
    def _process_list_block(
        self, 
        block: Dict[str, Any], 
        page_size: Tuple[int, int],
        page_idx: int
    ) -> List[Dict[str, Any]]:
        """处理列表块 - 为每个 list item 创建独立的内容项
        
        注意：返回列表，每个 list item 保持独立的 bbox
        """
        list_items = []
        list_type = block.get('sub_type', 'text')
        
        # 为每个 sub_block 创建独立的 list item
        for item_block in block.get('blocks', []):
            item_text = self._extract_text_content(item_block)
            if not item_text:
                continue
            
            # 获取该 item 的独立 bbox
            item_bbox = item_block.get('bbox')
            if not item_bbox:
                continue
            
            item_norm_bbox = normalize_bbox(item_bbox, page_size)
            
            list_items.append({
                'type': 'list',
                'sub_type': 'item',
                'bbox': item_norm_bbox,
                'page': page_idx,
                'content': item_text,
                'list_type': list_type
            })
        
        return list_items if list_items else []
    
    def _process_page_element(
        self, 
        block: Dict[str, Any], 
        norm_bbox: List[float],
        page_idx: int
    ) -> Dict[str, Any]:
        """处理页面元素（header/footer/page_number 等）"""
        block_type = block.get('type')
        content = self._extract_text_content(block)
        
        if not content:
            return None
        
        return {
            'type': 'page_element',  # 从 MinerU VLM 的 middle.json 文件中的 discarded_blocks 获取
            'sub_type': block_type,
            'bbox': norm_bbox,
            'page': page_idx,
            'content': content
        }


def convert_middle_to_content_list(
    middle_json: Dict[str, Any],
    group_by_page: bool = True,
    PIC_URL_TEMPLATE: str = ""
) -> List[Any]:
    """
    便捷函数：将 middle.json 转换为 content_list.json
    
    返回:
        content_list: 内容列表（按页分组时为二维列表）
    """
    converter = MiddleToContentListConverter(PIC_URL_TEMPLATE)
    return converter.convert(middle_json, group_by_page=group_by_page)



def convert_file(
    pdf_name,
    middle_json,
    request_id: str,
    PIC_URL_TEMPLATE: str,
    is_split: int,
    save_type='iobs'
):
    """
    从文件读取 middle.json 并转换为 content_list.json
    """

    if is_split == 1:
        group_by_page = True
    else:
        group_by_page = False

    # 调用转换器进行核心格式转换
    content_list = convert_middle_to_content_list(
        middle_json=middle_json, group_by_page=group_by_page,
        PIC_URL_TEMPLATE=PIC_URL_TEMPLATE
    )
    # 生成结果文件名
    res_path = f"./{request_id}_{pdf_name}.json"
    # 根据保存类型决定输出方式
    if save_type == "iobs":
        # 1. 先写入本地临时JSON文件
        res_json_file_path = write_res_to_json(res_path, content_list)
        logger.info(f'res_json_file_path: {res_json_file_path}')
        # 2. 上传至IOBS存储服务
        file_id, file_url = upload_iobs(res_json_file_path)
        logger.info(f'res_json_file_path: {file_id}, file_url: {file_url}')
        # 返回IOBS文件ID、URL及本地路径
        return file_id, file_url, res_json_file_path
    else:
        # 保存到本地文件
        with open(res_path, 'w', encoding='utf-8') as f:
            json.dump(content_list, f, ensure_ascii=False, indent=2)
        logger.info(f"Converted {len(content_list)} items, saved to {res_path}")
        # 返回空ID和URL及本地路径
        return "", "", res_path