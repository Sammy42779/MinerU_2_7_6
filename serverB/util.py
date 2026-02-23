# -*- coding: utf-8 -*-
import os
import time
import json
import logging
import re
import requests
from Configs.config import Config

config = Config.config




def write_json_file(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return


def get_file_extension(file_name):
    file_extension = os.path.splitext(file_name)[1]

    return file_extension


def create_logger(logger_name, path=None, level=logging.INFO, record_format=None):
    """
    创建日志记录器
    
    Args:
        logger_name: logger对象的名称
        path: 日志存储路径，默认为空，日志存储磁盘
        level: 日志等级，默认为 logging.INFO
        record_format: 记录格式，默认 None
    Returns:
        logger对象
    """
    if record_format is None:
        record_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logger = logging.getLogger(logger_name)
    if not logger.handlers:
        logger.setLevel(level)
        
        formatter = logging.Formatter(record_format)
        
        if path is not None:
            # FileHandler
            fileHandler = logging.FileHandler(path, encoding='utf-8')
            fileHandler.setFormatter(formatter)
            logger.addHandler(fileHandler)
        
        # StreamHandler
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)
    
    return logger

logger = create_logger(logger_name=config.get('log', 'name'))

def get_iobs_url(iobsKey, bucketName=None):
    iobs_get_url = config.get('iobs', 'get_url')
    t1 = time.time()
    params = {
        "iobsKey": iobsKey,
        "urlType": 3
    }
    if bucketName:
        params['bucketName'] = bucketName
    
    res = requests.post(iobs_get_url, json=params)
    t2 = time.time()
    logger.info("{}获取iobs url耗时: {}".format(iobsKey, t2-t1))
    if res.status_code == 200 and res.json()['code'] == 200:
        url = res.json().get('data', {}).get('iobsUrl', '')
        return url
    else:
        return ""



def get_iobs_file(iobs_url, file_name):
    t1 = time.time()
    max_retries = 2
    retry_delay = 1
    file_path_name = ""
    logger.info('iobs_url: {}'.format(iobs_url))
    for attempt in range(max_retries):
        res = requests.get(iobs_url, timeout=(5, 10))
        logger.info(f'get_iobs_file status code={res.status_code}')
        if res.status_code == 200:
            content = res.content
            file_path = os.getcwd()
            file_path_name = os.path.join(file_path, file_name)
            with open(file_path_name, 'wb') as f:
                f.write(content)
            break
        else:
            time.sleep(retry_delay)
    if not file_path_name:
        logger.info('文件: {}下载失败'.format(file_name))
        return ""
    else:
        t2 = time.time()
        logger.info('文件: {}从iobs下载耗时: {}'.format(file_name, t2-t1))
        return file_path_name


def write_res_to_json(file_name, json_res):
    file_name_str = os.path.splitext(file_name)[0]
    json_file_path = file_name_str + '.json'
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(json_res, f, ensure_ascii=False, indent=4)

    return json_file_path


def upload_iobs(file_path, max_retries=2):
    
    uploadFileUrl = config.get('upload', 'get_url')
    logger.info(f'ENV: {Config.env}, uploadFileUrl: {uploadFileUrl}')
    







    for attempt in range(max_retries):
        try:
            with open(file_path, 'rb') as file:
                file_param = {'file': (os.path.basename(file_path), file)}
                res = requests.post(uploadFileUrl, files=file_param)
            logger.info('上传成功')
            file_url = res.get('data', {}).get('fileUrl', '')
            file_id = res.get('data', {}).get('fileId', '')

            if file_url and file_id:
                logger.info(f'文件URL: {file_url}, 文件ID: {file_id}')
                return file_url, file_id
            else:
                logger.error('上传失败，响应中缺少fileUrl或fileId')
                return None, None
        except Exception as e:
            logger.error(f'上传失败，尝试次数: {attempt + 1}, 错误: {e}')
    return None, None


def unwrap_markdown_fence(text):
    """
    检查文本是否被Markdown代码块(```)包裹，如果是，则提取其内容。
    支持```markdown```和```...```两种形式。
    """

    stripped_text = text.strip()
    # 正则表达式匹配被```包裹的内容
    # ^```(?:markdown)?\s* 匹配开头的```或```markdown加上可能的空白
    # (.*?) 匹配中间的内容(非贪婪)
    # \s*```$ 匹配结尾的空白加上```
    match = re.match(r'^```(?:markdown)?\s*(.*?)\s*```$', stripped_text, re.DOTALL | re.IGNORECASE)
    
    if match:
        # 如果匹配成功，返回捕获组1的内容，即代码块内部的文本
        return match.group(1).strip()
    
    # 如果不匹配(例如只有开头没有结尾，或者根本没有包裹)，尝试只去除开头的标记
    # 有时候模型会输出```markdown但忘记输出结尾的```
    if stripped_text.startswith('```markdown'):
        # 移除开头的```markdown，保留剩余部分
        return stripped_text.replace('```markdown', '', 1).strip()
    elif stripped_text.startswith('```'):
        # 移除开头的```，保留剩余部分
        return stripped_text.replace('```', '', 1).strip()
    
    return text

def get_pdf_img_url_list(pdf_img_id_list):
    """
    获取PDF图片URL列表
    
    Args:
        pdf_img_id_list: PDF图片ID列表
        
    Returns:
        图片URL列表
    """
    generated_images = []  # 记录生成的JPG路径
    
    for img_id in pdf_img_id_list:
        img_iobs_url = get_iobs_url(img_id)
        if img_iobs_url:
            # 如果是空字符串，则不保存，外层判断数量是否一致
            generated_images.append(img_iobs_url)
    
    return generated_images
