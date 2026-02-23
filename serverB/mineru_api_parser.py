import os
import time
import requests
import traceback
from util import logger
from util import get_jobs_url, get_file_extension, write_json_file, get_jobs_file, write_res_to_json, upload_iobs  
from util import get_pdf_img_url_list

from configs.config import Config
from rocketmq_service import mp_product

from mineru_api_processor_parallel import parse_pdf_chunked_sync
from mineru_middle_to_content_list import convert_file


def minen_parser_send_file(input_param):
    res = {
        "code": 200,
        "msg": "成功",
        "data": {
            "reqId": "",
            "result": ""
        }
    }
    try:
        res = minen_process(input_param)
    except Exception as e:
        res['code'] = 400
        res['msg'] = str(e)  # ✅用str(e)
        logger.error(f'minen_process error: {e}', exc_info=True)
    
    request_id = input_param.get('request_id', '')
    # tag: minen_api_parser_split
    is_send_ok = mp_product.send_msg(request_id, "minen_api_parser_split", res)
    if is_send_ok:
        logger.info('request_id:{} mq发送Minen解析消息成功'.format(request_id))
    else:
        logger.info('request_id:{} mq发送Minen解析消息失败'.format(request_id))


def minen_process(input_param):
    request_id = input_param.get('request_id', '')
    dataId = input_param.get('dataId', '')
    file_name = input_param.get('file_name', '')
    pdfFileId = input_param.get('pdfFileId', '')
    is_split = input_param.get('is_split', '')
    
    res = {
        "code": 200,
        "msg": "成功",
        "data": {
            "reqId": request_id,
            "fileUrl": "",
            "fileId": "",
            "fileType": "IOBS",
            "fileName": ""
        }
    }

    logger.info(f'START MinerU PARSE: request_id:{{request_id}}')
    logger.info(f'dataId:{{dataId}}, file_name:{{file_name}}, pdfFileId:{{pdfFileId}}, is_split:{{is_split}}')

    try:
        # file_jobs_url = get_jobs_url(dataId)
        # logger.info(f'file_jobs_url:{file_jobs_url}')

        logger.info('Process PDF file via MinerU.')
        
        # 注释：MinerU解析调用大模型平台接口，返回合并后结果（已为JSON格式）
        middle_api_json_output = parse_pdf_chunked_sync(dataId, file_name)  # 这里已经是json的内容了
        if 'error_code' in middle_api_json_output:
            logger.info(f'middle_api_json_output:{middle_api_json_output}')

            res['code'] = 400
            res['msg'] = middle_api_json_output.get('res_msg', '')
            return res

        logger.info(f'file_name:{file_name}')
        pdf_name = os.path.splitext(os.path.basename(file_name))[0]
        logger.info(f'pdf_name:{pdf_name}')
        
        PIC_URL_TEMPLATE = Config.config.get('pic_url_temp', 'pic_url')
        logger.info(f'PIC_URL_TEMPLATE:{PIC_URL_TEMPLATE}')
        file_id, file_url, res_json_file_path = convert_file(
            pdf_name,
            middle_api_json_output,
            request_id,
            PIC_URL_TEMPLATE,
            is_split,
        )
        
        if file_id and file_url:
            res['code'] = 200
            res['data']['fileUrl'] = file_url
            res['data']['fileId'] = file_id
            res['data']['fileName'] = res_json_file_path  # json文件名
            logger.info('SUCCESS!!')
        else:
            res['code'] = 400
            res['msg'] = '结果上传iobs失败'
        # print(f'res:\n{res}')
        return res


    except Exception as e:
        res['code'] = 4006
        res['msg'] = str(e)  # ✘用str(e)
        logger.error(f'mineru process error: {e}', exc_info=True)
        return res