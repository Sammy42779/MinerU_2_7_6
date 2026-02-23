import json
import os
import threading

from flask import Flask, request
from util import logger
from mineru_api_parser import mineru_parser_send_file

from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor

# 定义队列和线程池
in_queue = Queue()
executor = ThreadPoolExecutor(max_workers=2)

# 配置Flask端口（可从环境变量读取）
flask_port = os.environ.get('FLASK_PORT', 29475)

# 初始化Flask应用
application = Flask(__name__)
application.config["JSON_AS_ASCII"] = False

# 健康检查接口
@application.route("/health", methods=['GET', 'POST'])
def health():
    #
    return "Hello"

# 主服务接口：文件解析与分割
@application.route("/mineru_parser_split", methods=["post"])
def ppt_doc_parser_split():
    """接口函数，主要描述入参到出参总体计算过程
    
    Args:
    
    Returns:

    """# 初始化返回结构
    res = {
        "code": 200,
        "msg": 'success',
        "data": {
            "requestId": ""
        }
    }
    # 内部函数：参数检查
    def para_check(input_param):

        para_res = {
            "code": 200,
            "msg": "success"
        }
        # 1. 检查必要字段是否存在
        required_keys = {'request_id', 'dataId', 'file_name', 'pdfFileId', 'is_split'}
        
        
        missing_keys = required_keys - input_param.keys()
        if missing_keys:
            para_res['code'] = 400
            para_res['msg'] = '缺少必要字段: ' + '、'.join(missing_keys)
            return para_res
        
        # 2. 检查字段值是否为空（逻辑存在，但主要检查被注释）
        no_values = []

        for key in required_keys:
            logger.info(f'check key: {key}')
            value = input_param.get(key, "")
            if not value or (isinstance(value, str) and value.strip() == ""):
                no_values.append(key)
        # 注：此处存在对空值检查的代码，但关于`pdfImgIdList`字段的判断已被注释
        
        
        
        
        return para_res
    # 获取并处理请求参数
    input_param = json.loads(request.get_data().decode('utf-8'))
    logger.info('ppt_doc_parse_split input para:{}'.format(input_param))
    para_res = para_check(input_param)
    res['data']['requestId'] = input_param.get('request_id', '')
    if para_res['code'] == 400:
        res['code'] = para_res['code']
        res['msg'] = para_res['msg']
        return res
    # 检查通过，将任务放入队列异步处理
    in_queue.put(("mineru_parser_split", input_param))
    return res
# 后台任务处理函数
def process_requests():
    while True:
        try:
            serve_type, input_param = in_queue.get()
            if input_param is not None:  # 提交任务到线程池执行
                future = executor.submit(mineru_parser_send_file, input_param)
        except Exception as e:
            logger.info(e.args)

# 程序入口
if __name__ == "__main__":
    # 启动后台处理线程
    workers = list()
    workers.append(threading.Thread(target=process_requests))
    workers[-1].setDaemon(True)
    workers[-1].start()
    
    # 启动Flask服务
    logger.info(f'>> PPT解析、切片、数据发送mq服务启动-mineru-parser')
    application.run("0.0.0.0", port=int(flask_port), debug=False, threaded=True)