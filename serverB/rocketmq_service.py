# -*- coding: utf-8 -*-
"""
Created on 2022/10/25
@author: 
"""

import json
import traceback
import os
from rocketmq.client import Producer, Message
from util import logger
from configs.config import Config

config = Config.config
group_id = config.get('mq', 'group_id')
topic = config.get('mq', 'topic')

# RocketMQ环境配置字典
rocketmq_config = {
    'stg': {
        'server_address': '30.16.9.194:9876,30.16.9.199:9876',
        'virtual_account': 'V_PA003_ICORE_AAS'
    },
    'dev': {
        'server_address': '30.184.202.141:30105;30.184.202.139:30106',
        'virtual_account': 'ICORE_AAS'
    },
    'prd': {
        'server_address': '30.16.9.194:9876,30.16.9.199:9876',
        'virtual_account': 'V_PA003_ICORE_AAS'
    },
    'stg-xc': {
        'server_address': '30.184.202.141:30105;30.184.202.139:30106',
        'virtual_account': 'ICORE_AAS'
    },
    'prd-xc': {
        'server_address': '30.16.9.194:9876,30.16.9.199:9876',
        'virtual_account': 'V_PA003_ICORE_AAS'
    }
}


class RocketmqProducerService():
    """RocketMQ生产者服务类"""
    def __init__(self, env):
        self.env = env
        self.producer = self.init_producer()
        self.topic = topic

        logger.info(f'producer: {self.producer}')
        logger.info(f'topic: {self.topic}')
    

    def init_producer(self):
        """

        初始化生产者，连接RocketMQ集群
        
        返回:
            rocketmq.client.Producer: RocketMQ生产者实例
        """
        server_address = rocketmq_config.get(self.env, {}).get('server_address')
        virtual_account = rocketmq_config.get(self.env, {}).get('virtual_account')
        logger.info(f'group_id: {group_id}')
        
        producer = Producer(group_id)
        producer.set_namesrv_addr(server_address)
        producer.set_session_credentials(virtual_account, virtual_account, "FMQ")
        producer.start()
        logger.info('>[RocketMQ] 初始化生产者成功')
        return producer
    
    def send_msg(self, key, tag, body):
        """
        发送消息到RocketMQ
        
        参数:
            key: 消息键
            tag: 消息标签
            body: 消息体（字典格式）
            
        返回:
            bool: 消息发送是否成功
        """
        logger.info(f'key: {key}, tag: {tag}, body: {body}, env: {self.env}')
        
        is_send_ok = True
        try:
            # 将字典序列化为JSON字符串并转换成字节数据
            body_byte = json.dumps(body, ensure_ascii=False)
            msg = Message(self.topic)
            msg.set_keys(key)
            msg.set_tags(tag)
            msg.set_body(body_byte)
            
            ret = self.producer.send_sync(msg)
            logger.info(
                f'>> [RocketMQ] 发送消息成功 key: {key}, tag: {tag}, body: {body}. '
                f'返回 status: {ret.status}, msg_id: {ret.msg_id}, offset: {ret.offset}'
            )
        except Exception:
            err = traceback.format_exc()
            logger.error(f'>> [RocketMQ] 发送消息失败 key: {key}, tag: {tag}, body: {body}. error: {err}')
            is_send_ok = False
        
        return is_send_ok
    
mp_product = RocketmqProducerService(env=Config.env)