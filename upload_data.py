# -*- coding: utf-8 -*-
import time
import json
import base64
import socket

import arrow

from helper_kakou_v2 import Kakou
from helper_consul import ConsulAPI
from helper_kafka import KafkaData
from my_yaml import MyYAML
from my_logger import *


debug_logging('/home/logs/error.log')
logger = logging.getLogger('root')


class UploadData(object):
    def __init__(self):
        # 配置文件
        self.my_ini = MyYAML('/home/my.yaml').get_ini()

        # request方法类
        self.kk = None
        self.ka = KafkaData(**dict(self.my_ini['kafka']))
        self.con = ConsulAPI()
	
        # ID上传标记
        self.kk_name = dict(self.my_ini['kakou'])['name']
        self.step = dict(self.my_ini['kakou'])['step']
        self.kkdd = dict(self.my_ini['kakou'])['kkdd']

        self.uuid = None                    # session id
        self.session_time = time.time()     # session生成时间戳
        self.ttl = dict(self.my_ini['consul'])['ttl']               # 生存周期
        self.lock_name = dict(self.my_ini['consul'])['lock_name']   # 锁名

        self.local_ip = socket.gethostbyname(socket.gethostname())  # 本地IP

    def get_id(self):
        """获取上传id"""
        r = self.con.get_id()[0]
        return json.loads(base64.b64decode(r['Value']).decode()), r['ModifyIndex']

    def set_id(self, _id, modify_index):
        """设置ID"""
        if self.con.put_id(_id, modify_index):
            print(_id)

    def get_lost(self):
        """获取未上传数据id列表"""
        r = self.con.get_lost()[0]
        return json.loads(base64.b64decode(r['Value']).decode()), r['ModifyIndex']

    def post_lost_data(self):
        """未上传数据重传"""
        lost_list, modify_index = self.get_lost()
        if len(lost_list) == 0:
            return 0
        for i in lost_list:
            value = {'timestamp': arrow.now('PRC').format('YYYY-MM-DD HH:mm:ss'), 'message': i['message']}
            self.ka.produce_info(key=str(i['message']['id']), value=json.dumps(value))
            print('lost={0}'.format(i['message']['id']))
        self.ka.flush()
        lost_list = []
        if len(self.ka.lost_msg) > 0:
            for i in self.ka.lost_msg:
                lost_list.append(json.loads(i.value()))
            self.ka.lost_msg = []
        self.con.put_lost(json.dumps(lost_list))
        return len(lost_list)

    def post_info(self):
        """上传数据"""
        id, modify_index = self.get_id()
        info = self.kk.get_kakou(id+1, id+self.step, 1, self.step)
        # 如果查询数据为0
        if info['total_count'] == 0:
            return 0

        for i in info['items']:
            if i['kkdd_id'] is None or i['kkdd_id'] == '':
                i['kkdd_id'] = self.kkdd
            if i['hphm'] is None or i['hphm'] == '':
                i['hphm'] = '-'
            value = {'timestamp': arrow.now('PRC').format('YYYY-MM-DD HH:mm:ss'), 'message': i}
            self.ka.produce_info(key=str(i['id']), value=json.dumps(value))
        self.ka.flush()
        if len(self.ka.lost_msg) > 0:
            lost_list = []
            for i in self.ka.lost_msg:
                lost_list.append(json.loads(i.value()))
            self.ka.lost_msg = []
            self.con.put_lost(json.dumps(lost_list))
        # 设置最新ID
        self.set_id(info['items'][-1]['id'], modify_index)
        return info['total_count']

    def get_lock(self):
        """获取锁"""
        if self.uuid is None:
            self.uuid = self.con.put_session(self.ttl, self.lock_name)['ID']
            self.session_time = time.time()
        # 大于一定时间间隔则更新session
        t = time.time() - self.session_time
        if t > (self.ttl - 10):
            self.con.renew_session(self.uuid)
            self.session_time = time.time()
        l = self.con.get_lock(self.uuid, self.local_ip)
        # print(self.uuid, l)
        # session过期
        if l == None:
            self.uuid = None
            return False
        return l

    def get_service(self, service):
        """获取服务信息"""
        s = self.con.get_service(service)
        if len(s) == 0:
            return None
        h = self.con.get_health(service)
        if len(h) == 0:
            return None
        service_status = {}
        for i in h:
            service_status[i['ServiceID']] = i['Status']
        for i in s:
            if service_status[i['ServiceID']] == 'passing':
                return {'host': i['ServiceAddress'], 'port': i['ServicePort']}
        return None

    def main_loop(self):
        while 1:
            if not self.get_lock():
                time.sleep(2)
                continue
            if self.kk is not None and self.kk.status:
                try:
                    m = self.post_lost_data()
                    if m > 0:
                        time.sleep(0.5)
                        continue
                    n = self.post_info()
                    if n < self.step:
                        time.sleep(1)
                except Exception as e:
                    logger.exception(e)
                    time.sleep(15)
            else:
                try:
                    if self.kk is None or not self.kk.status:
                        s = self.get_service(self.kk_name)
                        if s is None:
                            time.sleep(5)
                            continue
                        self.kk = Kakou(**{'host':s['host'], 'port':s['port']})
                        self.kk.status = True
                except Exception as e:
                    logger.error(e)
                    time.sleep(1)
        
