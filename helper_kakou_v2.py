# -*- coding: utf-8 -*-
import json

import requests
from requests.auth import HTTPBasicAuth


class Kakou(object):
    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.port = kwargs['port']

        self.headers = {'content-type': 'application/json'}

        self.status = False

    def get_stat(self, st, et, kkdd, fxbh):
        """根据时间,地点,方向获取车流量"""
        url = 'http://%s:%s/stat?q={"st":"%s","et":"%s","kkdd":"%s","fxbh":"%s"}' % (
            self.host, self.port, st, et, kkdd, fxbh)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)['count']
            else:
                self.status = False
                raise Exception(u'url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_kakou(self, start_id, end_id, page=1, per_page=100):
        """根据ID范围获取卡口信息"""
        url = 'http://%s:%s/cltx?q={"page":%s,"per_page":%s,"startid":%s,"endid":%s}' % (
            self.host, self.port, page, per_page, start_id, end_id)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception(u'url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_kakou_by_id(self, _id):
        """根据ID范围获取卡口信息"""
        url = 'http://{0}:{1}/cltx/{3}'.format(
            self.host, self.port, _id)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception(u'url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_maxid(self):
        """获取cltx表最大id值"""
        url = 'http://{0}:{1}/cltx/maxid'.format(
            self.host, self.port)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)['maxid']
            else:
                self.status = False
                raise Exception(u'url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

