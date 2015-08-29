#-*-coding:utf-8 -*-
__author__ = 'Defei'

from redis import Redis
from rq import Queue
from ParseCompany import parse_company

#连接redis
redis_conn = Redis(host='192.168.0.108', port=6379)
q = Queue(connection=redis_conn, async=True)  # 设置async为False则入队后会自己执行 不用调用perform

with open("su_companies.json", 'r') as f:
    i = 0
    for line in f:
        job = q.enqueue(parse_company, line.strip())
        i += 1
        print i, ":", job.id


