# _*_ coding:utf-8 _*_
# Author:Atlantis
# Date:2020-01-14

import json
import time
from kafka import KafkaProducer


class AsyncWrite(object):
    def __init__(self):
        self.kafka_ips = "xx.111.127.121:9092,xx.111.148.96:9092,xx.111.154.168:9092"
        self.producer = None

    def reconnect(self):
        config = {
            "bootstrap_servers": self.kafka_ips,
            "value_serializer": lambda v: json.dumps(v).encode('utf-8')
        }
        self.producer = KafkaProducer(**config)

    def write(self, info):
        self.producer.send("test", info)

    @staticmethod
    def test(info):
        time.sleep(5)
        with open("/Users/wqq/PycharmProjects/py-kafka-test/log/test.log", "a+") as f:
            f.write(info)
        print("日志写入完毕。。。")

