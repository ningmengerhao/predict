# -*- coding: utf-8 -*-
import pandas as pd
import json
import os
import string
import time

import requests
import schedule

from tool.factory.predict import *
from tool.logistics.predict import *
from tool.store.predict import *

os.environ['NUMEXPR_MAX_THREADS'] = '16'


# 定义logger日志输出
logger = logging.getLogger(__name__)
# 设置输出格式
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def send_msg(url, reminders, msg):
    headers = {'Content-Type': 'application/json;charset=utf-8'}
    data = {
        "msgtype": "text",  # 发送消息类型为文本
        "at": {
            "atMobiles": reminders,
            "isAtAll": False,
        },
        "text": {
            "content": msg,
        }
    }
    r = requests.post(url, data=json.dumps(data), headers=headers)
    return r.text


class Schedule:
    def __init__(self, tasks, task_names):
        self.tasks = tasks
        self.url = 'https://oapi.dingtalk.com/robot/send?access_token=f0202f28cdd53b4d96636d01daee75e0ce386a71bab94f8600cf247c1b6168ae'
        self.tasks_names = task_names

    def record(self, task):
        start_time = datetime.datetime.now()
        try:
            task.run()
            status = "成功"
        except Exception as e:
            status = '失败\n{}'.format(e)

        end_time = datetime.datetime.now()
        td = end_time - start_time
        hours, remainder = divmod(td.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        td_str = "%02d:%02d:%02d" % (hours, minutes, seconds)
        return [start_time.strftime("%H:%M"),
                end_time.strftime("%H:%M"),
                td_str,
                status]

    def run(self):
        result = []
        for task in self.tasks:
            result.append(self.record(task))
        result = pd.DataFrame(result, columns=["开始", '结束', '花费', '状态'])
        result["任务名称"] = self.tasks_names
        result = result.set_index("任务名称")
        send_msg(url=self.url, reminders=[
                 '17671749819'], msg="运营优化部任务公告\n" + result.to_string())
        return result


def task1(task_names):
    Schedule(tasks=[
        StoreSalePredict(),
        FactoryPredict(),
        Logistics(),
        DemandForecast()
    ],
        task_names=task_names).run()


task1(['销售预测', '工厂预测', '干线预测', '要货推荐'])
schedule.every().day.at("05:00").do(
    task1, task_names=['销售预测', '工厂预测', '干线预测', '要货推荐'])
while True:
    schedule.run_pending()
    time.sleep(1)
