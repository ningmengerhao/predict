import http.client
import time
import urllib.parse
import demjson3
import numpy as np
import pandas as pd
from chinese_calendar import *
from tool.dba import Oracle


class DaySpider:
    def __init__(self):
        self.oracle = Oracle()
        self.apipath = "api.djapi.cn"
        self.apiuri = "/wannianli/get"
        self.param = {"date": "", "cn_to_unicode": "1",
                      "token": "c73b30f1a6889794069f21d1d1c142a1", "datatype": "json"}
        self.headers = {
            "Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}

    def get_day_detail(self, date):
        tried = 10
        while tried:
            print(date)
            try:
                time.sleep(np.random.random() / 10)
                paramslist = {"date": date.strftime("%Y-%m-%d"),
                              "cn_to_unicode": "1",
                              "token": "c73b30f1a6889794069f21d1d1c142a1",
                              "datatype": "json"}
                params = urllib.parse.urlencode(paramslist)
                headers = {
                    "Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
                conn = http.client.HTTPConnection(self.apipath)
                conn.request('POST', self.apiuri, params, headers)
                data = conn.getresponse().read().decode("unicode-escape")
                data = demjson3.decode(data)["Result"]
                return pd.Series([data['week'],
                                  data['festivals_nl'].strip("/"),
                                  data['festivals_gl'].strip("/"),
                                  np.nan if '后' in data['jieqi'] else data['jieqi']])
            except Exception as e:
                tried -= 1
        return None

    def get_day_details(self, start_date, end_date):
        df_holiday = pd.DataFrame(pd.date_range(
            start_date, end_date), columns=["RPTDATE"])
        df_holiday[['WEEK', 'FESTIVALS_NL', 'FESTIVALS_GL', 'JIEQI']
                   ] = df_holiday["RPTDATE"].apply(self.get_day_detail)
        for var in ['FESTIVALS_NL', 'FESTIVALS_GL']:
            df_holiday[var] = df_holiday[var].replace(["护士节 / 母亲节"], ["母亲节"])
            df_holiday[var] = df_holiday[var].replace(["护士节/母亲节"], ["母亲节"])
            for holiday in ['除夕', '平安夜', '护士节', '消费者日', '禁毒日', '愚人节', '冬至节', '清明节', '植树节', '万圣节', '腊八节', '龙抬头', '建党节', '父亲节', '儿童节']:
                df_holiday[var] = df_holiday[var].replace([holiday], [np.nan])
        df_holiday["IN_LIEU"] = df_holiday["RPTDATE"].apply(
            is_in_lieu).replace([True, False], ['调休', np.nan])
        df_holiday["HOLIDAY"] = df_holiday["RPTDATE"].apply(
            is_holiday).replace([True, False], ['节假日', np.nan])
        df_holiday["WORKDAY"] = df_holiday["RPTDATE"].apply(
            is_workday).replace([True, False], ['工作日', np.nan])
        return df_holiday


df_holiday = DaySpider().get_day_details("2016-01-01", "2022-12-31")
Oracle().write_dataframe(key_col=["RPTDATE"],
                         table_name="ZCB_DAY_INFO", df=df_holiday)
