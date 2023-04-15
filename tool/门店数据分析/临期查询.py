# -*- coding: utf-8 -*-
import schedule
import os
import logging
import time
import pandas as pd
import numpy as np
import datetime
from tool.dba import oracle97, oracle155, Oracle
import requests
import json
from itertools import product
from datetime import timedelta
os.environ['NUMEXPR_MAX_THREADS'] = '16'
# 定义logger日志输出
logger = logging.getLogger(__name__)
# 设置输出格式
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

path = 'data/门店盘点政策/'
try:
    os.makedirs(path)
except:
    print(1)
date_range = pd.date_range(start=datetime.date.today(
) - timedelta(days=5), end=datetime.date.today())
start_date, end_date = date_range.min(), date_range.max()

table_dict = {'store': """
                SELECT PRODUCT_STORE_ID ORGCODE,
                STORE_NAME,
                STORE_BRAND,
                STORE_MGT_AREA_NAME,
                PROVINCE_GEO_NAME ,
                GEO_NAME,
                COUNTY_GEO_NAME,
                ADDRESS,
                ORGTYPE,
                STORE_STATUS_NAME  ,
                MGT_PARTY_NAME_LOCAL 
                FROM ORGVIEW
            """,
              'plu': 'SELECT PRODUCT_ID PLUCODE, INTERNAL_NAME, PRODUCT_CATEGORY_NAME, UOM FROM PLUVIEW',
              'qm': f"""
              SELECT ORGCODE, PLUCODE, RPTDATE, YJTOTAL QM 
              FROM TZYPLUPRO_PD
              WHERE RPTDATE >= TO_DATE('{start_date}','yyyy-mm-dd hh24:mi:ss') AND RPTDATE <= TO_DATE('{end_date}','yyyy-mm-dd hh24:mi:ss') 
              """,
              'qc': f"""
              SELECT ORGCODE, PLUCODE, RPTDATE + 1 RPTDATE, YJTOTAL QC 
              FROM TZYPLUPRO_PD 
              WHERE RPTDATE >= TO_DATE('{start_date}','yyyy-mm-dd hh24:mi:ss') - 1 AND RPTDATE <= TO_DATE('{end_date}','yyyy-mm-dd hh24:mi:ss') - 1
              """,
              'ps': f"""
              SELECT ORGCODE , PLUCODE , RPTDATE, YJTOTAL PS 
              FROM TZYPLUPRO_PS 
              WHERE RPTDATE >= TO_DATE('{start_date}','yyyy-mm-dd hh24:mi:ss') AND RPTDATE <= TO_DATE('{end_date}','yyyy-mm-dd hh24:mi:ss')
              """,
              'th': f"""
              SELECT ORGCODE, PLUCODE, DZ_DATE RPTDATE, YJTOTAL TH FROM TZYPLUPRO_TH
              WHERE DZ_DATE >= TO_DATE('{start_date}','yyyy-mm-dd hh24:mi:ss') AND DZ_DATE <= TO_DATE('{end_date}','yyyy-mm-dd hh24:mi:ss')
              """,
              'db': f"""
              SELECT ORGCODE, PLUCODE, RPTDATE, 
              CASE WHEN PLUTYPE = 'DBPS' THEN YJTOTAL ELSE 0 END DBPS,
              CASE WHEN PLUTYPE = 'DBTH' THEN YJTOTAL ELSE 0 END DBTH
              FROM TZYPLUPRO_DB
              WHERE RPTDATE >= TO_DATE('{start_date}','yyyy-mm-dd hh24:mi:ss') AND RPTDATE <= TO_DATE('{end_date}','yyyy-mm-dd hh24:mi:ss')
              """}


for table_name in table_dict:
    df = Oracle(user="zyngprd", password='zyngprd',
                dsn="10.10.201.97:1521/crmngpsd").read(table_dict[table_name])
    df.to_feather(f"{path}{table_name}.feather")


store = pd.read_feather(f"{path}store.feather")
plu = pd.read_feather(f"{path}plu.feather")
use_store = store[store.ORGTYPE.isin(
    ['经销商内加盟店', '经销商外加盟店', '加盟店', '直营店'])].ORGCODE
use_plu = plu[plu.PRODUCT_CATEGORY_NAME.isin(
    ['散装食品']) & plu.UOM.isin(['千克'])].PLUCODE
df_ps = pd.read_feather(f"{path}ps.feather")
df_qc = pd.read_feather(f"{path}qc.feather")
df_qm = pd.read_feather(f"{path}qm.feather")
df_th = pd.read_feather(f"{path}th.feather")
df_db = pd.read_feather(f"{path}db.feather")
df_kc = pd.concat([df_ps, df_th, df_db, df_qc, df_qm])
df_kc = df_kc[df_kc.ORGCODE.isin(use_store) & df_kc.PLUCODE.isin(use_plu)]
df_kc = df_kc.groupby(["ORGCODE", "PLUCODE", "RPTDATE"],
                      as_index=False, sort=True).sum()
df_kc = df_kc[(np.max(df_kc, axis=1) > 0) | (np.min(df_kc, axis=1) < 0)]
df_kc['推断销售量'] = df_kc['QC'] + df_kc['PS'] + df_kc['DBPS'] - df_kc['TH'] - df_kc['DBTH'] - df_kc['QM'] + df_kc['TH'] + \
    df_kc['DBTH']
df_kc['推断销售量'] = np.where(df_kc['推断销售量'] > 0, df_kc['推断销售量'], 0)
df_kc['累积推断销售量'] = df_kc.groupby(["ORGCODE", "PLUCODE"], as_index=False)[
    '推断销售量'].transform('cumsum')

df_rule = df_kc[df_kc["RPTDATE"] == date_range.min(
)][['ORGCODE', 'PLUCODE', 'RPTDATE', 'QC', 'PS']]
df_rule.QC = df_rule.QC.fillna(0)
df_rule = df_rule[df_rule.PS > 0]
df_rule.columns = ['ORGCODE', 'PLUCODE', '考核日期', '考核库存', '考核配送']
df_rule = df_rule.merge(df_kc, how='left', left_on=[
                        "ORGCODE", "PLUCODE"], right_on=["ORGCODE", "PLUCODE"])
df_rule = df_rule.sort_values(
    by=["ORGCODE", "PLUCODE", "RPTDATE"]).reset_index(drop=True)
s1 = df_rule['累积推断销售量'] <= df_rule['考核库存'] + df_rule['考核配送']
s2 = (df_rule['累积推断销售量'] - df_rule['推断销售量']
      ) < df_rule['考核库存'] + df_rule['考核配送']
df_rule = df_rule[s1 | s2].reset_index(drop=True)
df_rule["已过生产日期天数"] = (df_rule['RPTDATE'] - date_range.min()).dt.days + 1
choose_end = (df_rule['累积推断销售量'] >= df_rule['考核库存'] + df_rule['考核配送']) & \
             (df_rule['累积推断销售量'] - df_rule['推断销售量']
              < df_rule['考核库存'] + df_rule['考核配送'])
choose_start = (df_rule['累积推断销售量'] >= df_rule['考核库存']) & \
               (df_rule['累积推断销售量'] <= df_rule['考核库存'] + df_rule['考核配送'])
df_rule["是否销售期间"] = np.where(choose_end | choose_start, 1, 0)
df_rule["t1"] = np.where(df_rule["累积推断销售量"] - df_rule["考核库存"]
                         > 0, df_rule["累积推断销售量"] - df_rule["考核库存"], 0)
df_rule["t2"] = np.where(df_rule["累积推断销售量"] - df_rule["考核库存"] - df_rule['考核配送'] > 0,
                         df_rule["累积推断销售量"] - df_rule["考核库存"] - df_rule['考核配送'], 0)
df_rule["t3"] = np.where(df_rule["累积推断销售量"] - df_rule["考核库存"] - df_rule['推断销售量'] > 0,
                         df_rule["累积推断销售量"] - df_rule["考核库存"] - df_rule['推断销售量'], 0)
df_rule['对应销售量'] = df_rule["t1"] - df_rule["t2"] - df_rule["t3"]
df_rule['对应销售量占比'] = df_rule['对应销售量'] / df_rule['考核配送']
df_rule['剩余金额'] = df_rule["累积推断销售量"] - df_rule["考核库存"] - df_rule['考核配送']
df_rule['MAX已过生产日期天数'] = df_rule.groupby(["ORGCODE", "PLUCODE"], as_index=False)[
    '已过生产日期天数'].transform('max')

df_rule = df_rule.drop(['t1', 't2', 't3'], axis=1)
# df_rule = df_rule[choose_end | choose_start].reset_index(drop=True)
# df_rule["已过生产日期天数"] = np.where(df_rule['考核库存'] == 0, 1, df_rule["已过生产日期天数"])
df_rule = df_rule.merge(store, how="left", left_on=[
                        "ORGCODE"], right_on=["ORGCODE"])
df_rule = df_rule.merge(plu, how="left", left_on=[
                        "PLUCODE"], right_on=["PLUCODE"])
df_rule = df_rule.rename(columns={'ORGCODE': '门店编码',
                                  'PLUCODE': '产品编码',
                                  'INTERNAL_NAME': '产品名称',
                                  'PS': '配送',
                                  'TH': '退货',
                                  'DBPS': '调拨配送',
                                  'DBTH': '调拨退货',
                                  'QC': '期初盘点',
                                  'QM': '期末盘点',
                                  'STORE_NAME': '门店名称',
                                  'STORE_BRAND': '门店品牌',
                                  'STORE_MGT_AREA_NAME': '大区名称',
                                  'PROVINCE_GEO_NAME': '省份名称',
                                  'GEO_NAME': '地区名称',
                                  'ORGTYPE': '门店性质',
                                  'COUNTY_GEO_NAME': '区',
                                  'ADDRESS': '地址',
                                  'STORE_STATUS_NAME': '门店状态',
                                  'MGT_PARTY_NAME_LOCAL': '法人简称',
                                  'PRODUCT_CATEGORY_NAME': '商品小类',
                                  'UOM': '计量单位',
                                  'RPTDATE': '业务日期'
                                  })
c = df_rule[df_rule["是否销售期间"] == 1].groupby(["门店编码", '产品编码', "产品名称"]).已过生产日期天数.mean().groupby(
    ['产品编码', "产品名称"]).mean().reset_index()
df_rule = df_rule[df_rule["地区名称"] == "武汉市"]
df_rule[df_rule['MAX已过生产日期天数'] >= 4].to_csv(
    "test.csv", index=False, encoding="gbk")
