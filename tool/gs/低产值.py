# -*- coding: utf-8 -*-
from tqdm import tqdm
import schedule
import os
import logging
import time
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import streamlit as st
from tool.dba import *
import requests
import json

os.environ['NUMEXPR_MAX_THREADS'] = '16'
# 定义logger日志输出
logger = logging.getLogger(__name__)
# 设置输出格式
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
today = pd.to_datetime(datetime.datetime.today().strftime('%Y-%m-%d'))
df_org = Oracle().read("SELECT PRODUCT_STORE_ID 门店编码, GEO_NAME 地区名称 FROM ORGVIEW")
df_sale = Oracle97.read(
    """
    SELECT RPTDATE 业务日期, A.ORGCODE 门店编码, TRADETOTAL 线下金额, TRADENUM 交易次数, WMTOTAL 外卖金额
FROM TRPTSALORGRPT A
INNER JOIN (SELECT  ORGCODE, MIN(RPTDATE) OPEN_DATE FROM TRPTSALORGRPT GROUP BY ORGCODE) B ON A.ORGCODE = B.ORGCODE
WHERE  A.RPTDATE > B.OPEN_DATE + 7 and A.RPTDATE >= TRUNC(SYSDATE - 180)
""")
df = pd.read_excel("tool/gs/低产值扶持效果-Python.xlsx",
                   sheet_name="基础信息表", dtype={"门店编码": np.str}).fillna(pd.NaT)
df['临时列'] = pd.to_datetime(datetime.date.today())
df['线下推广中结束日期'] = np.where(df['线下推广中结束日期'].isnull() & df['线下推广中开始日期'].notnull(),
                           df['线下推广中开始日期'] + timedelta(days=28),
                           df['线下推广中结束日期'])
df['线下推广中结束日期'] = np.where(
    df['线下推广中结束日期'] > df['临时列'], df['临时列'], df['线下推广中结束日期'])
df['外卖推广中结束日期'] = np.where(df['外卖推广中结束日期'].isnull() & df['外卖推广中开始日期'].notnull(),
                           df['外卖推广中开始日期'] + timedelta(days=28),
                           df['外卖推广中结束日期'])
df['外卖推广中结束日期'] = np.where(
    df['外卖推广中结束日期'] > df['临时列'], df['临时列'], df['外卖推广中结束日期'])
df = df.drop("临时列", axis=1)
df['汇总推广中开始日期'] = np.min(df[['外卖推广中开始日期', '线下推广中开始日期']], axis=1)
df['汇总推广中结束日期'] = np.max(df[['外卖推广中结束日期', '线下推广中结束日期']], axis=1)
df = pd.melt(df, id_vars="门店编码")
df["来源"] = df['variable'].apply(lambda x: x[:2])
df["类型"] = df['variable'].apply(lambda x: x[2:])
df = pd.pivot_table(df, index=["门店编码", "来源"], values="value", columns=[
                    "类型"], aggfunc=lambda x: x).reset_index()
df = df.dropna(subset=["推广中开始日期"])
df['持续天数'] = (df['推广中结束日期'] - df['推广中开始日期']).dt.days
df['推广前开始日期'] = df['推广中开始日期'] - \
    df['持续天数'].apply(lambda x: timedelta(days=np.ceil(x / 7) * 7))
df['推广前结束日期'] = df['推广前开始日期'] + df['持续天数'].apply(lambda x: timedelta(days=x))
df['推广后结束日期'] = df['推广中结束日期'] + \
    df['持续天数'].apply(lambda x: timedelta(days=np.ceil(x / 7) * 7))
df['推广后开始日期'] = df['推广后结束日期'] - df['持续天数'].apply(lambda x: timedelta(days=x))
df = df.merge(df_org, how='left', left_on=['门店编码'], right_on=['门店编码'])
df_sale = df_sale.merge(df_org, how='left', left_on=[
                        '门店编码'], right_on=['门店编码'])
df_sale['外卖金额'] = np.where(df_sale['外卖金额'] > 0, df_sale['外卖金额'], np.nan)
result = []
geo1 = df_sale.groupby("地区名称", as_index=False).门店编码.nunique()
geo2 = df.groupby("地区名称", as_index=False).门店编码.nunique()
geo = geo1.merge(geo2, how="left", left_on=["地区名称"], right_on=["地区名称"])
geo.columns = ["地区名称", "总门店数", "低产值门店数"]

for g_name, g_data in df.groupby('来源'):
    for row in tqdm(g_data.to_dict('records')):
        df_sale_org = df_sale[df_sale['门店编码'] ==
                              row['门店编码']].reset_index(drop=True).copy()
        df_sale_org.drop('门店编码', axis=1, inplace=True)
        result_single = pd.Series([row['门店编码'], g_name], index=["门店编码", '来源'])
        for var in ['推广前', '推广中', '推广后']:
            re_store = df_sale_org[(df_sale_org["业务日期"] >= row['%s开始日期' % var]) &
                                   (df_sale_org["业务日期"] < row['%s结束日期' % var])][['线下金额', '交易次数', '外卖金额']].mean()
            re_store.index = ["%s%s" % (var, x)
                              for x in ['日店均', '日客流', '外卖日店均']]
            if row['地区名称'] in geo[geo["低产值门店数"] / geo["总门店数"] >= 0.5].地区名称:
                print(row['地区名称'])
                re_geo = df_sale[(df_sale['门店编码'].isin(df['门店编码']) == False) &
                                 (df_sale["业务日期"] >= row['%s开始日期' % var]) &
                                 (df_sale["业务日期"] < row['%s结束日期' % var])][['线下金额', '交易次数', '外卖金额']].mean()
            else:
                re_geo = df_sale[(df_sale['门店编码'].isin(df['门店编码']) == False) &
                                 (df_sale['地区名称'] == row['地区名称']) &
                                 (df_sale["业务日期"] >= row['%s开始日期' % var]) &
                                 (df_sale["业务日期"] < row['%s结束日期' % var])][['线下金额', '交易次数', '外卖金额']].mean()
            re_geo.index = ["%s%s" % (var, x)
                            for x in ['地区日店均', '地区日客流', '地区外卖日店均']]
            result_single = pd.concat([result_single, re_geo, re_store])
        result.append(result_single)
result = pd.DataFrame(result)
for type in ['', '地区']:
    for period in ['推广中', '推广后']:
        for metric in ['日店均', '日客流', '外卖日店均']:
            result['%s环比推广前%s%s' % (period, type, metric)] = np.where(result['%s%s%s' % (period, type, metric)] > 0,
                                                                      result['%s%s%s' % (period, type, metric)] /
                                                                      result[
                                                                          '推广前%s%s' % (type, metric)] - 1,
                                                                      np.nan)
for period in ['推广中', '推广后']:
    for metric in ['日店均', '日客流', '外卖日店均']:
        result['增量%s环比推广前%s' % (period, metric)] = result['%s环比推广前%s' % (period, metric)] - result[
            '%s环比推广前地区%s' % (period, metric)].fillna(0)
result = result[[x for x in result.columns if '地区' not in x]]
result = result.merge(df, how='left', left_on=[
                      '门店编码', '来源'], right_on=['门店编码', '来源'])
result.to_excel("tool/gs/低产值数据分析底表.xlsx", index=False)
