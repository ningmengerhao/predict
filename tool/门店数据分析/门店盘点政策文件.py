# -*- coding: utf-8 -*-
import schedule
import os
import logging
import time
import pandas as pd
import numpy as np
import datetime
# from tool.dba import *
import dba
import requests
import json
from itertools import product

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
date_range = pd.date_range(start='2022-01-01', end='2022-07-31')
start_date, end_date = date_range.min(), date_range.max()

oracle97 = dba.Oracle(user="zyngprd", password='zyngprd',
                      dsn="10.10.201.97:1521/crmngpsd")  # 连接数据库

table_dict = {'store': """
                SELECT *
                FROM ORGVIEW""",
              'plu': 'SELECT PRODUCT_ID PLUCODE, INTERNAL_NAME, PRODUCT_CATEGORY_NAME, UOM FROM PLUVIEW',
              'qm': f"""
              SELECT ORGCODE, PLUCODE, RPTDATE, PLUCOUNT QM, YJTOTAL QMTOTAL
              FROM TZYPLUPRO_PD
              WHERE RPTDATE >= TO_DATE('{start_date}', 'yyyy-mm-dd hh24:mi:ss') AND RPTDATE <= TO_DATE('{end_date}', 'yyyy-mm-dd hh24:mi:ss')
              """,
              'qc': f"""
              SELECT ORGCODE, PLUCODE, RPTDATE + 1 RPTDATE, PLUCOUNT QC, YJTOTAL QCTOTAL
              FROM TZYPLUPRO_PD
              WHERE RPTDATE + 1 >= TO_DATE('{start_date}', 'yyyy-mm-dd hh24:mi:ss') AND RPTDATE + 1 <= TO_DATE('{end_date}', 'yyyy-mm-dd hh24:mi:ss')
              """,
              'ps': f"""
              SELECT ORGCODE, PLUCODE, RPTDATE, PLUCOUNT PS, YJTOTAL PSTOTAL
              FROM TZYPLUPRO_PS
              WHERE RPTDATE >= TO_DATE('{start_date}', 'yyyy-mm-dd hh24:mi:ss') AND RPTDATE <= TO_DATE('{end_date}', 'yyyy-mm-dd hh24:mi:ss')
              """,
              'sale': f"""
                SELECT RPTDATE, ORGCODE, PLUCODE, XSCOUNT XS, SSTOTAL + DSCTOTAL SSTOTAL
                FROM TRPTSALPLURPT
                WHERE RPTDATE >= TO_DATE('{start_date}', 'yyyy-mm-dd hh24:mi:ss') AND RPTDATE <= TO_DATE('{end_date}', 'yyyy-mm-dd hh24:mi:ss')
              """,
              'th': f"""
              SELECT ORGCODE, PLUCODE, DZ_DATE RPTDATE, PLUCOUNT TH, YJTOTAL THTOTAL FROM TZYPLUPRO_TH
              WHERE DZ_DATE >= TO_DATE('{start_date}', 'yyyy-mm-dd hh24:mi:ss') AND DZ_DATE <= TO_DATE('{end_date}', 'yyyy-mm-dd hh24:mi:ss')
              """,
              'db': f"""
              SELECT ORGCODE, PLUCODE, RPTDATE,
              CASE WHEN PLUTYPE = 'DBPS' THEN YJTOTAL ELSE - YJTOTAL END DBTOTAL,
              CASE WHEN PLUTYPE = 'DBPS' THEN PLUCOUNT ELSE - PLUCOUNT END DB
              FROM TZYPLUPRO_DB
              WHERE RPTDATE >= TO_DATE('{start_date}', 'yyyy-mm-dd hh24:mi:ss') AND RPTDATE <= TO_DATE('{end_date}', 'yyyy-mm-dd hh24:mi:ss')
              """}
for table_name in table_dict:
    df = oracle97.read(table_dict[table_name])
    df.to_feather(f"{path}{table_name}.feather")
store = pd.read_feather(f"{path}store.feather")
plu = pd.read_feather(f"{path}plu.feather")
use_store = store[
    store.STORE_BRAND.isin(['紫燕百味鸡', '钟记油烫鸭', '赛八珍', '巧川婆', '鸭鲜滋']) &
    store.ORGTYPE.isin(['经销商内加盟店', '经销商外加盟店', '加盟店'])].ORGCODE
use_plu = plu[plu.PRODUCT_CATEGORY_NAME.isin(
    ['散装食品']) & plu.UOM.isin(['千克'])].PLUCODE
df_ps = pd.read_feather(f"{path}ps.feather")
df_qc = pd.read_feather(f"{path}qc.feather")
df_qm = pd.read_feather(f"{path}qm.feather")
df_sale = pd.read_feather(f"{path}sale.feather")
df_th = pd.read_feather(f"{path}th.feather")
df_db = pd.read_feather(f"{path}db.feather")
df_kc = pd.concat([df_ps, df_qc, df_sale, df_th, df_db])
df_kc = df_kc[df_kc.ORGCODE.isin(use_store) & df_kc.PLUCODE.isin(use_plu)]
df_kc = df_kc.groupby(["ORGCODE", "PLUCODE", "RPTDATE"], as_index=False).sum()
df_kc['推断期末金额'] = df_kc['PSTOTAL'] + df_kc['QCTOTAL'] - \
    df_kc['SSTOTAL'] - df_kc['THTOTAL'] + df_kc['DBTOTAL']
df_kc.drop(['PS', 'PSTOTAL', 'QC', 'QCTOTAL', 'XS', 'SSTOTAL',
           'TH', 'THTOTAL', 'DBTOTAL', 'DB'], axis=1, inplace=True)
df_kc = df_kc[(df_kc["RPTDATE"] <= np.max(date_range)) &
              (df_kc["RPTDATE"] >= np.min(date_range))]
df_store_product = pd.concat([df_ps, df_qc, df_th, df_db])[
    ["ORGCODE", "PLUCODE"]].drop_duplicates()
df_store_day = pd.concat([df_ps, df_sale, df_qc, df_th, df_db])[
    ["ORGCODE", "RPTDATE"]].drop_duplicates()
df_base = df_store_product.merge(df_store_day, how="left", left_on=[
                                 "ORGCODE"], right_on=["ORGCODE"])
df_base = df_base.merge(df_ps, how="left", left_on=["ORGCODE", "PLUCODE", "RPTDATE"],
                        right_on=["ORGCODE", "PLUCODE", "RPTDATE"])
df_base = df_base.merge(df_qm, how="left", left_on=["ORGCODE", "PLUCODE", "RPTDATE"],
                        right_on=["ORGCODE", "PLUCODE", "RPTDATE"])
df_base = df_base.merge(df_kc, how="left", left_on=["ORGCODE", "PLUCODE", "RPTDATE"],
                        right_on=["ORGCODE", "PLUCODE", "RPTDATE"])
df_base = df_base[df_base.ORGCODE.isin(
    use_store) & df_base.PLUCODE.isin(use_plu)]
df_base = df_base.groupby(
    ["ORGCODE", "PLUCODE", "RPTDATE"], as_index=False).sum()
df_base['应盘次数'] = 1
df_base['实盘次数'] = np.where(df_base['推断期末金额'].fillna(0) <= 10, 1, 0)
df_base['实盘次数'] = np.where(df_base['QM'].fillna(0) > 0, 1, df_base['实盘次数'])
df_base['未盘次数'] = 1 - df_base['实盘次数']
df_base = df_base.sort_values(
    by=["ORGCODE", "PLUCODE", "RPTDATE"]).reset_index(drop=True)
df_base['年月'] = df_base.RPTDATE.dt.year * 100 + df_base.RPTDATE.dt.month
df_ps['年月'] = df_ps.RPTDATE.dt.year * 100 + df_ps.RPTDATE.dt.onth
df_ps = df_ps.merge(store, how="left", left_on=[
                    "ORGCODE"], right_on=["ORGCODE"])
df_ps_rate = df_ps.groupby(["ORGCODE", "年月"]).RPTDATE.agg(
    {'nunique', 'min', 'max'}).reset_index()
df_ps_rate.columns = ["".join(x) for x in df_ps_rate.columns]
df_ps_rate = df_ps_rate.rename(
    columns={'max': '最晚配送日期', 'min': '最早配送日期', 'nunique': '实际配送天数', })
df_ps_rate['应配送天数'] = (df_ps_rate['最晚配送日期'] - df_ps_rate['最早配送日期']).dt.days + 1
df_ps_rate = df_ps_rate.groupby(["ORGCODE", "年月"], as_index=False)[
    ['实际配送天数', '应配送天数']].sum()
df_ps_rate['配送频率'] = df_ps_rate['实际配送天数'] / np.maximum(df_ps_rate['应配送天数'], 2)
df_summary = df_base.groupby(['ORGCODE', '年月'], as_index=False)[
    ["应盘次数", "实盘次数", "未盘次数"]].sum()
df_summary['盘点比例'] = df_summary['实盘次数'] / df_summary['应盘次数']
df_summary = df_summary.merge(store, how="left", left_on=[
                              "ORGCODE"], right_on=["ORGCODE"])
df_summary = df_summary.merge(df_ps_rate, how="left", left_on=[
                              "ORGCODE", "年月"], right_on=["ORGCODE", "年月"])
df_summary['应处罚门店'] = np.where((df_summary['盘点比例'] < 0.8) & (df_summary['配送频率'] > 0.5) & (
    df_summary['实际配送天数'] > 14) & (df_summary['盘点比例'] / df_summary['配送频率'] < 0.8), 1, 0)
df_summary = df_summary.rename(columns={'ORGCODE': '门店编码',
                                        'STORE_NAME': '门店名称',
                                        'STORE_BRAND': '门店品牌',
                                        'STORE_MGT_AREA_NAME': '大区名称',
                                        'PROVINCE_GEO_NAME': '省份名称',
                                        'GEO_NAME': '地区名称',
                                        'ORGTYPE': '门店性质',
                                        'STORE_STATUS_NAME': '门店状态',
                                        'MGT_PARTY_NAME_LOCAL': '法人简称',
                                        })
df_summary.to_csv("/home/zyuser/workspace/test.csv",
                  index=False, encoding="gbk")
