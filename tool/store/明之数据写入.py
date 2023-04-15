# import pandas as pd
# import pymysql
# # from tool.dba import *
# import numpy as np
#
# class Mysql:
#     def __init__(self, user="zyuser", password='Zyuser123!@#', host="220.179.250.44", port=9889, database="sale_data"):
#         self.conn = pymysql.connect(user=user, password=password, host=host, port=port, database=database)
#         self.cursor = self.conn.cursor()
#         self.base = {'datetime64[ns]': 'DATE', 'object': 'VARCHAR2(50)', 'float': 'NUMBER(5, 2)'}
#
#     def read(self, sql):
#         self.cursor.execute(sql)
#         var_name = [var[0] for var in self.cursor.description]
#         df = self.cursor.fetchall()
#         df = pd.DataFrame(list(df), columns=var_name)
#         return df
#
#     def write_many(self, sql, data):
#         # logger.info(sql, data)
#         self.cursor.executemany(sql, data)
#         self.conn.commit()
#
#     def write_single(self, sql, data):
#         self.cursor.execute(sql, data)
#         self.conn.commit()
#
#     def execute(self, sql):
#         self.cursor.execute(sql)
#
#     def __del__(self):
#         self.cursor.close()
#         self.conn.close()
#
#
# #
# # path = "data/"
# #
# # table_dict = {'store': """
# #                 SELECT PRODUCT_STORE_ID STORE_ID,
# #                 STORE_NAME,
# #                 STORE_BRAND,
# #                 STORE_MGT_AREA_NAME,
# #                 PROVINCE_GEO_NAME ,
# #                 GEO_NAME,
# #                 ORGTYPE,
# #                 STORE_STATUS_NAME  ,
# #                 MGT_PARTY_NAME_LOCAL
# #                 FROM ORGVIEW
# #                 WHERE PRODUCT_STORE_ID = '80018508'
# #             """,
# #               'plu': 'SELECT PRODUCT_ID PLUCODE, INTERNAL_NAME 产品名称, PRODUCT_CATEGORY_NAME 产品类型, UOM 计量单位, PARENT_PRODUCT_ID PRODUCT_ID FROM PLUVIEW',
# #               'pd': f"""
# #               SELECT RPTDATE POSTING_DATE, ORGCODE STORE_ID, PLUCODE , PLUCOUNT FROM ZYNGPRD.TZYPLUPRO_PD
# #               WHERE RPTDATE >= TRUNC(SYSDATE-31) AND ORGCODE = '80018508' """,
# #               'sale': f"""
# #               SELECT POSTING_DATE, STORE_ID, PRODUCT_ID, EXTRACT(HOUR FROM CREATED_STAMP) HOUR, SUM(REBATE_AMOUNT) AMOUNT  FROM SALE_ORDER_DTL
# #               WHERE POSTING_DATE >= TRUNC(SYSDATE-31) and POSTING_DATE < trunc(sysdate) AND ORGCODE = '80018508'
# #               GROUP BY POSTING_DATE, STORE_ID, PRODUCT_ID, EXTRACT(HOUR FROM CREATED_STAMP)
# #               """,
# #               }
# # for table_name in table_dict:
# #     df = oracle97.read(table_dict[table_name])
# #     df.to_feather(f"{path}{table_name}.feather")
# # store = pd.read_feather(f"{path}store.feather")
# # plu = pd.read_feather(f"{path}plu.feather")
# # plu["PRODUCT_ID"] = np.where(plu["PRODUCT_ID"].isnull(), plu["PLUCODE"], plu["PRODUCT_ID"])
# # df_pd = pd.read_feather(f"{path}pd.feather")
# # df_sale = pd.read_feather(f"{path}sale.feather")
# # df_sale["PRODUCT_ID"] = np.where(df_sale["PRODUCT_ID"] == '10564', '10003', df_sale["PRODUCT_ID"])
# # df_sale["PRODUCT_ID"] = df_sale["PRODUCT_ID"].replace(plu["PLUCODE"].tolist(), plu["PRODUCT_ID"].tolist())
# # df_sale = df_sale.groupby(['STORE_ID', 'PRODUCT_ID', 'POSTING_DATE', 'HOUR'], as_index=False).sum()
# # df_summary = df_sale.groupby(['STORE_ID', 'PRODUCT_ID', 'HOUR'], as_index=False).mean()
# # df_summary = df_summary.sort_values(by=['STORE_ID', 'PRODUCT_ID', 'HOUR'], ascending=[True, True, False]).reset_index(
# #     drop=True)
# # df_summary["CUM_AMOUNT"] = df_summary.groupby(['STORE_ID', 'PRODUCT_ID'], sort=False).AMOUNT.transform("cumsum").values
# # df_sale = df_sale.groupby(["POSTING_DATE", 'STORE_ID', 'PRODUCT_ID'], as_index=False).HOUR.max()
# # df_pd = df_pd.merge(plu, how="left", left_on=["PLUCODE"], right_on=["PLUCODE"])
# # df_pd = df_pd.groupby(["POSTING_DATE", 'STORE_ID', 'PRODUCT_ID'], as_index=False).sum()
# # df_sale = df_sale.merge(df_pd, how="left", left_on=["STORE_ID", "PRODUCT_ID", "POSTING_DATE"],
# #                         right_on=["STORE_ID", "PRODUCT_ID", "POSTING_DATE"])
# # df_sale = df_sale.merge(store, how="left", left_on=["STORE_ID"], right_on=["STORE_ID"])
# # df_sale = df_sale.merge(plu.drop("PRODUCT_ID", axis=1), how="left", left_on=["PRODUCT_ID"], right_on=["PLUCODE"]).drop(
# #     "PLUCODE", axis=1)
# #
# # df_sale = df_sale.merge(df_summary,
# #                         how="left",
# #                         left_on=["STORE_ID", "PRODUCT_ID", "HOUR"],
# #                         right_on=["STORE_ID", "PRODUCT_ID", "HOUR"])
# # df_sale = df_sale.drop("AMOUNT", axis=1)
#
# df = Mysql().read("""
# select
# receive_date as POSTING_DATE,
# store_code as STORE_ID,
# sku_code as PLUCODE,
# predict_qty as XSCOUNT_PREDICT,
# suggest_qty	AS YH_COUNT,
# ordering_date AS EXEDATE,
# min_qty AS MIN_YH_COUNT,
# max_qty AS MAX_YH_COUNT,
#  unit_weight as YH_UNIT,
#  total_weight as YHCOUNT_PREDICT
# from ordering_bill
# where store_code = '80018508'
# """)
# plu = pd.read_feather("plu.feather")
# plu["PRODUCT_ID"] = np.where(plu["PRODUCT_ID"].isnull(), plu["PLUCODE"], plu["PRODUCT_ID"])
# df = df.merge(plu, how="left", left_on=["PLUCODE"], right_on=["PLUCODE"])
# df.POSTING_DATE = pd.to_datetime(df.POSTING_DATE)
# df.STORE_ID = df.STORE_ID.astype(np.int64)
# df.PRODUCT_ID = df.PRODUCT_ID.astype(np.int64)
# df.YHCOUNT_PREDICT = df.YHCOUNT_PREDICT.astype(float)
# df = df.merge(plu, how="left", left_on=["PLUCODE"], right_on=["PLUCODE"])
# df = df.groupby(["POSTING_DATE", 'STORE_ID', 'PRODUCT_ID'], as_index=False).YHCOUNT_PREDICT.sum()
# df_base = pd.read_csv("test.csv", encoding="gbk", parse_dates=["业务日期"])
# df.dtypes
# df_base = df_base.merge(df, how="left", left_on=['业务日期', '门店编码', '产品编码'],
#                         right_on=["POSTING_DATE", "STORE_ID", "PRODUCT_ID"]).drop(["STORE_ID", "PRODUCT_ID", "POSTING_DATE"], axis=1)
# df_base.to_excel("test.xlsx", index=False)
