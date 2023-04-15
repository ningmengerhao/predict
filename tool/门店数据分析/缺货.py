from tool.dba import *

path = "data/"
table_dict = {'store': """
                SELECT PRODUCT_STORE_ID STORE_ID,
                STORE_NAME,
                STORE_BRAND,
                STORE_MGT_AREA_NAME,
                PROVINCE_GEO_NAME ,
                GEO_NAME,
                ORGTYPE,
                STORE_STATUS_NAME  ,
                MGT_PARTY_NAME_LOCAL 
                FROM ORGVIEW
            """,
              'plu': 'SELECT PRODUCT_ID PLUCODE, INTERNAL_NAME 产品名称, PRODUCT_CATEGORY_NAME 产品类型, UOM 计量单位, PARENT_PRODUCT_ID PRODUCT_ID FROM PLUVIEW',
              'pd': f"""
              SELECT RPTDATE POSTING_DATE, ORGCODE STORE_ID, PLUCODE , PLUCOUNT FROM ZYNGPRD.TZYPLUPRO_PD
              WHERE RPTDATE >= TRUNC(SYSDATE-31)""",
              'sale': f"""
              SELECT POSTING_DATE, STORE_ID, PRODUCT_ID, EXTRACT(HOUR FROM CREATED_STAMP) HOUR, SUM(REBATE_AMOUNT) AMOUNT  FROM SALE_ORDER_DTL
              WHERE POSTING_DATE >= TRUNC(SYSDATE-31) and POSTING_DATE < trunc(sysdate)
              GROUP BY POSTING_DATE, STORE_ID, PRODUCT_ID, EXTRACT(HOUR FROM CREATED_STAMP)
              """,
              }
for table_name in table_dict:
    df = oracle97.read(table_dict[table_name])
    df.to_feather(f"{path}{table_name}.feather")
store = pd.read_feather(f"{path}store.feather")
plu = pd.read_feather(f"{path}plu.feather")
plu["PRODUCT_ID"] = np.where(
    plu["PRODUCT_ID"].isnull(), plu["PLUCODE"], plu["PRODUCT_ID"])
df_pd = pd.read_feather(f"{path}pd.feather")
df_sale = pd.read_feather(f"{path}sale.feather")
df_sale["PRODUCT_ID"] = np.where(
    df_sale["PRODUCT_ID"] == '10564', '10003', df_sale["PRODUCT_ID"])
df_sale["PRODUCT_ID"] = df_sale["PRODUCT_ID"].replace(
    plu["PLUCODE"].tolist(), plu["PRODUCT_ID"].tolist())
df_sale = df_sale.groupby(
    ['STORE_ID', 'PRODUCT_ID', 'POSTING_DATE', 'HOUR'], as_index=False).sum()
df_summary = df_sale.groupby(
    ['STORE_ID', 'PRODUCT_ID', 'HOUR'], as_index=False).mean()
df_summary = df_summary.sort_values(by=['STORE_ID', 'PRODUCT_ID', 'HOUR'], ascending=[True, True, False]).reset_index(
    drop=True)
df_summary["CUM_AMOUNT"] = df_summary.groupby(
    ['STORE_ID', 'PRODUCT_ID'], sort=False).AMOUNT.transform("cumsum").values
df_sale = df_sale.groupby(
    ["POSTING_DATE", 'STORE_ID', 'PRODUCT_ID'], as_index=False).HOUR.max()
df_pd = df_pd.merge(plu, how="left", left_on=["PLUCODE"], right_on=["PLUCODE"])
df_pd = df_pd.groupby(
    ["POSTING_DATE", 'STORE_ID', 'PRODUCT_ID'], as_index=False).sum()
df_sale = df_sale.merge(df_pd, how="left", left_on=["STORE_ID", "PRODUCT_ID", "POSTING_DATE"],
                        right_on=["STORE_ID", "PRODUCT_ID", "POSTING_DATE"])
df_sale = df_sale.merge(store, how="left", left_on=[
                        "STORE_ID"], right_on=["STORE_ID"])
df_sale = df_sale.merge(plu.drop("PRODUCT_ID", axis=1), how="left", left_on=["PRODUCT_ID"], right_on=["PLUCODE"]).drop(
    "PLUCODE", axis=1)

df_sale = df_sale.merge(df_summary,
                        how="left",
                        left_on=["STORE_ID", "PRODUCT_ID", "HOUR"],
                        right_on=["STORE_ID", "PRODUCT_ID", "HOUR"])
df_sale = df_sale.drop("AMOUNT", axis=1)
df_sale = df_sale.rename(columns={'STORE_ID': '门店编码',
                                  "HOUR": '最后销售时刻',
                                  'CUM_AMOUNT': '缺货金额',
                                  "PLUCOUNT": "盘点重量",
                                  'POSTING_DATE': '业务日期',
                                  'PRODUCT_ID': '产品编码',
                                  'STORE_NAME': '门店名称',
                                  'STORE_BRAND': '门店品牌',
                                  'STORE_MGT_AREA_NAME': '大区名称',
                                  'PROVINCE_GEO_NAME': '省份名称',
                                  'GEO_NAME': '地区名称',
                                  'ORGTYPE': '门店性质',
                                  'STORE_STATUS_NAME': '门店状态',
                                  'MGT_PARTY_NAME_LOCAL': '法人简称',
                                  })
df_sale = df_sale[df_sale["地区名称"].isin(["武汉市", "上海市", '南京市'])]
df_sale["缺货"] = np.where((df_sale["盘点重量"].fillna(0) == 0)
                         & (df_sale["最后销售时刻"] < 18), 1, 0)
df_sale["缺货金额"] = np.where(df_sale["缺货"], df_sale["缺货金额"], np.nan)
df_sale.to_csv("缺货数据.csv", index=False, encoding="gbk")
