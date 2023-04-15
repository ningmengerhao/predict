# %%
import numpy as np
import logging
import matplotlib.pyplot as plt
import numpy as np
import json
import pandas as pd
import tool.dba as database
from datetime import datetime, timedelta
import os


# 连接数据库
Oracle = database.Oracle()
Hana = database.Hana()
Oracle97 = database.Oracle(
    user="zyngprd", password='zyngprd', dsn="10.10.201.97:1521/crmngpsd")


# %%
# 外卖地址&中台地址
address = Oracle97.read('''
SELECT
    PRODUCT_STORE_ID,
    ADDRESS_TEXT ADDRESS,
    PROVINCE_NAME,
    CITY_NAME,
    DISTRICT_NAME,
    LATITUDE,
    LONGITUDE,
    -- C.SOURCE AS source,
    '饿了么' AS PLAT_FROM
FROM
    TAKEAWAY_ELEME_STORE_INFO A
    INNER JOIN TAKEAWAY_STORE_STATUS B ON A.ID = B.TAKEAWAY_STORE_ID
    AND B.PLATFORM_TYPE_ID = 'eleme'
UNION ALL
SELECT
    PRODUCT_STORE_ID,
    ADDRESS,
    null AS PROVINCE_NAME,
    null AS CITY_NAME,
    null AS DISTRICT_NAME,
    LATITUDE / 1000000 LATITUDE,
    LONGITUDE / 1000000 LONGITUDE,
    '美团' as PLAT_FROM
FROM
    TAKEAWAY_MEITUAN_STORE_INFO D
    INNER JOIN TAKEAWAY_STORE_STATUS E ON D.APP_POI_CODE = E.TAKEAWAY_STORE_ID
    AND E.PLATFORM_TYPE_ID = 'meituan'
UNION ALL
SELECT
    PRODUCT_STORE_ID,
    ADDRESS,
    PROVINCE_GEO_NAME PROVINCE_NAME,
    GEO_NAME CITY_NAME,
    COUNTY_GEO_NAME DISTRICT_NAME,
    TO_NUMBER(LAT) LATITUDE,
    TO_NUMBER(LNG) LONGITUDE,
    '中台' as PLAT_FROM
FROM
    ORGVIEW
WHERE
    STORE_STATUS_ID = 'OPEN'
''')


address = address[address['PRODUCT_STORE_ID'].str.match(
    r'^800[\d]{5}$')]  # 筛选编码合格数据

address.drop_duplicates(subset=['PRODUCT_STORE_ID',
                                'PLAT_FROM', 'LATITUDE', 'LONGITUDE'], inplace=True)  # 针对饿了么重复数据去重

address = address.pivot_table(index='PRODUCT_STORE_ID',
                              columns='PLAT_FROM', values=['LATITUDE', 'LONGITUDE'])

# %%
storeInfo = Oracle97.read("""
SELECT
    PRODUCT_STORE_ID AS 门店编码,
    STORE_NAME AS 门店名称,
    STORE_BRAND AS 品牌,
    -- PRIMARY_STORE_GROUP_ID ,
    -- STORE_STATUS_NAME AS 营业状态,
    ORGTYPE AS 门店类型,
    STORE_OPENTIME AS 开店日期,
    STORE_CLOSETIME AS 关店日期,
    PROVINCE_GEO_NAME AS 省,
    GEO_NAME AS 市,
    COUNTY_GEO_NAME AS 区,
    ADDRESS AS 地址,
    TO_NUMBER(LNG) AS 中台经度,
    TO_NUMBER(LAT) AS 中台纬度
FROM
    ORGVIEW
WHERE
    PRIMARY_STORE_GROUP_ID = 'S' --剔除非门店实体
    AND STORE_STATUS_ID = 'OPEN' -- 剔除非营业中
    AND ORGTYPE IN ('直营店','加盟店','经销商内加盟店','经销商外加盟店') --剔除渠道客户
""")

# %%
# 门店线路数据
line = Hana.read("""SELECT
    LINE_NAME,
    LINE_ID,
    ORGCODE as STORE_ID,
    LINE_TYPE,
    TRANS_TIME
FROM
    (
        SELECT
            DESCRIPTION AS LINE_NAME,
            SCH_ID AS LINE_ID,
            CONSIGNEE_ID AS ORGCODE,
            UNASSGN_END AS TRANS_TIME,
            SCH_TYPE AS LINE_TYPE,
            ROW_NUMBER() OVER(
                PARTITION BY CONSIGNEE_ID,
                SCH_TYPE
                ORDER BY
                    UNASSGN_END DESC
            ) RN
        FROM
            (
                SELECT
                    B.DESCRIPTION,
                    C.LOG_LOCID,
                    A.PARENT_KEY,
                    -- D.SCH_TYPE,
                    CASE
                        WHEN D.SCH_TYPE IN ('ZS01', 'ZS02') THEN '干线'
                        ELSE '市配'
                    END AS SCH_TYPE,
                    D.SCH_ID
                FROM
                    "/SCMTMS/D_TORITE" A
                    INNER JOIN "/SCMTMS/D_SCHDSC" B ON B.PARENT_KEY = A.SCHED_KEY
                    INNER JOIN "/SCMTMS/D_TORSTP" C ON A.PARENT_KEY = C.PARENT_KEY
                    INNER JOIN "/SCMTMS/D_SCHROT" D ON D.DB_KEY = A.SCHED_KEY --WHERE SCH_TYPE IN ('ZS01', 'ZS02')
                GROUP BY
                    A.PARENT_KEY,
                    C.LOG_LOCID,
                    B.DESCRIPTION,
                    D.SCH_TYPE,
                    SCH_ID
            ) E1
            INNER JOIN (
                SELECT
                    PARENT_KEY,
                    CONSIGNEE_ID,
                    MIN(UNASSGN_END) UNASSGN_END
                FROM
                    "/SCMTMS/D_TORITE" A
                GROUP BY
                    PARENT_KEY,
                    CONSIGNEE_ID
            ) E2 ON E1.PARENT_KEY = E2.PARENT_KEY
    )
WHERE
    RN = 1
    AND LINE_NAME IS NOT NULL
    AND TRANS_TIME IS NOT NULL -- AND LINE_ID LIKE '%GX%'
    AND ORGCODE LIKE '%008%' -- AND LINE_NAME NOT LIKE '%机场%'
ORDER BY
    TRANS_TIME""")

line['TRANS_TIME'] = pd.to_datetime(line['TRANS_TIME'], format='%Y%m%d%H%M%S')

line['STORE_ID'] = line['STORE_ID'].str[2:]

line.rename(columns={'LINE_NAME': '线路名称',
                     'LINE_ID': '线路编码',
                     'STORE_ID': '门店编码',
                     'LINE_TYPE': '线路类型',
                     'TRANS_TIME': '时间'}, inplace=True)

# 包材配送框数
Oracle97.read("""
SELECT 
    DD.POSTING_DATE,
    DD.PRODUCT_STORE_ID,
    DD.STORE_NAME,
    DD.PRODUCT_STORE_ID_TO,
    DD.STORE_TO_NAME,
    DI.PRODUCT_ID,
    DI.PRODUCT_NAME,
    NVL(DI.QUANTITY,0) AS QUANTITY
FROM 
    ZDEV_PS_DOC DD
    INNER JOIN ZDEV_PS_ITEM DI ON DD.DOC_ID=DI.DOC_ID 
    INNER JOIN PRODUCT_STORE PS ON PS.PRODUCT_STORE_ID=DD.PRODUCT_STORE_ID_TO
    INNER JOIN PLUVIEW ON DI.PRODUCT_ID = PLUVIEW.PRODUCT_ID
WHERE 
    DD.MOVEMENT_TYPE_ID IN ('DZ','DR')
    AND DD.POSTING_DATE>=TO_DATE('2022-08-05','YYYY-MM-DD')
    AND REGEXP_LIKE (DD.STORE_TO_NAME,'^南京(*)|^武汉(*)')
    AND PLUVIEW.PRODUCT_CATEGORY_NAME = '包装材料'
   """)


# 成品配送框数
Oracle97.read("""SELECT
    DD.POSTING_DATE AS 业务日期,
    DD.PRODUCT_STORE_ID AS 配送中心编码,
    PS.STORE_NAME AS 配送中心,
    DD.PRODUCT_STORE_ID_TO AS 门店编码,
    ORG.STORE_NAME AS STORE_TO_NAME AS 门店名称,
    ORG.GEO_NAME AS GEO_TO_NAME,
    MAX(DB.BOX_ID) AS BOX_NUMS
FROM
    DELIVERY_DOC DD
    INNER JOIN (
        SELECT
            DB.DOC_ID,
            DB.BOX_ID
        FROM
            DELIVERY_BOX DB
    ) DB ON DD.DOC_ID = DB.DOC_ID
    LEFT JOIN ORGVIEW ORG ON DD.PRODUCT_STORE_ID_TO = ORG.PRODUCT_STORE_ID
    LEFT JOIN ORGVIEW PS ON DD.PRODUCT_STORE_ID = PS.PRODUCT_STORE_ID
WHERE
    DD.MOVEMENT_TYPE_ID IN ('DR', 'DZ', 'PS') --配送框数，ZGD BZ 属于退货，
    AND DD.POSTING_DATE >= TO_DATE('2021-01-01', 'YYYY-MM-DD')
    --AND ORG.GEO_NAME IN('武汉市','南京市')
GROUP BY
    DD.POSTING_DATE,
    DD.PRODUCT_STORE_ID ,
    PS.STORE_NAME ,
    DD.PRODUCT_STORE_ID_TO,
    ORG.STORE_NAME ,
    ORG.GEO_NAME
""")


# 干线预测数据

start_date = (datetime.now()+timedelta(days=-40)).strftime('%Y-%m-%d')

df = Oracle.read("""
SELECT
    POSTING_DATE as 门店业务日期,
    LINE_NAME 线路名称,
    TRUE 转换框数,
    PREDICT 预测框数,
    ADVICE 建议方案
FROM
    ZCB_PREDICT_GX_KS
ORDER BY
    LINE_NAME,
    POSTING_DATE""")

df[df['门店业务日期'] > f'{start_date}'].to_clipboard()


# df2 = pd.read_clipboard()
# df3 = df2[['成品应装数量', '线路描述', '计划发车日期']]
# df3['线路'] = df3['线路描述'].str[:-2]

# %%
start_date = (datetime.now()+timedelta(days=-40)).strftime('%Y-%m-%d')


# 日志输出样式
log_format = '%(levelname)s %(asctime)s %(filename)s %(lineno)d %(message)s'
logging.basicConfig(format=log_format, level=logging.DEBUG)

logging.debug('debug log test')
logging.info('info log test')
logging.warning('warning log test')
logging.error('error log test')
logging.critical('critical log test')


# 日志输出样式
log_format = '%(levelname)s %(asctime)s %(filename)s %(lineno)d %(message)s'
logging.basicConfig(
    filename='test.log',
    format=log_format,
    level=logging.DEBUG
)

logging.debug('debug log test')
logging.info('info log test')
logging.warning('warning log test')
logging.error('error log test')
logging.critical('critical log test')


查找了ChatGPT Excel。这是第一个，如果有的话，可以做出一些漂亮的数据处理了。


如何把多个文件合并，去掉水印，然后提炼出重要信息，并且做成表格的形式。


经纬度，然后呢，如果这时候我借助BI，就可以非常高效地给你提供数据支持。

如果可以很快地进行一个数据管理。

因为我本人比较擅长数据方面的工作，我就觉得呢，我们的门店管理啊，也不用那么复杂了？
直接一个甘特图就看出来了。

这是钉钉云文档支持的功能。


接下来放几个大招，你们就是比如有些东西需要填写成文档。
workflow 非常科学高效。
