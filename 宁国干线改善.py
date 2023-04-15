# %%
from ast import If
import datetime
from datetime import datetime
import string
import pandas as pd
import tool.dba as database
import os


# 连接数据库
Oracle = database.Oracle()
Hana = database.Hana()
Oracle97 = database.Oracle(
    user="zyngprd", password='zyngprd', dsn="10.10.201.97:1521/crmngpsd")
# %%
# 车型数据
car = Oracle.read("SELECT * FROM CARVIEW")

# 干线框数、车型预测结果。
TrunkPredict = Oracle.read("""SELECT
                            POSTING_DATE as 业务日期,
                            LINE_NAME 线路名称,
                            TRUE 转换框数,
                            PREDICT 预测框数,
                            ADVICE 建议方案
                        FROM
                            ZCB_PREDICT_GX_KS
                        ORDER BY
                            LINE_NAME,
                            POSTING_DATE""")


TrunkPredict.set_index('业务日期', inplace=True)
TrunkPredict.describe()
TrunkPredict.loc['2022-07-24']


# 门店每日筐数——配送秤数据
BasketNum = Oracle97.read("""SELECT

                                PRODUCT_STORE_ID_TO AS STORE_ID,
                                POSTING_DATE ,
                                COUNT(*) NUMS
                            FROM
                                (
                                    SELECT
                                        DOC_ID,
                                        POSTING_DATE,
                                        PRODUCT_STORE_ID_TO
                                    FROM
                                        DELIVERY_DOC
                                    UNION
                                    ALL
                                    SELECT
                                        DOC_ID,
                                        POSTING_DATE,
                                        PRODUCT_STORE_ID_TO
                                    FROM
                                        DELIVERY_DOC_HIS
                                    WHERE

                                ) A
                                INNER JOIN (
                                    SELECT
                                        DOC_ID,
                                        BOX_ID
                                    FROM
                                        DELIVERY_BOX
                                    UNION
                                    ALL
                                    SELECT
                                        DOC_ID,
                                        BOX_ID
                                    FROM
                                        DELIVERY_BOX_HIS
                                ) B ON A.DOC_ID = B.DOC_ID
                            GROUP BY
                                PRODUCT_STORE_ID_TO,
                                POSTING_DATE""")


# 门店线路匹配关系，来自TMS
LineInfo = Hana.read("""
        -- noinspection SqlResolve
        SELECT LINE_NAME, LINE_ID, ORGCODE as STORE_ID, LINE_TYPE, TRANS_TIME
        FROM (
                 SELECT DESCRIPTION  AS LINE_NAME,
                        SCH_ID       AS LINE_ID,
                        CONSIGNEE_ID AS ORGCODE,
                        UNASSGN_END  AS TRANS_TIME,
                        SCH_TYPE     AS LINE_TYPE,
                        ROW_NUMBER()    OVER(PARTITION BY CONSIGNEE_ID, SCH_TYPE ORDER BY UNASSGN_END DESC) RN
                 FROM (
                          SELECT B.DESCRIPTION,
                                 C.LOG_LOCID,
                                 A.PARENT_KEY,
                                 -- D.SCH_TYPE,
                                 CASE WHEN D.SCH_TYPE IN ('ZS01', 'ZS02') THEN '干线' ELSE '市配' END AS SCH_TYPE,
                                 D.SCH_ID
                          FROM "/SCMTMS/D_TORITE" A
                                   INNER JOIN "/SCMTMS/D_SCHDSC" B ON B.PARENT_KEY = A.SCHED_KEY
                                   INNER JOIN "/SCMTMS/D_TORSTP" C ON A.PARENT_KEY = C.PARENT_KEY
                                   INNER JOIN "/SCMTMS/D_SCHROT" D ON D.DB_KEY = A.SCHED_KEY
                          --WHERE SCH_TYPE IN ('ZS01', 'ZS02') 
                          GROUP BY A.PARENT_KEY, C.LOG_LOCID, B.DESCRIPTION, D.SCH_TYPE, SCH_ID
                      ) E1
                          INNER JOIN (SELECT PARENT_KEY, CONSIGNEE_ID, MIN(UNASSGN_END) UNASSGN_END
                                      FROM "/SCMTMS/D_TORITE" A
                                      GROUP BY PARENT_KEY, CONSIGNEE_ID
                 ) E2 ON E1.PARENT_KEY = E2.PARENT_KEY
             )
        WHERE RN = 1
          AND LINE_NAME IS NOT NULL
          AND TRANS_TIME IS NOT NULL
          -- AND LINE_ID LIKE '%GX%'
          AND ORGCODE LIKE '%008%'
          -- AND LINE_NAME NOT LIKE '%机场%'
        ORDER BY TRANS_TIME DESC
        """)

LineInfo["STORE_ID"] = LineInfo["STORE_ID"].apply(lambda x: x[2:])


LineInfo["LINE_ID"] = LineInfo["LINE_ID"].apply(
    lambda x: x.split("GX")[0][:-1] if "GX" in x else x)

LineInfo


# # 读取 sql 文件文本内容
# sql = open(sql_path + sql_file, 'r', encoding='utf8')
# sqltxt = sql.readlines()

# # 读取之后关闭文件
# sql.close()

# # list 转 str
# sql = "".join(sqltxt)


# import datetime module from datetime
# consider the time stamp in string format
# DD/MM/YY H:M:S.micros
time_data = "25/05/99 02:35:5.523"
# format the string in the given format :
# day/month/year hours/minutes/seconds-micro
# seconds
format_data = "%d/%m/%y %H:%M:%S.%f"

# Using strptime with datetime we will format
# string into datetime
date = datetime.strptime(time_data, format_data)

# display milli second
print(date.microsecond)

# display hour
print(date.hour)

# display minute
print(date.minute)

# display second
print(date.second)

# display date
print(date)


# import the datetime module


# datetime in string format for may 25 1999
input = '2021/05/25'

# format
format = '%Y/%m/%d'

# convert from string format to datetime format
datetime = datetime.datetime.strptime(input, format)

# get the date from the datetime using date()
# function
print(datetime.date())

# %%
df = Oracle97.read("""SELECT
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
    """)
# %%
df2 = Oracle97.read("""select * from ORGVIEW""")
# %%
Hana.read("""-- noinspection SqlResolve
                            SELECT 
                                B.DESCRIPTION,
                                C.LOG_LOCID,
                                A.PARENT_KEY,
                                D.SCH_TYPE,
                                CASE WHEN D.SCH_TYPE IN ('ZS01', 'ZS02') THEN '干线' ELSE '市配' END AS SCH_TYPE,
                                D.SCH_ID
                            FROM "/SCMTMS/D_TORITE" A
                                INNER JOIN "/SCMTMS/D_SCHDSC" B ON B.PARENT_KEY = A.SCHED_KEY
                                INNER JOIN "/SCMTMS/D_TORSTP" C ON A.PARENT_KEY = C.PARENT_KEY
                                INNER JOIN "/SCMTMS/D_SCHROT" D ON D.DB_KEY = A.SCHED_KEY
                          GROUP BY A.PARENT_KEY, C.LOG_LOCID, B.DESCRIPTION, D.SCH_TYPE, SCH_ID""")
