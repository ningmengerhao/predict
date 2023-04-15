import os
from datetime import timedelta
import pytz
import tool.dba as database

os.environ['NUMEXPR_MAX_THREADS'] = '16'

tz = pytz.timezone('Asia/Shanghai')
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

try:
    os.makedirs("线路规划")
except:
    1


def get_address():
    sql_address = """
    SELECT PRODUCT_STORE_ID,
           ADDRESS_TEXT ADDRESS,
           PROVINCE_NAME,
           CITY_NAME,
           DISTRICT_NAME,
           LATITUDE,
           LONGITUDE,
           C.SOURCE AS  source,
           '饿了么'    AS  platform
    FROM TAKEAWAY_ELEME_STORE_INFO A
             INNER JOIN TAKEAWAY_STORE_STATUS B
                        ON A.ID = B.TAKEAWAY_STORE_ID AND B.PLATFORM_TYPE_ID = 'eleme'
             INNER JOIN (
        SELECT SHOP_ID, MAX(CREATED_AT) SOURCE
        FROM ZDEV_ELEME_ORDER_DATA
        GROUP BY SHOP_ID
    ) C ON C.SHOP_ID = B.TAKEAWAY_STORE_ID AND B.PLATFORM_TYPE_ID = 'eleme'
    UNION ALL
    SELECT PRODUCT_STORE_ID,
           ADDRESS,
           null     AS         PROVINCE_NAME,
           null     AS         CITY_NAME,
           null     AS         DISTRICT_NAME,
           LATITUDE / 1000000  LATITUDE,
           LONGITUDE / 1000000 LONGITUDE,
           F.SOURCE AS         source,
           '美团'     as         platform
    FROM TAKEAWAY_MEITUAN_STORE_INFO D
             INNER JOIN TAKEAWAY_STORE_STATUS E
                        ON D.APP_POI_CODE = E.TAKEAWAY_STORE_ID AND E.PLATFORM_TYPE_ID = 'meituan'
             INNER JOIN (
        SELECT APP_POI_CODE, MAX(C_TIME) SOURCE
        FROM ZDEV_MT_ORDER_DETAIL
        GROUP BY APP_POI_CODE
    ) F ON F.APP_POI_CODE = E.TAKEAWAY_STORE_ID
    UNION ALL
    select PRODUCT_STORE_ID,
           ADDRESS,
           PROVINCE_GEO_NAME     PROVINCE_NAME,
           GEO_NAME              CITY_NAME,
           COUNTY_GEO_NAME       DISTRICT_NAME,
           TO_NUMBER(LAT)        LATITUDE,
           TO_NUMBER(LNG)        LONGITUDE,
           CREATED_TX_STAMP    AS source,
           '中台'               as platform
    FROM ORGVIEW
    """
    df_base = Oracle.read("""
    SELECT PRODUCT_STORE_ID, B.STORE_NAME
    FROM (SELECT ORGCODE
          FROM TZYPLUPRO_PS
          WHERE RPTDATE > trunc(sysdate - 91)
          GROUP BY ORGCODE
          UNION
          SELECT ORGCODE
          FROM TZYPLUPRO_PS_RT
          GROUP BY ORGCODE) A
             INNER JOIN ORGVIEW B ON A.ORGCODE = B.PRODUCT_STORE_ID
    GROUP BY PRODUCT_STORE_ID, STORE_NAME
    """)
    df_zt = Oracle.read(sql_address)
    df_tms = pd.read_excel(
        "data/白名单.xlsx", dtype={"PRODUCT_STORE_ID": str}, parse_dates=["SOURCE"])
    df_tms["SOURCE"] = df_zt["SOURCE"].max()
    df_address = pd.concat([df_tms, df_zt])
    df_address = df_address[df_address["PRODUCT_STORE_ID"].str.len() == 8]
    df_address = df_address[df_address["PRODUCT_STORE_ID"].str.startswith(
        '80')]
    df_address = df_address.sort_values(
        by=["PRODUCT_STORE_ID", "SOURCE"], ascending=False).reset_index(drop=True)
    for var in ['ADDRESS', 'PROVINCE_NAME', 'CITY_NAME', 'DISTRICT_NAME']:
        df_address[var] = df_address.groupby(["PRODUCT_STORE_ID"])[
            var].fillna(method="ffill").values
        df_address[var] = df_address.groupby(["PRODUCT_STORE_ID"])[
            var].fillna(method="bfill").values
    df_address = df_address.drop_duplicates(
        subset=["PRODUCT_STORE_ID"]).reset_index(drop=True)
    df_address = df_base.merge(df_address, how="inner", left_on=[
                               "PRODUCT_STORE_ID"], right_on=["PRODUCT_STORE_ID"])
    df_address = df_address.rename(columns={'PRODUCT_STORE_ID': 'ORGCODE'})
    return df_address.drop(["SOURCE", "PLATFORM"], axis=1)


start_day = (datetime.date.today() - timedelta(days=35)
             ).strftime("%Y%m%d%H%M%S")
end_day = (datetime.datetime.now() -
           timedelta(hours=8)).strftime("%Y%m%d%H%M%S")

line = f"""
-- noinspection SqlResolve
SELECT LINE_NAME, LINE_ID, ORGCODE, LINE_TYPE, TRANS_TIME
FROM (
         SELECT DESCRIPTION  AS LINE_NAME,
                SCH_ID       AS LINE_ID,
                LOG_LOCID AS ORGCODE,
                PLAN_TRANS_TIME  AS TRANS_TIME,
                SCH_TYPE     AS LINE_TYPE,
                ROW_NUMBER()    OVER(PARTITION BY LOG_LOCID, SCH_TYPE ORDER BY PLAN_TRANS_TIME DESC) RN
               -- CASE WHEN SCH_TYPE IN ('ZS01', 'ZS02') THEN '干线' ELSE '市配' END AS      LINE_TYPE
         FROM (
                  SELECT B.DESCRIPTION,
                         C.LOG_LOCID,
                         A.PARENT_KEY,
                         -- D.SCH_TYPE,
                         CASE WHEN D.SCH_TYPE IN ('ZS01', 'ZS02') THEN '干线' ELSE '市配' END AS SCH_TYPE,
                         D.SCH_ID,
                         MAX(C.PLAN_TRANS_TIME) PLAN_TRANS_TIME
                  FROM "/SCMTMS/D_TORITE" A
                           INNER JOIN "/SCMTMS/D_SCHDSC" B ON B.PARENT_KEY = A.SCHED_KEY
                           INNER JOIN "/SCMTMS/D_TORSTP" C ON A.PARENT_KEY = C.PARENT_KEY
                           INNER JOIN "/SCMTMS/D_SCHROT" D ON D.DB_KEY = A.SCHED_KEY
                  WHERE PLAN_TRANS_TIME > {start_day}
                  GROUP BY A.PARENT_KEY, C.LOG_LOCID, B.DESCRIPTION, D.SCH_TYPE, SCH_ID
              ) E1
     )
WHERE RN = 1
  AND LINE_NAME IS NOT NULL
  AND LINE_ID IS NOT NULL
  AND ORGCODE IS NOT NULL
ORDER BY TRANS_TIME DESC
"""
df_line = Hana().read(line)
df_line = df_line.sort_values(
    by=["TRANS_TIME"], ascending=False).reset_index(drop=True)
# df_line['LINE_TYPE'] = np.where(df_line['LINE_NAME'] == "武汉机场-海口机场线", "ZS13", df_line['LINE_TYPE'])
df_line = df_line.drop_duplicates(
    subset=["ORGCODE", 'LINE_TYPE']).reset_index(drop=True)
df_line["TRANS_TIME"] = pd.to_datetime(
    df_line["TRANS_TIME"].astype(str), format='%Y%m%d%H%M%S')
df_line["ORGCODE"] = df_line["ORGCODE"].apply(lambda x: x[2:])
df_line = df_line[df_line['LINE_ID'].str.startswith('NC')]
df_line = df_line[~df_line['LINE_TYPE'].isin(['干线'])].reset_index(drop=True)
# df_line = pd.concat([df_line,
#                      pd.DataFrame({"LINE_NAME": ['武汉-孝感线', '武汉市配03线', '武汉市配06线'],
#                                    "LINE_ID": ['WH-SP13', 'WH-SP06', 'WH-SP03'],
#                                    "ORGCODE": ['80070624', '80018362', '80018363']})
#                      ], ignore_index=True)

df_line = df_line[df_line['LINE_NAME'].str.startswith(
    '武汉市配') | df_line['LINE_NAME'].str.startswith('武汉-孝感线')]
df_line = df_line[~df_line['LINE_NAME'].str.contains('盒马')]
df_line = df_line.drop_duplicates(subset=["ORGCODE"]).reset_index(drop=True)
df_line = df_line.sort_values(
    by=["LINE_NAME", "TRANS_TIME"]).reset_index(drop=True)
df_line["ORDER"] = df_line.groupby(
    "LINE_NAME").TRANS_TIME.transform("rank").values
df_line = df_line.drop(["TRANS_TIME", "LINE_TYPE"], axis=1)
sql_ps = """
SELECT PRODUCT_STORE_ID_TO as ORGCODE, POSTING_DATE, COUNT(*) NUMS
FROM (SELECT DOC_ID, POSTING_DATE, PRODUCT_STORE_ID_TO
      FROM DELIVERY_DOC
      UNION
      SELECT DOC_ID, POSTING_DATE, PRODUCT_STORE_ID_TO
      FROM DELIVERY_DOC_HIS
      -- WHERE POSTING_DATE > TRUNC(SYSDATE - 367)
     ) A
         INNER JOIN (SELECT DOC_ID, BOX_ID
                     FROM DELIVERY_BOX
                     UNION
                     SELECT DOC_ID, BOX_ID
                     FROM DELIVERY_BOX_HIS
) B ON A.DOC_ID = B.DOC_ID
GROUP BY PRODUCT_STORE_ID_TO, POSTING_DATE
"""
sql_ps = """
SELECT PRODUCT_STORE_ID_TO as ORGCODE, POSTING_DATE, COUNT(*) NUMS
FROM DELIVERY_DOC A
INNER JOIN DELIVERY_BOX B ON A.DOC_ID = B.DOC_ID AND POSTING_DATE > TRUNC(SYSDATE - 31)  AND POSTING_DATE < TRUNC(SYSDATE +1)
GROUP BY PRODUCT_STORE_ID_TO, POSTING_DATE
"""
df_address = get_address()
df_ps = Oracle.read(sql_ps)
a, b, c = df_ps.ORGCODE.tolist(), df_address.ORGCODE.tolist(), df_line.ORGCODE.tolist()
use_store = list(set(a).intersection(b, c))
df = Oracle.read("""
SELECT RS.PRODUCT_STORE_ID
FROM REPLENISHMENT RS
         INNER JOIN REPLENISHMENT_ITEM RSI
                    ON RS.DOC_ID = RSI.DOC_ID AND RS.POSTING_DATE >= TRUNC(SYSDATE - 35)
         LEFT JOIN PRODUCT_STORE ORG ON ORG.PRODUCT_STORE_ID = RS.PRODUCT_STORE_ID
         LEFT JOIN PRODUCT PLU ON RSI.PRODUCT_ID = PLU.PRODUCT_ID
         LEFT JOIN DELIVERY_PLAN_ITEM DPI ON DPI.PRODUCT_STORE_ID = RS.PRODUCT_STORE_ID
         LEFT JOIN DELIVERY_PLAN_HEADER DPH ON DPH.DOC_ID = DPI.DOC_ID
WHERE 1 = 1
  and DPH.DESCRIPTION LIKE '%武汉%' and DPH.DESCRIPTION LIKE '%成品%'
  AND RS.DOC_STATUS NOT IN ('4', '6')
GROUP BY RS.PRODUCT_STORE_ID
""")
error_store = df_address[df_address.ORGCODE.isin(
    df[~df["PRODUCT_STORE_ID"].isin(use_store)].PRODUCT_STORE_ID)]
df_ps[df_ps.ORGCODE.isin(use_store)].to_csv(
    "线路规划/df_ps.csv", index=False, encoding="gbk")
df_address[df_address.ORGCODE.isin(use_store)].to_excel(
    "线路规划/df_address.xlsx", index=False)
df_line[df_line.ORGCODE.isin(use_store)].to_excel(
    "线路规划/df_line.xlsx", index=False)
df_ps["MONTH"] = df_ps["POSTING_DATE"].dt.month
df_ps["YEAR"] = df_ps["POSTING_DATE"].dt.year
df_ps[df_ps.ORGCODE.isin(use_store)].groupby(["YEAR", "MONTH"]).mean()
df_ps[df_ps.ORGCODE.isin(use_store)].groupby(
    ["POSTING_DATE"]).NUMS.sum().tail(20)
