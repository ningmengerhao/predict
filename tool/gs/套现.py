import pandas as pd
from tqdm import tqdm
import numpy as np
from tool.dba import oracle97

np.set_printoptions(suppress=True)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

df_header = []
for day in tqdm(range(1, 31)):
    df = oracle97.read(
        f"""
    SELECT A.RPTDATE 业务日期,
           A.DOC_ID 销售流水号,
           A.STORE_ID 门店编号,
           A.BASE_ENTRY,
           A.XSDATE 销售日期,
           A.TAKEAWAY_DATE 外卖下单日期,
           A.SCDATE 上传时间,
           A.STORE_NAME 门店名称,
           A.ZFTYPE 支付方式,
           A.PAYMENT_NAME 支付名称,
           A.PAYMENT_TYPE_NAME 移动支付类别,
           A.REBATE_AMOUNT 订单总额,
           A.PAY_DOC 支付订单号,
           A.USER_LOGIN_ID 顾客账号,
           A.SYS_TRANSACTION_ID 移动交易号,
           A.PAYTY_ID ,
           A.EXT_DOC_NO,
           A.EXT_NO,
           A.AMOUNT 支付金额,
           A.REBATE_AMOUNT - A.AMOUNT 优惠金额,
           A.ORGTYPE,
           A.GEO_NAME,
           A.GROUP_NAME

    FROM (
             SELECT TO_CHAR(SOH.POSTING_DATE, 'YYYY-MM-DD') AS RPTDATE,
                    SOH.DOC_ID,
                    NULL                                    AS BASE_ENTRY,
                    SOH.CREATE_DOC_DATE                     AS XSDATE,
                    SOH.LAST_UPDATED_TX_STAMP               AS SCDATE,
                    (CASE
                         WHEN COD.TYPE IN (SELECT PAYMENT_ID FROM PAYVIEW_WM) THEN TO_CHAR(SOH.TAKEAWAY_DATE, 'YYYY-MM-DD')
                         ELSE NULL END)                     AS TAKEAWAY_DATE,
                    SOH.STORE_ID,
                    ORG.STORE_NAME,
                    COD.TYPE                                AS ZFTYPE,
                    PMT.DESCRIPTION                         AS PAYMENT_NAME,
                    MPT.PAYMENT_TYPE_NAME,
                    SOH.REBATE_AMOUNT,
                    COD.ATTR_NAME1                          AS PAY_DOC,
                    COD.ATTR_NAME2                          AS USER_LOGIN_ID,
                    COD.ATTR_NAME3                          AS SYS_TRANSACTION_ID,
                    NULL                                    AS PAYTY_ID,
                    SOH.EXT_DOC_NO,
                    COD.EXT_NO,
                    COD.AMOUNT,
                    ORG.ORGTYPE,
                    ORG.GEO_NAME,
                    ORG.GROUP_NAME
             FROM SALE_ORDER_HEADER SOH
                      INNER JOIN COLLECTION_ORDER_DTL COD
                                 ON SOH.DOC_ID = COD.DOC_ID
                      INNER JOIN ORGVIEW ORG ON SOH.STORE_ID = ORG.PRODUCT_STORE_ID
                      LEFT JOIN PAYMENT_METHOD_TYPE PMT ON COD.TYPE = PMT.PAYMENT_METHOD_TYPE_ID
                      LEFT JOIN MIYA_PAYMENT_TYPE MPT ON COD.ATTR_NAME4 = MPT.PAYMENT_TYPE_ID
             WHERE 1 = 1 and COD.POSTING_DATE = trunc(sysdate-{day})
             UNION ALL
             SELECT TO_CHAR(RTOH.POSTING_DATE, 'YYYY-MM-DD') AS RPTDATE,
                    RFOD.DOC_ID,
                    RTOH.BASE_ENTRY,
                    RTOH.CREATE_DOC_DATE                     AS XSDATE,
                    RTOH.LAST_UPDATED_TX_STAMP               AS SCDATE,
                    (CASE
                         WHEN RFOD.TYPE IN (SELECT PAYMENT_ID FROM PAYVIEW_WM)
                             THEN TO_CHAR(RTOH.TAKEAWAY_DATE, 'YYYY-MM-DD')
                         ELSE NULL END)                      AS TAKEAWAY_DATE,
                    RTOH.STORE_ID,
                    ORG.STORE_NAME,
                    RFOD.TYPE                                AS ZFTYPE,
                    PMT.DESCRIPTION                          AS PAYMENT_NAME,
                    MPT.PAYMENT_TYPE_NAME,
                    -RTOH.REBATE_AMOUNT,
                    RFOD.ATTR_NAME1                          AS PAY_DOC,
                    RFOD.ATTR_NAME2                          AS USER_LOGIN_ID,
                    RFOD.ATTR_NAME3                          AS SYS_TRANSACTION_ID,
                    NULL                                     AS PAYTY_ID,
                    SOH.EXT_DOC_NO,
                    RFOD.EXT_NO,
                    -RFOD.AMOUNT,
                    ORG.ORGTYPE,
                    ORG.GEO_NAME,
                    ORG.GROUP_NAME
             FROM RETURN_ORDER_HEADER RTOH
                      INNER JOIN REFUND_ORDER_DTL RFOD
                                 ON RTOH.DOC_ID = RFOD.DOC_ID
                      INNER JOIN ORGVIEW ORG ON RTOH.STORE_ID = ORG.PRODUCT_STORE_ID
                      LEFT JOIN PAYMENT_METHOD_TYPE PMT ON RFOD.TYPE = PMT.PAYMENT_METHOD_TYPE_ID
                      LEFT JOIN MIYA_PAYMENT_TYPE MPT ON RFOD.ATTR_NAME4 = MPT.PAYMENT_TYPE_ID
                      LEFT JOIN SALE_ORDER_HEADER SOH ON RTOH.BASE_ENTRY = SOH.DOC_ID
             WHERE 1 = 1 and RTOH.POSTING_DATE = trunc(sysdate-{day})
             UNION ALL
             SELECT TO_CHAR(PIOH.POSTING_DATE, 'YYYY-MM-DD') AS RPTDATE,
                    PIOH.DOC_ID,
                    '门店结账' || PIOH.AMOUNT                    AS BASE_ENTRY,
                    PIOH.CREATE_DOC_DATE                     AS XSDATE,
                    PIOH.LAST_UPDATED_TX_STAMP               AS SCDATE,
                    NULL                                     AS TAKEAWAY_DATE,
                    PIOH.STORE_ID,
                    ORG.STORE_NAME,
                    NULL                                     AS ZFTYPE,
                    NULL                                     AS PAYMENT_NAME,
                    NULL                                     AS PAYMENT_TYPE_NAME,
                    NULL                                     AS REBATE_AMOUNT,
                    NULL                                     AS PAY_DOC,
                    NULL                                     AS USER_LOGIN_ID,
                    NULL                                     AS SYS_TRANSACTION_ID,
                    NULL                                     AS PAYTY_ID,
                    NULL                                     AS EXT_DOC_NO,
                    NULL                                     AS EXT_NO,
                    NULL                                     AS AMOUNT,
                    ORG.ORGTYPE,
                    ORG.GEO_NAME,
                    ORG.GROUP_NAME
             FROM PAYMENT_IN_ORDER_HEADER PIOH
                      INNER JOIN ORGVIEW ORG
                                 ON PIOH.STORE_ID = ORG.PRODUCT_STORE_ID
             WHERE 1 = 1 and PIOH.POSTING_DATE = trunc(sysdate-{day})
         ) A
    ORDER BY A.RPTDATE, A.STORE_ID, A.XSDATE
        """
    )
    df.to_feather(f"data/basic/{day}.feather")
    df = pd.read_feather(f"data/basic/{day}.feather")
    df = df[['业务日期', '销售流水号', '门店编号', '门店名称', '支付方式', '支付名称', '移动支付类别', '订单总额', '顾客账号', '支付金额', 'GEO_NAME', 'ORGTYPE']]
    df = df.rename(columns={'GEO_NAME': '地区名称', 'ORGTYPE': '门店性质'})
    df_header.append(df)
df_header = pd.concat(df_header)
df_header = df_header.sort_values(by="销售流水号").reset_index(drop=True)
df_header = df_header[df_header["订单总额"] > 0]
df_base = df_header[["销售流水号", "顾客账号"]].drop_duplicates().dropna()
df_order = df_header.groupby(['业务日期', '销售流水号', '门店编号', '门店名称', '地区名称', '门店性质'], as_index=False).agg({'订单总额': 'mean', '支付金额': 'sum'})
df_order = df_order.merge(df_base, how="inner", left_on=["销售流水号"], right_on=["销售流水号"])
df_summary = df_order.groupby(["顾客账号"], as_index=False)["订单总额"].agg(["sum", "count"])
df_summary.columns = ["订单总额", "订单数"]
df_summary = df_summary[df_summary["订单数"] > 30]
df_summary["套现金额"] = df_summary["订单总额"] * 8 / 48
df_order = df_order[df_order["顾客账号"].isin(df_summary.index)].copy()
df_write = df_header[df_header["销售流水号"].isin(df_order["销售流水号"])].copy()
df_write = df_write[df_write["门店性质"] != "直营店"]
df_write.to_excel("实际情况.xlsx", index=False)
df_write.groupby(['地区名称', '门店编号', '门店名称', '门店性质'], as_index=False)["订单总额"].count().to_excel("汇总情况.xlsx", index=False)
for i in range(15, 101):
    df_summary = df_summary[df_summary["订单数"] > i]
    print(i, df_summary["订单总额"].sum(), df_summary["订单数"].sum(), len(df_summary), df_summary["订单总额"].sum() * 8 / 48)
# df_write
