import tool.dba as database


# 连接数据库
Oracle = database.Oracle()
Hana = database.Hana()
Oracle97 = database.Oracle(
    user="zyngprd", password='zyngprd', dsn="10.10.201.97:1521/crmngpsd")


predict = Oracle.read("""
SELECT
    RPTDATE,
    ORGCODE,
    PLUCODE,
    YH_PRODUCT_ID,
    YH_COUNT
FROM
    ZCB_PREDICT_STORE_YY
WHERE
    RPTDATE = TO_DATE('2022-12-31','YYYY-MM-DD')
""")


actually = Oracle97.read("""
SELECT
    RPTDATE,
    ORGCODE,
    ORGNAME,
    PLUCODE,
    PLUNAME,
    PLUCOUNT
FROM
    TZYPLUPRO_YH
WHERE
    RPTDATE = TO_DATE('2022-12-31','YYYY-MM-DD')
""")


1.配送（ZDEV_PS_DOC ZDEV_PS_ITEM）两个表会有相同的字段AMOUNT、AMOUNT1、AMOUNT2
ZDEV_PS_ITEM 中AMOUNT由quantity*price得到, 即单个订单的金额
ZDEV_PS_DOC 中AMOUNT？（猜测是店家累计的AMOUNT）

2.配送（ZDEV_PS_ITEM）与 退货（ZDEV_TH_ITEM）中 price、price1、price2都不同，是不是意味着进货和退货都是不同的单价
按照少山的逻辑，使用amount3（quantity*price3）计算

3.配送表中amount、amount1、amount2分别对应给二级经销商、一级经销商、门店的金额，那我们计算临保退货的时候那个分母是选amount1嘛


4.原料只有price1（没有price、price2），是不是只有一级经销商才可以拿原料，而成品三个price都有

5.财务配送数据会出现amount amount1 amount2 都是空值情况（这种情况是否正常）

oracle97.read("""
SELECT YEAR, SUM(AMOUNT)
FROM (SELECT YEAR,PLUCODE,SUM(PLUTOTAL) AMOUNT
FROM TZYPLUPRO_PS
WHERE 1 = 1
AND RPTDATE >= TO_DATE('2021-01-01', 'YYYY-MM-DD')
AND RPTDATE < TO_DATE('2022-12-31', 'YYYY-MM-DD')
GROUP BY YEAR,PLUCODE
UNION ALL
SELECT YEAR,PLUCODE,-1 * SUM(PLUTOTAL) AMOUNT
FROM TZYPLUVIEW_TH
WHERE DZ_DATE >= TO_DATE('2021-01-01', 'YYYY-MM-DD')
AND DZ_DATE < TO_DATE('2022-12-31', 'YYYY-MM-DD')
GROUP BY YEAR,PLUCODE)
INNER JOIN (SELECT DISTINCT PRODUCT_ID, INTERNAL_NAME, PRODUCT_CATEGORY_NAME, UOM
FROM PLUVIEW
WHERE UOM = '千克'
AND PRODUCT_CATEGORY_NAME = '散装食品')
ON PRODUCT_ID = PLUCODE
GROUP BY YEAR""")


oracle97.read("""
SELECT YEAR,PLUCODE,SUM(PLUTOTAL) AMOUNT
FROM TZYPLUPRO_TH
WHERE 1 = 1
AND RPTDATE >= TO_DATE('2021-01-01', 'YYYY-MM-DD')
AND RPTDATE <= TO_DATE('2022-12-31','YYYY-MM-DD')
GROUP BY YEAR,PLUCODE""")


oracle97.read("""SELECT DISTINCT PRODUCT_ID, INTERNAL_NAME, PRODUCT_CATEGORY_NAME, UOM
FROM PLUVIEW
WHERE UOM = '千克'
AND PRODUCT_CATEGORY_NAME = '散装食品'""")
