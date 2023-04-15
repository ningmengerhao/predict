# %%
import json
import pandas as pd
import tool.dba as database


# 连接数据库
Oracle = database.Oracle()
Hana = database.Hana()
Oracle97 = database.Oracle(
    user="zyngprd", password='zyngprd', dsn="10.10.201.97:1521/crmngpsd")

# %%
df = Oracle97.read("select * from ORGVIEW")
df


# %%
df = Hana.read('''SELECT
                 AC.RBUKRS AS BUKRS ,  --公司代码
                 AC.WERKS  AS WERKS ,  --工厂
                 AC.MATNR  AS MATNR ,  --物料代码
                 MW.BKLAS  AS BKLAS ,  --评估类
                 AC.RACCT  AS HKONT,   --科目
                 AC.BUDAT,
                 AC.BLART,
                 AC.HSL,
                 AC.DRCRK
            FROM
                ACDOCA AS AC
                -- INNER JOIN :TAB_HKONT AS HKT ON AC.RACCT = HKT.HKONT
                LEFT JOIN MBEW AS MW ON MW.MATNR = AC.MATNR AND MW.BWKEY = AC.WERKS AND MW.BWTAR = ''
            WHERE
                AC.RLDNR = '0L' AND
                AC.RCLNT  = 100 AND
                (AC.BSTAT = '' OR AC.BSTAT = 'U')
            --     AC.BUDAT <= :EndDate AND
            LIMIT
                100;
''')

df.rename(columns={'BUKRS': '公司代码', 'WERKS': '工厂代码',
                   'MATNR': '物料代码', 'HKONT': '科目', 'BKLAS': '评估类'}, inplace=True)

# %%
# sql文件夹路径

sql_path = r'C:\Users\6433\Desktop\紫燕\SQL'


# sql文件名

os.path.join(sql_path, "")

sql_file = '干线推荐要货模型.sql'
# 读取 sql 文件文本内容
sql = open(sql_path + sql_file, 'r', encoding='utf8')
sqltxt = sql.readlines()


sql = open(r"C:\Users\6433\Desktop\紫燕\SQL\干线推荐要货模型.sql", 'r', encoding='utf-8')
sqltxt = sql.readlines()

# 此时 sqltxt 为 list 类型

# 读取之后关闭文件
sql.close()

# list 转 str
sql = "".join(sqltxt)

# charset用于修正中文输出为问号的问题
# df = pd.read_sql(sql=sql,con=Oracle)
# Oracle.close()


# 物料的期初期末金额:


"""
SELECT
    AC.RBUKRS AS BUKRS ,  --公司代码
    AC.WERKS  AS WERKS ,  --工厂
    AC.MATNR  AS MATNR ,  --物料代码
    MW.BKLAS  AS BKLAS ,  --评估类
    AC.RACCT  AS HKONT ,  --科目

    AC.HSL --金额数据
    AC.BUDAT --日期

    AC.BLART --分类（PR、RE、Z1）
    AC.DRCRK (S：借方；H：贷方)

FROM
    ACDOCA AS AC
    INNER JOIN :TAB_HKONT AS HKT ON AC.RACCT = HKT.HKONT
    LEFT OUTER JOIN MBEW AS MW ON MW.MATNR = AC.MATNR AND MW.BWKEY = AC.WERKS AND MW.BWTAR = ''
WHERE
    AC.RLDNR = '0L'
    AND AC.RCLNT  = 100 --SAP查询
    AND (AC.BSTAT = '' OR AC.BSTAT = 'U')
"""


"""
SELECT
    AC.BUKRS AS BUKRS, --公司代码
    AC.WERKS AS WERKS, --工厂
    AC.MATNR AS MATNR, --物料代码
    AC.BKLAS AS BKLAS, --评估类
    AC.HKONT AS HKONT,  --科目
    SUM(AC.HSL_QJ_RE) AS HSL_QJ_RE, --RE金额-期间发生
    SUM(AC.HSL_QJ_PR) AS HSL_QJ_PR, --PR金额-期间发生
    SUM(AC.HSL_QJ_Z1) AS HSL_QJ_Z1, --Z1金额-期间发生
    SUM(AC.HSL_QJ) AS HSL_QJ, --金额-期间发生
    SUM(AC.HSL_QJ_S) AS HSL_QJ_S, --金额-期间发生-借方
    SUM(AC.HSL_QJ_H) AS HSL_QJ_H, --金额-期间发生-贷方
    SUM(AC.HSL_QC) AS HSL_QC, --金额-期初
    SUM(AC.HSL_QM) AS HSL_QM, --金额-期末
    SUM(AC.HSL_LJ_S) AS HSL_LJ_S, --金额-本年累计发生-借方
    SUM(AC.HSL_LJ_H) AS HSL_LJ_H - -金额-本年累计发生-贷方
FROM
    ACDOCA
GROUP BY
    BUKRS,
    WERKS,
    MATNR,
    BKLAS,
    HKONT
"""
