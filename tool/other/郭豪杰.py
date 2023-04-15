from tool.dba import *

start_day = '20201231'
end_day = '20220228'
# df_pd = Hana().read(f"""
# SELECT M.MATNR      AS "物料编码",
#        M.WERKS      AS "工厂编码",
#        SUM(M.MENGE) AS "期末库存"
# FROM (
#          SELECT MATDOC.MATNR                                                             AS MATNR,
#                 MATDOC.WERKS                                                             AS WERKS,
#                 CASE WHEN MATDOC.SHKZG = 'S' THEN MATDOC.MENGE ELSE 0 - MATDOC.MENGE END AS MENGE
#          FROM MATDOC
#          WHERE MATDOC.BSTAUS_SG <> 'C'
#            AND MATDOC.MANDT = '100'
#            AND MATDOC.BUDAT <= '{start_day}') AS M
# GROUP BY MATNR, WERKS
# having SUM(M.MENGE) > 0""")
df_pd = pd.read_excel("2020.12.31.xlsx")
df_use = Hana().read(f"""
SELECT A.WERKS AS "工厂编码",
       A.MATBF AS "物料编码",
       A.BUDAT AS "发生日期",
       A.BWART AS "移动类型",
       A.MENGE AS "数量"
FROM MATDOC A
WHERE A.BUDAT > '{start_day}' 
  AND A.BUDAT <= '{end_day}'  
  AND A.BWART in
      ('102', '122', '161', '261', '309', '310', '532', '601', '633', '654', 'T01', 'Z01', 'Z05', 'Z07', 'Z11', 'Z13',
       'Z16', 'Z17', 'Z19', 'Z23', 'Z28', 'Z31')
  AND A.SHKZG = 'H'
  """)
df_pd["物料编码"] = df_pd["物料编码"].apply(lambda x: '0000000000000' + str(x)[-5:])
df_pd["工厂编码"] = df_pd["工厂编码"].astype(str)
df_use = df_use.sort_values(by=["工厂编码", "物料编码", "发生日期"]).reset_index(drop=True)
df_use["数量"] = df_use["数量"].astype(float)
df_pd["期末库存"] = df_pd["期末库存"].astype(float)
df_use["发生日期"] = df_use["发生日期"].astype(int)
df_use["累积数量"] = df_use.groupby(["工厂编码", "物料编码"])["数量"].transform("cumsum")
df = df_pd.merge(df_use, how="left", left_on=["工厂编码", "物料编码"], right_on=["工厂编码", "物料编码"])
df["期末库存"] = df["期末库存"].fillna(0)
df["组合编码"] = df["工厂编码"] + df["物料编码"]
df_summary = df[df["累积数量"] - df["数量"] > df["期末库存"]].groupby(["工厂编码", "物料编码"], as_index=False)["发生日期"].min()
df_summary.columns = ["工厂编码", "物料编码", '清空日期']
df = df.merge(df_summary, how="left", left_on=["工厂编码", "物料编码"], right_on=["工厂编码", "物料编码"])
df = df[(df["发生日期"] <= df["清空日期"]) | (df["清空日期"].isnull())].reset_index(drop=True)
df["类型"] = np.where(df["清空日期"].isnull(), "未清空", "已清空")
C = df[df["组合编码"] == '7240000000000000010001']
df[["工厂编码", "物料编码", "组合编码", "发生日期", "移动类型", "数量", "期末库存", "类型"]].to_excel("结果表2021.xlsx", index=False)
print(df)
