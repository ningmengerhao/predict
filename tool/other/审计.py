import pandas as pd
from tqdm import tqdm
import numpy as np
from pandas.testing import assert_frame_equal

select_var = ['公司代码', '外部凭证号', '行项目', '外部过帐日期', '内部过帐日期',
              '内部凭证号', '科目号', '借贷标识', '金额', '客户编号', '供应商编号']
# select_var = ['公司代码', '公司名称', '财年', '会计期间', '外部凭证号', '行项目', '外部过帐日期', '内部过帐日期',
#               '外部凭证类型', '内部凭证类型', '制单人', '外部凭证抬头文本', '附件数量', '科目号', '成本中心',
#               '功能范围', '供应商编号', '供应商名称', '客户编号', '客户名称', '资产号', '资产分类',
#               '原因代码', '评估类', '借贷标识', '金额', '货币码', '项目文本', '外部凭证号-冲销', '内部凭证号',
#               '内部凭证号-冲销', '内部凭证年度-冲销', ]
#


def select_data(row, df):
    p = df[(df["公司代码"] == row["公司代码"]) & (df["外部凭证号"] == row["外部凭证号"])
           & (df["行项目"] == row["行项目"])][select_var].copy()
    if len(p) == 1:
        p.index = [0]
    p = p.dropna(axis=1)
    return p


# df1 = pd.read_excel("【紫燕】序时账2021年H1-年报审计导出20220206.xlsx")
# df2 = pd.read_excel("【紫燕】序时账2021年H1-半年报审计导出.XLSX")
# df1.to_feather("df1.feather")
# df2.to_feather("df2.feather")
df1 = pd.read_feather("df1.feather")
df2 = pd.read_feather("df2.feather")
flag = []
error = []
for idx1, row1 in tqdm(df1.iterrows()):
    p1 = select_data(row1, df1)
    p2 = select_data(row1, df2)
    var = list(set(p1.columns).intersection(set(p2.columns)))
    p1 = p1[var]
    p2 = p2[var]
    flag.append(p1.equals(p2))
    if p1.equals(p2):
        1
    else:
        # print("\n")
        try:
            result = pd.concat(
                [(p1 == p2).T, p1[var].T, p2[var].T, ], ignore_index=True, axis=1)
            result = result.reset_index()
            result.columns = ["变量", "判断", "年报", "半年报"]
            result["行"] = idx1 + 1
            error.append(result)
            print(result[result["判断"] == False])
        except:
            1
        # print(result)
df1["标记"] = flag
error = pd.concat(error, ignore_index=True)
error = error.drop_duplicates()
df1[["标记"]].to_csv("test.csv", index=False, encoding="gbk")
error.to_csv("error.csv", index=False, encoding="gbk")
print(error)
