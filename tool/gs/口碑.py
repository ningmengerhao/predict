import pandas as pd
import numpy as np
from tqdm import tqdm
import pulp

df_full = []
df = pd.read_excel("test/20210701苟思测试/方程求解.xlsx")
df['活动价2'] = np.where(df['活动价2'].isnull(), df['活动价1'], df['活动价2'])
df = df.dropna(subset=['活动价1', '活动价2', '挂牌价', '原价', '核销金额', "核销次数(包含次卡)"]).reset_index(drop=True)
var_name = ['活动价1', '活动价2', '挂牌价', '原价']

for row in tqdm(df.to_dict('records')):
    MyProbLP = pulp.LpProblem("LPProbDemo1", sense=pulp.LpMinimize, )
    x1 = pulp.LpVariable('x1', lowBound=0, upBound=row["核销金额"] / row['活动价1'], cat='Integer')
    if row["活动价1"] == row["活动价2"]:
        x2 = pulp.LpVariable('x2', lowBound=0, upBound=0, cat='Integer')
    else:
        x2 = pulp.LpVariable('x2', lowBound=0, upBound=row["核销金额"] / row['活动价2'], cat='Integer')
    x3 = pulp.LpVariable('x3', lowBound=0, upBound=row["核销金额"] / row['挂牌价'], cat='Integer')
    x4 = pulp.LpVariable('x4', lowBound=0, upBound=row["核销金额"] / row['原价'], cat='Integer')
    MyProbLP += row['核销金额'] - row['活动价1'] * x1 + row['活动价2'] * x2 + row['挂牌价'] * x3 + row['原价'] * x4
    MyProbLP += (row['活动价1'] * x1 + row['活动价2'] * x2 + row['挂牌价'] * x3 + row['原价'] * x4 == row['核销金额'])
    MyProbLP += (x1 + x2 + x3 + x4 == row["核销次数(包含次卡)"])
    MyProbLP.solve(pulp.PULP_CBC_CMD(gapRel=0.01, msg=False, timeLimit=60))
    res = np.array([v.varValue for v in MyProbLP.variables()]).reshape(-1, len(var_name))
    df_param = pd.DataFrame(res, columns=["求解%s次数" % var for var in var_name])
    df_param = df_param[np.round(np.abs(np.sum(df_param, axis=1) - row["核销次数(包含次卡)"]), 3) == 0]
    df_param["金额总和"] = 0
    for var in var_name:
        df_param["金额总和"] += df_param["求解%s次数" % var] * row[var]
    df_param["ERROR"] = np.round(np.abs(df_param["金额总和"] - row["核销金额"]), 3)
    df_param = df_param[df_param["ERROR"] == 0].sort_values(by=["ERROR"], ascending=False).reset_index(drop=True)
    df_solve = df_param.to_dict('records')[0]
    res = {**row, **df_solve}
    df_full.append(pd.DataFrame(res, index=[1]))
df_full = pd.concat(df_full)
df_full.to_excel("输出结果.xlsx", index=False)
