# import tool.dba as database
from sklearn.model_selection import KFold
from catboost import CatBoostRegressor
from mip import Model, INTEGER, xsum
from chinese_calendar import *
import pytz
import logging
import numpy as np
import pandas as pd
from datetime import date, timedelta
import datetime
import os
import sys


print('当前路径是：'+os.getcwd())

Oracle = database.Oracle()
Hana = database.Hana()
Oracle97 = Oracle(user="zyngprd", password='zyngprd',
                  dsn="10.10.201.97:1521/crmngpsd")

tz = pytz.timezone('Asia/Shanghai')

car = pd.DataFrame({'CAR_NAME': ['L042', 'L076', 'L096'], 'VOLUME': [
                   190, 560, 760], 'PRICE': [4.2, 6, 7.4]})


def get_car(nums):
    n = len(car)
    p = car.PRICE.tolist()
    v = car.VOLUME.tolist()
    model = Model()
    x = [model.add_var(var_type=INTEGER) for i in range(n)]
    model.objective = xsum(p[i] * x[i] for i in range(n))
    model += xsum(v[i] * x[i] for i in range(n)) * 1 >= nums
    model.verbose = 0
    model.optimize()
    choose = np.array([x[i].x for i in range(n)])
    cars = []
    for a, b in zip(v, choose):
        cars.extend([int(a)] * int(b))
    description = ["%d辆%s" % (x[i].x, car.CAR_NAME[i])
                   for i in range(n) if x[i].x > 0]
    return [", ".join(description), int(np.sum(choose * v)), cars]


class Logistics:
    def __init__(self):
        self.predict_days = 7
        self.history_days = 35
        self.today = pd.to_datetime(
            datetime.datetime.now(tz).strftime('%Y-%m-%d'))
        self.summary_col = ['LINE_NAME', "LINE_ID", 'DS']
        self.target_col = 'NUMS'
        self.group_col = ['LINE_NAME', "LINE_ID"]
        self.date_col = 'DS'
        self.sql_ps = """
        SELECT PRODUCT_STORE_ID_TO AS STORE_ID, POSTING_DATE DS, COUNT(*) NUMS
        FROM (SELECT DOC_ID, POSTING_DATE, PRODUCT_STORE_ID_TO
              FROM DELIVERY_DOC
              -- WHERE PRODUCT_STORE_ID_TO = '80010001'
              UNION ALL
              SELECT DOC_ID, POSTING_DATE, PRODUCT_STORE_ID_TO
              FROM DELIVERY_DOC_HIS
              -- WHERE PRODUCT_STORE_ID_TO = '80010001'
             ) A
                 INNER JOIN (SELECT DOC_ID, BOX_ID
                             FROM DELIVERY_BOX
                             UNION ALL
                             SELECT DOC_ID, BOX_ID
                             FROM DELIVERY_BOX_HIS
        ) B ON A.DOC_ID = B.DOC_ID
        GROUP BY PRODUCT_STORE_ID_TO, POSTING_DATE
        """

        # self.sql_ps = """
        # SELECT PRODUCT_STORE_ID_TO as STORE_ID, POSTING_DATE DS, COUNT(*) NUMS
        # FROM DELIVERY_DOC A
        # INNER JOIN DELIVERY_BOX B ON A.DOC_ID = B.DOC_ID AND POSTING_DATE > TRUNC(SYSDATE - 31) AND POSTING_DATE < TRUNC(SYSDATE +1)
        # GROUP BY PRODUCT_STORE_ID_TO, POSTING_DATE
        # """

        self.line_info = f"""
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
        """

        self.sql_holiday = """
        SELECT RPTDATE DS, FESTIVALS_NL, FESTIVALS_GL
        FROM ZCB_DAY_INFO 
        ORDER BY RPTDATE
        """

    def get_data(self):
        df_line = Hana.read(self.line_info)
        df_line["STORE_ID"] = df_line["STORE_ID"].apply(lambda x: x[2:])
        df_line["LINE_ID"] = df_line["LINE_ID"].apply(
            lambda x: x.split("GX")[0][:-1] if "GX" in x else x)
        df_line["LENGTH"] = df_line.apply(lambda x: len(
            df_line["LINE_NAME"]) if "SP" in x else len(x["LINE_ID"]), axis=1)
        # df_line["LINE_ID"] = df_line["LINE_ID"].apply(lambda x: x.split("GX")[0][:-1] if "GX" in x else x.split("SP")[0][:-1])
        df_line["LINE_NAME"] = df_line.apply(
            lambda x: x["LINE_NAME"][:x["LENGTH"]], axis=1)
        df_input = Oracle97.read(self.sql_ps)
        df_input = df_input.merge(df_line.drop(
            "LENGTH", axis=1), how="inner", left_on="STORE_ID", right_on="STORE_ID")
        df_summary_sp = df_input[df_input["LINE_TYPE"] == "市配"].groupby(
            ["LINE_NAME", 'LINE_ID', "DS"], as_index=False).sum()
        df_summary_sp.to_csv("市配.csv", index=False, encoding="gbk")

        df_summary_sp = df_summary_sp.groupby(
            ["LINE_NAME", 'LINE_ID'], as_index=False).mean()
        df_view = df_input[df_input[self.date_col] > df_input[self.date_col].max(
        ) - timedelta(days=30)].groupby(["STORE_ID", "LINE_TYPE", "LINE_NAME", "LINE_ID"], as_index=False).mean()
        df_view = pd.pivot_table(df_view, index=["STORE_ID"], columns=[
                                 "LINE_TYPE"], values="LINE_NAME", aggfunc=lambda x: "".join(x)).reset_index()
        df_input["STORE_NUM"] = 1
        df_input = df_input[df_input["LINE_TYPE"] == "干线"].groupby(
            ["LINE_NAME", 'LINE_ID', "DS"], as_index=False).sum()
        df_input = df_input.sort_values(
            by=["LINE_NAME", 'LINE_ID', "DS"]).reset_index(drop=True)
        # 去除2日一配线路
        df_summary = df_input.groupby(
            ["LINE_NAME"]).DS.describe(datetime_is_numeric=True)
        df_summary = df_summary[(
            df_summary['max'] - df_summary['min']).dt.days > 0.55 * df_summary["count"]]
        df_input = df_input[df_input["LINE_NAME"].isin(
            df_summary.index.values)].reset_index(drop=True)
        # 去除新线路
        df_summary = df_input.groupby(
            ["LINE_NAME"], as_index=False).NUMS.mean()
        df_summary = df_summary[df_summary["NUMS"] > 180]
        df_input = df_input[df_input["LINE_NAME"].isin(
            df_summary.LINE_NAME.values)].reset_index(drop=True)
        df_group = df_input.groupby(["LINE_NAME", 'LINE_ID'], sort=False)
        df_input['Q10'] = df_group[self.target_col].rolling(
            self.history_days, min_periods=7).quantile(0.1).values
        df_input['Q90'] = df_group[self.target_col].rolling(
            self.history_days, min_periods=7).quantile(0.9).values
        df_input['TRUE'] = df_group[self.target_col].shift(
            -self.predict_days).values
        df_input['TRUE_STD'] = (
            df_input['TRUE'] - df_input['Q10']) / (df_input['Q90'] - df_input['Q10'])
        df_input[self.date_col] = df_input[self.date_col] + \
            timedelta(days=self.predict_days)
        df_view = df_view[df_view["干线"].isin(df_input["LINE_NAME"])]
        df_view = df_view.groupby(
            ["干线", "市配"], as_index=False).STORE_ID.count()
        df_view = df_view.sort_values(by=["STORE_ID"], ascending=False)
        df_view = df_view.dropna().drop_duplicates(
            subset=["市配"]).reset_index(drop=True).drop("STORE_ID", axis=1)
        df_view = df_view.merge(df_summary_sp, how="left", left_on=["市配"], right_on=[
                                "LINE_NAME"]).drop("LINE_NAME", axis=1)
        return df_input, df_view

    def get_holiday(self, df_input):
        df_holiday = Oracle().read(self.sql_holiday)
        for var in ['FESTIVALS_NL', 'FESTIVALS_GL']:
            for day_num in range(-1, 2):
                if day_num > 0:
                    df_holiday["%s(t-%02d)" % (var, day_num)
                               ] = df_holiday[var].shift(day_num)
                if day_num < 0:
                    df_holiday["%s(t+%02d)" % (var, np.abs(day_num))
                               ] = df_holiday[var].shift(day_num)
        df_holiday["HOLIDAY"] = df_holiday['DS'].apply(
            is_holiday).replace([True, False], ["1", "0"])
        df_holiday["HOLIDAY_T1"] = df_holiday["HOLIDAY"].shift(-1).fillna("0")
        df_holiday["HOLIDAY_T-1"] = df_holiday["HOLIDAY"].shift(1).fillna("0")
        df_holiday["WEEK_ADJ"] = df_holiday['DS'].dt.month.astype(
            str) + df_holiday["HOLIDAY_T-1"] + df_holiday["HOLIDAY"] + df_holiday["HOLIDAY_T1"]
        df_holiday = df_holiday.drop(
            ['HOLIDAY', 'HOLIDAY_T1', 'HOLIDAY_T-1'], axis=1)

        df_input = df_input.merge(
            df_holiday, how="left", left_on=self.date_col, right_on=self.date_col)
        df_input["HOLIDAY_AFFECT"] = np.nan
        for var in df_holiday.columns[1:]:
            df_input[var] = df_input.groupby(
                self.group_col + [var], sort=False)["TRUE_STD"].transform('mean').values
            df_input[var] = df_input[var].fillna(
                df_input.groupby(self.date_col)[var].transform('mean'))
            df_input["HOLIDAY_AFFECT"] = np.where(df_input["HOLIDAY_AFFECT"].isnull(),
                                                  df_input[var].values,
                                                  df_input["HOLIDAY_AFFECT"].values)
        df_input = df_input.sort_values(
            by=["LINE_ID", "LINE_NAME", "WEEK_ADJ", "DS"]).reset_index(drop=True)
        for day in [1, 2, 3, 4]:
            df_input["T%02d" % day] = df_input.groupby(
                ["LINE_ID", "LINE_NAME", "WEEK_ADJ"])["TRUE_STD"].shift(day)
        df_input = df_input.sort_values(
            by=["LINE_ID", "LINE_NAME", "DS"]).reset_index(drop=True)
        return df_input

    def predict(self, df_input):
        feature = [x for x in df_input.columns if
                   x not in self.summary_col + ['Q10', 'Q90', 'TRUE', 'TRUE_STD', 'Y']]
        x = df_input.dropna(subset=["TRUE_STD"])[feature].values
        y = df_input.dropna(subset=["TRUE_STD"])['TRUE_STD'].values
        y_predict = np.zeros(len(df_input)).flatten()
        kf = KFold(5, shuffle=True, random_state=1)
        for k, (train_index, test_index) in enumerate(kf.split(x, y)):
            x_train, x_test = x[train_index], x[test_index]
            y_train, y_test = y[train_index], y[test_index]
            model = CatBoostRegressor(iterations=500,
                                      task_type="GPU",
                                      loss_function='MAE',
                                      min_data_in_leaf=10,
                                      use_best_model=True)
            model.fit(
                x_train, y_train,
                eval_set=(x, y),
                early_stopping_rounds=500,
                verbose=0
            )
            y_predict = model.predict(
                df_input[feature]).flatten() / 5 + y_predict
        # 做强输出控制
        df_input["PREDICT"] = y_predict * \
            (df_input["Q90"] - df_input["Q10"]) + df_input["Q10"]
        df_input["PREDICT"] = np.maximum(df_input["PREDICT"], 0)
        df_input["TRUE"] = df_input["TRUE_STD"] * \
            (df_input["Q90"] - df_input["Q10"]) + df_input["Q10"]
        return df_input

    def run(self):
        df_input, df_view = self.get_data()
        df_input = self.get_holiday(df_input)
        df_input = self.predict(df_input)
        df_input["ADVICE"], df_input["总容纳量"], df_input["明细"] = zip(
            *df_input["NUMS"].apply(lambda x: get_car(x)))
        df_input = df_input[["LINE_NAME", "LINE_ID",
                             "DS", 'TRUE', 'PREDICT', 'ADVICE']]
        df_input.columns = ["LINE_NAME", "LINE_ID",
                            "POSTING_DATE", 'TRUE', 'PREDICT', 'ADVICE']
        df_input = df_input[df_input.POSTING_DATE > (
            self.today - timedelta(days=35))].reset_index(drop=True)
        Oracle().write_dataframe(key_col=[
            "LINE_NAME", "POSTING_DATE"], table_name="ZCB_PREDICT_GX_KS", df=df_input)
        return df_input, df_view
