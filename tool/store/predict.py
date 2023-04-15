import sys
import logging
import numpy as np
import pandas as pd
from sklearn.model_selection import KFold
from catboost import CatBoostRegressor
from chinese_calendar import *
from datetime import timedelta
import datetime
import os
from ..dba import *



print(os.getcwd())

print('导入包路径：')
for i in sys.path:
    print(i)


pd.set_option('display.max_rows', 500)  # 打印最大行数
pd.set_option('display.max_columns', 500)  # 打印最大列数
logger = logging.getLogger(__name__)

Oracle = database.Oracle()
Hana = database.Hana()
Oracle97 = database.Oracle(
    user="zyngprd", password='zyngprd', dsn="10.10.201.97:1521/crmngpsd")


class StoreSalePredict:
    def __init__(self, ):
        self.predict_days = 3
        self.history_days = 35
        self.summary_col = ['STORE_ID', 'DS']
        self.target_col = 'Y'
        self.group_col = ['STORE_ID']
        self.date_col = 'DS'
        self.dis_col = ['DISCOUNT']
        self.sql_xs = """
        SELECT RPTDATE DS, ORGCODE STORE_ID, TRADETOTAL Y, (TRADETOTAL - DSCTOTAL) / TRADETOTAL DISCOUNT
        FROM TRPTSALORGRPT A -- INNER JOIN ORGVIEW B ON A.ORGCODE = B.PRODUCT_STORE_ID AND B.ORGTYPE='直营店' and B.GEO_NAME='武汉市'
        WHERE TRADETOTAL > 300
        """
        self.sql_holiday = """
        SELECT RPTDATE DS, FESTIVALS_NL, FESTIVALS_GL
        FROM ZCB_DAY_INFO 
        ORDER BY RPTDATE
        """

    def get_data(self):
        df_input = Oracle97.read(self.sql_xs)
        df_input = df_input.groupby(
            self.summary_col, as_index=False, sort=True).sum()
        df_group = df_input.groupby(self.group_col, sort=False)
        df_input['TRUE'] = df_group[self.target_col].shift(
            -self.predict_days).values
        df_input['Q10'] = df_group[self.target_col].rolling(
            self.history_days, min_periods=7).quantile(0.1).values
        df_input['Q90'] = df_group[self.target_col].rolling(
            self.history_days, min_periods=7).quantile(0.9).values
        df_input['TRUE_STD'] = (
            df_input['TRUE'] - df_input['Q10']) / (df_input['Q90'] - df_input['Q10'])
        df_input[self.dis_col] = df_group[self.dis_col].shift(
            -self.predict_days).values
        df_input[self.dis_col] = df_group[self.dis_col].fillna(
            method='ffill').fillna(method='bfill').values
        df_input[self.dis_col] = np.ceil(
            df_input[self.dis_col] * 20) / 20 * 100
        df_input[self.dis_col] = df_input[self.dis_col].fillna(100)
        df_input[self.date_col] = df_input[self.date_col] + \
            timedelta(days=self.predict_days)
        return df_input

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
        df_holiday["WEEK_ADJ"] = df_holiday['DS'].dt.month.astype(str) + df_holiday["HOLIDAY_T-1"] + df_holiday[
            "HOLIDAY"] + df_holiday["HOLIDAY_T1"]
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
            by=["STORE_ID", "WEEK_ADJ", "DS"]).reset_index(drop=True)
        for day in [1, 2, 3, 4]:
            df_input["T%02d" % day] = df_input.groupby(["STORE_ID", "WEEK_ADJ"])[
                "TRUE_STD"].shift(day)
        df_input = df_input.sort_values(
            by=["STORE_ID", "DS"]).reset_index(drop=True)
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
                                      min_data_in_leaf=100,
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

    def write(self, df_input):
        df_input = df_input.sort_values(
            by=self.summary_col).reset_index(drop=True)
        limit_low = df_input["DS"] >= datetime.datetime.today() - \
            timedelta(days=92)
        df_input = df_input[limit_low].reset_index(drop=True)
        df_input = df_input[['STORE_ID', 'DS', 'TRUE', 'PREDICT']]
        Oracle().write_dataframe(table_name="ZCB_PREDICT_STORE_XS",
                                 df=df_input, key_col=self.summary_col)
        return df_input

    def run(self):
        logger.info("开始执行销量预测任务")
        df_input = self.get_data()
        df_input = self.get_holiday(df_input)
        df_input = self.predict(df_input)
        df_input = self.write(df_input)
        return df_input


class DemandForecast:
    def __init__(self, ):
        self.oracle = Oracle()
        self.today = pd.to_datetime(
            datetime.datetime.today().strftime('%Y-%m-%d'))
        self.sql_xs = """
        SELECT *
        FROM ZCB_PREDICT_STORE_XS
        WHERE DS > TRUNC(SYSDATE - 31)
        """
        self.sql_rate = """
        SELECT A.ORGCODE STORE_ID,
               A.PLUCODE YH_PRODUCT_ID,
               SUM(A.PLUCOUNT) 消耗量
        FROM (
                 SELECT PS.ORGCODE, PS.PLUCODE, PLUCOUNT
                 FROM TZYPLUPRO_PS PS
                 WHERE PS.RPTDATE > TRUNC(SYSDATE - 31) and PS.RPTDATE <= TRUNC(SYSDATE - 1)
                   AND YJTOTAL > 0
                 UNION ALL
                SELECT DI.PRODUCT_STORE_ID_TO AS ORGCODE,
                       DI.PRODUCT_ID          AS PLUCODE,
                       SUM(DI.QUANTITY)       AS PLUCOUNT
                FROM ZY_DB_ITEM DI
                WHERE 1 = 1
                  AND DI.POSTING_DATE > TRUNC(SYSDATE - 31) and DI.POSTING_DATE <= TRUNC(SYSDATE - 1) AND DI.AMOUNT2 > 0
                GROUP BY DI.PRODUCT_STORE_ID_TO,
                         DI.PRODUCT_ID
                UNION ALL
                SELECT DI.PRODUCT_STORE_ID    AS DCORGCODE,
                         DI.PRODUCT_ID          AS PLUCODE,
                       -1 *SUM(DI.QUANTITY)   AS PLUCOUNT
                FROM ZY_DB_ITEM DI
                WHERE 1 = 1
                  AND DI.POSTING_DATE > TRUNC(SYSDATE - 31) and DI.POSTING_DATE <= TRUNC(SYSDATE - 1) AND DI.AMOUNT2 > 0
                GROUP BY DI.PRODUCT_STORE_ID,
                         DI.PRODUCT_ID
                 UNION ALL
                 SELECT TH.ORGCODE, TH.PLUCODE, PLUCOUNT * -1 PLUCOUNT
                 FROM TZYPLUPRO_TH TH
                 WHERE TH.DZ_DATE > TRUNC(SYSDATE - 31) and TH.DZ_DATE <= TRUNC(SYSDATE - 1)
                   AND YJTOTAL > 0
                 UNION ALL
                 SELECT PD.PRODUCT_STORE_ID AS ORGCODE,
                        PD.PRODUCT_ID AS PLUCODE, 
                        SUM(PD.QUANTITY) AS PLUCOUNT  
                 FROM ZY_PD_ITEM PD 
                 WHERE PD.POSTING_DATE = TRUNC(SYSDATE - 31)  and PD.AMOUNT > 0
                 GROUP BY PD.PRODUCT_STORE_ID, PD.PRODUCT_ID
                 UNION ALL
                 SELECT PD.PRODUCT_STORE_ID AS ORGCODE,
                        PD.PRODUCT_ID AS PLUCODE, 
                        SUM(PD.QUANTITY) AS PLUCOUNT  
                 FROM ZY_PD_ITEM PD 
                 WHERE PD.POSTING_DATE = TRUNC(SYSDATE - 1)  and PD.AMOUNT > 0
                 GROUP BY PD.PRODUCT_STORE_ID, PD.PRODUCT_ID
             ) A INNER JOIN (
              SELECT PS.ORGCODE, PS.PLUCODE, COUNT(*)
              FROM TZYPLUPRO_PS PS
              WHERE PS.RPTDATE > TRUNC(SYSDATE - 31) and PS.RPTDATE <= TRUNC(SYSDATE - 1)
               AND YJTOTAL > 0
              GROUP BY PS.ORGCODE, PS.PLUCODE
              HAVING COUNT(*) >= 20
             ) B ON A.ORGCODE = B.ORGCODE AND A.PLUCODE = B.PLUCODE
        GROUP BY A.ORGCODE, A.PLUCODE, A.PLUCODE
        """
        self.sql_base_data = """
        SELECT PD.PRODUCT_STORE_ID AS STORE_ID,
               PD.PRODUCT_ID AS YH_PRODUCT_ID, 
               PD.POSTING_DATE DS,
               SUM(PD.QUANTITY) AS 今日库存  
        FROM ZY_PD_ITEM PD 
        WHERE PD.POSTING_DATE > TRUNC(SYSDATE - 7)  and PD.AMOUNT > 0
        GROUP BY PD.PRODUCT_STORE_ID, PD.PRODUCT_ID, PD.POSTING_DATE;

        SELECT PD.PRODUCT_STORE_ID AS STORE_ID,
               PD.PRODUCT_ID AS YH_PRODUCT_ID, 
               PD.POSTING_DATE + 1 AS DS,
               SUM(PD.QUANTITY) AS 昨日库存  
        FROM ZY_PD_ITEM PD 
        WHERE PD.POSTING_DATE > TRUNC(SYSDATE - 8)  and PD.AMOUNT > 0
        GROUP BY PD.PRODUCT_STORE_ID, PD.PRODUCT_ID, PD.POSTING_DATE;


        SELECT ORGCODE STORE_ID, PLUCODE YH_PRODUCT_ID, RPTDATE DS, YH_UNIT, YH_UNIT*PLUCOUNT 今日要货
        FROM TZYPLUPRO_YH A
        WHERE A.RPTDATE > TRUNC(SYSDATE - 7);

        SELECT ORGCODE STORE_ID, PLUCODE YH_PRODUCT_ID, RPTDATE DS, PLUCOUNT 今日配送
        FROM (
        SELECT ORGCODE, PLUCODE , RPTDATE , PSCOUNT + DRCOUNT - DCCOUNT - THCOUNT PLUCOUNT
        FROM  TRPTSALPLUKC_RT
        WHERE RPTDATE = TRUNC(SYSDATE)
        UNION ALL
        SELECT  ORGCODE, PLUCODE , RPTDATE , PLUCOUNT
        FROM TZYPLUPRO_PS
        WHERE RPTDATE > TRUNC(SYSDATE - 7) AND RPTDATE < TRUNC(SYSDATE)
        UNION ALL
        SELECT  ORGCODE, PLUCODE , RPTDATE , PLUCOUNT
        FROM TZYPLUPRO_PS_RT
        WHERE RPTDATE > TRUNC(SYSDATE)
            ) A;


        SELECT ORGCODE STORE_ID, PLUCODE YH_PRODUCT_ID, RPTDATE + 1 DS,YH_UNIT*PLUCOUNT 昨日要货
        FROM TZYPLUPRO_YH A
        WHERE A.RPTDATE > TRUNC(SYSDATE - 8);

        SELECT ORGCODE STORE_ID, PLUCODE YH_PRODUCT_ID, RPTDATE + 1 DS, PLUCOUNT 昨日配送
        FROM (
        SELECT ORGCODE, PLUCODE , RPTDATE , PSCOUNT + DRCOUNT - DCCOUNT - THCOUNT PLUCOUNT
        FROM  TRPTSALPLUKC_RT
        WHERE RPTDATE = TRUNC(SYSDATE)
        UNION ALL
        SELECT  ORGCODE, PLUCODE , RPTDATE , PLUCOUNT
        FROM TZYPLUPRO_PS
        WHERE RPTDATE > TRUNC(SYSDATE - 8) AND RPTDATE < TRUNC(SYSDATE)
        UNION ALL
        SELECT  ORGCODE, PLUCODE , RPTDATE , PLUCOUNT
        FROM TZYPLUPRO_PS_RT
        WHERE RPTDATE > TRUNC(SYSDATE)
            ) A
            """
        self.sql_plu = """
        SELECT PRODUCT_ID YH_PRODUCT_ID,
               CASE WHEN PARENT_PRODUCT_ID IS NOT NULL THEN PARENT_PRODUCT_ID
               ELSE PRODUCT_ID END PLUCODE  
        FROM PLUVIEW
           """

    def run(self):
        df_store_xs = Oracle.read(self.sql_xs)
        df_plu_his = Oracle97.read(self.sql_rate)
        df_plu_his = df_plu_his.merge(df_store_xs.groupby("STORE_ID")["TRUE"].sum(),
                                      left_on=["STORE_ID"], right_on=["STORE_ID"])
        df_plu_his["用量/元"] = df_plu_his["消耗量"] / \
            np.maximum(df_plu_his["TRUE"], 1)
        df_plu_his = df_plu_his.drop(["消耗量", "TRUE"], axis=1)
        df_store_yy = df_store_xs.merge(
            df_plu_his, left_on=["STORE_ID"], right_on=["STORE_ID"])
        df_full = [oracle97.read(x) for x in self.sql_base_data.split(
            ";") if "SELECT" in x]
        for df in df_full:
            df = df.groupby(["STORE_ID", "YH_PRODUCT_ID",
                            "DS"], as_index=False).sum()
            df_store_yy = df_store_yy.merge(df,
                                            how="left",
                                            left_on=["STORE_ID",
                                                     "YH_PRODUCT_ID", "DS"],
                                            right_on=["STORE_ID", "YH_PRODUCT_ID", "DS"])
            var = df.columns[-1]
            if var != "YH_UNIT":
                df_store_yy[var] = df_store_yy[var].fillna(0)
        df_store_yy = df_store_yy.sort_values(
            by=["STORE_ID", "YH_PRODUCT_ID", "DS"]).reset_index(drop=True)
        df_store_yy['YH_UNIT'] = df_store_yy.groupby(["STORE_ID", "YH_PRODUCT_ID"])['YH_UNIT'].fillna(
            method='ffill').fillna(method='bfill').values
        df_store_yy["YH_UNIT"] = np.where(
            df_store_yy["YH_UNIT"] > 0, df_store_yy["YH_UNIT"], 1)
        today = pd.to_datetime(datetime.datetime.today().strftime('%Y-%m-%d'))
        df_store_yy["今日最大配送重量"] = df_store_yy.groupby(
            ["STORE_ID", "DS"]).今日配送.transform("max")
        df_store_yy["安全库存"] = df_store_yy["用量/元"] * \
            df_store_yy["PREDICT"] * 0.2
        df_store_yy["安全库存"] = np.minimum(
            df_store_yy["安全库存"], df_store_yy["YH_UNIT"] * 2.5)
        df_store_yy["安全库存"] = np.minimum(df_store_yy["安全库存"], 5)
        df_store_yy["安全库存"] = np.maximum(
            df_store_yy["安全库存"], df_store_yy["YH_UNIT"] * 0.5)
        for i in range(3):
            pd_choose = (df_store_yy["昨日库存"] == 0) | (
                df_store_yy["昨日库存"].isnull())
            date_choose = df_store_yy["DS"] == today + timedelta(days=i)
            df_store_yy["昨日库存"] = np.where(date_choose & pd_choose,
                                           df_store_yy.groupby(
                                               ["STORE_ID", "YH_PRODUCT_ID"]).今日库存.shift(1),
                                           df_store_yy["昨日库存"])
            df_store_yy["昨日库存"] = np.maximum(df_store_yy["昨日库存"], 0)
            df_store_yy["建议要货重量"] = df_store_yy["用量/元"] * df_store_yy["PREDICT"] + df_store_yy["安全库存"] - df_store_yy[
                "昨日库存"]
            df_store_yy["建议要货重量"] = np.where(
                df_store_yy["建议要货重量"] > 0, df_store_yy["建议要货重量"], 0)
            df_store_yy["建议要货数量"] = np.round(
                df_store_yy["建议要货重量"] / df_store_yy["YH_UNIT"])
            df_store_yy["建议要货重量"] = df_store_yy["YH_UNIT"] * \
                df_store_yy["建议要货数量"]
            yh_choose = (df_store_yy["今日要货"] == 0) | (
                df_store_yy["今日要货"].isnull())
            date_choose = df_store_yy["DS"] == today + timedelta(days=i)
            df_store_yy["今日要货"] = np.where(
                yh_choose & date_choose, df_store_yy["建议要货重量"], df_store_yy["今日要货"])
            pd_choose = (df_store_yy["今日库存"] == 0) | (
                df_store_yy["今日库存"].isnull())
            date_choose = df_store_yy["DS"] == today + timedelta(days=i)
            df_store_yy["今日可销售重量"] = np.where(df_store_yy["今日最大配送重量"] > 0,
                                              df_store_yy['昨日库存'] +
                                              df_store_yy["今日配送"],
                                              df_store_yy['昨日库存'] + df_store_yy["今日要货"])
            df_store_yy["今日库存"] = np.where(pd_choose & date_choose,
                                           df_store_yy["今日可销售重量"] - df_store_yy["用量/元"] * df_store_yy["PREDICT"] -
                                           df_store_yy["安全库存"],
                                           df_store_yy["今日库存"])
            df_store_yy["今日库存"] = np.maximum(df_store_yy["今日库存"], 0)
        df_store_yy = df_store_yy.sort_values(
            by=["DS", "YH_PRODUCT_ID"]).reset_index(drop=True)
        df_store_yy["实际要货数量"] = df_store_yy["今日要货"] / df_store_yy["YH_UNIT"]
        df_store_yy['XSCOUNT_PREDICT'] = df_store_yy["用量/元"] * \
            df_store_yy["PREDICT"]
        df_plu = oracle97.read(self.sql_plu)
        df_store_yy = df_store_yy.merge(df_plu, how="left", left_on=[
                                        "YH_PRODUCT_ID"], right_on=["YH_PRODUCT_ID"])
        df_store_yy = df_store_yy[
            ['DS', '安全库存', 'STORE_ID', 'PLUCODE', "用量/元", 'XSCOUNT_PREDICT', 'YH_PRODUCT_ID',
             'YH_UNIT', '建议要货重量', '建议要货数量']]
        df_store_yy.columns = ['RPTDATE', 'SAFETY_STOCK_RATE', 'ORGCODE', 'PLUCODE', "RATE", 'XSCOUNT_PREDICT',
                               'YH_PRODUCT_ID', 'YH_UNIT', 'YHCOUNT_PREDICT', 'YH_COUNT']
        control_num = np.where(df_store_yy["YH_UNIT"] < 1, 1, 2)
        adjust_num = df_store_yy["YH_COUNT"] * 0.1
        change_num = np.where(control_num > adjust_num,
                              control_num, adjust_num)
        df_store_yy["MAX_YH_COUNT"] = np.maximum(df_store_yy["YH_COUNT"] + change_num,
                                                 np.round(df_store_yy["YH_COUNT"] + change_num))
        df_store_yy["MAX_YH_COUNT"] = np.maximum(
            df_store_yy["MAX_YH_COUNT"], 2)
        df_store_yy["MAX_YH_COUNT"] = np.minimum(
            df_store_yy["MAX_YH_COUNT"], 999)
        df_store_yy["MIN_YH_COUNT"] = np.minimum(df_store_yy["YH_COUNT"] - change_num,
                                                 np.round(df_store_yy["YH_COUNT"] - change_num))
        df_store_yy["MIN_YH_COUNT"] = np.maximum(
            df_store_yy["MIN_YH_COUNT"], 0)
        df_store_yy["CREATE_TIME"] = pd.to_datetime(
            datetime.datetime.now(), format='%Y-%m-%d %T')
        df_store_yy['EXEDATE'] = df_store_yy["CREATE_TIME"].dt.date
        select_row = df_store_yy["RPTDATE"] >= pd.to_datetime(
            datetime.datetime.now() - timedelta(days=1))
        df_store_yy = df_store_yy[select_row].reset_index(drop=True)
        self.oracle.write_dataframe(df=df_store_yy,
                                    key_col=["RPTDATE", "ORGCODE",
                                             "PLUCODE", "YH_PRODUCT_ID"],
                                    table_name="ZCB_PREDICT_STORE_YY",
                                    add_ts=True)
        return df_store_yy


# df_input = StoreSalePredict().run()
df_store_yy = DemandForecast().run()
