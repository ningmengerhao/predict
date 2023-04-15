from ..dba import *
from chinese_calendar import *
import os
import numpy as np
import logging
from catboost import CatBoostRegressor
from sklearn.model_selection import KFold

os.environ['NUMEXPR_MAX_THREADS'] = '16'
# 定义logger日志输出
logger = logging.getLogger(__name__)


class FactoryPredict:
    def __init__(self):
        self.predict_days = 3
        self.history_days = 35
        self.dir_path = "data/生产安排"
        self.date_col = 'DS'
        self.group_col = ['FACTORY_ID', "PRODUCT_ID"]
        self.target_col = "Y"
        self.summary_col = self.group_col + [self.date_col]
        self.plu = 'SELECT PRODUCT_ID FROM PLUVIEW WHERE PRODUCT_CATEGORY_NAME = \'散装食品\''
        self.org = "SELECT FACTORY_ID, ORGCODE STORE_ID FROM TZYORG_YH_PS_MODE"
        self.sql_holiday = """
        SELECT RPTDATE DS, FESTIVALS_NL, FESTIVALS_GL
        FROM ZCB_DAY_INFO 
        ORDER BY RPTDATE
        """
        self.unit = """
        SELECT PRODUCT_STORE_ID FACTORY_ID, PRODUCT_ID, PACK_WEGIHT YH_UNIT 
        FROM FACTORY_PRODUCT_ENABLE
        """

    def update_data(self, full=True):
        if full:
            date_range = pd.date_range(
                start='2019-01-01', end=datetime.datetime.now())
        else:
            from datetime import date, timedelta
            today = date.today()
            d = date(today.year, today.month, 1) - timedelta(1)
            date_range = pd.date_range(start=date(
                d.year, d.month, 1), end=datetime.datetime.now())
        df_date = pd.DataFrame(date_range, columns=["DS"])
        df_date["YEAR"] = df_date["DS"].dt.year
        df_date["MONTH"] = df_date["DS"].dt.month
        try:
            os.makedirs("data/生产安排")
        except FileExistsError:
            logger.info(f"Path exists: {self.dir_path} ")
        for (year, month), g_data in df_date.groupby(["YEAR", "MONTH"], as_index=False):
            start_date, end_date = g_data["DS"].min(), g_data["DS"].max()
            sql_ps = f"""
            SELECT ORGCODE STORE_ID, 
                    PLUCODE PRODUCT_ID, 
                    RPTDATE DS,
                    PLUCOUNT Y 
            FROM TZYPLUPRO_PS
            WHERE RPTDATE >= TO_DATE('{start_date}','yyyy-mm-dd hh24:mi:ss') AND RPTDATE <= TO_DATE('{end_date}','yyyy-mm-dd hh24:mi:ss')
            """
            df_ps = oracle97.read(sql_ps)
            df_ps.to_feather(os.path.join(
                self.dir_path, "%04d%02d.feather" % (year, month)))

    def get_data(self):
        df_plu = oracle97.read(self.plu)
        df_org = oracle97.read(self.org)
        df_input = []
        if len(os.listdir(self.dir_path)) < 7:
            self.update_data(full=True)
        for file in os.listdir(self.dir_path):
            df_part = pd.read_feather(os.path.join(self.dir_path, file))
            df_part = df_part.merge(df_org, how="left", left_on=[
                                    "STORE_ID"], right_on=["STORE_ID"])
            df_part = df_part[df_part["PRODUCT_ID"].isin(df_plu["PRODUCT_ID"])]
            df_part = df_part.groupby(self.summary_col, as_index=False).sum()
            df_input.append(df_part)
        df_input = pd.concat(df_input, ignore_index=True, sort=False)
        df_input = df_input[df_input.groupby(
            ["FACTORY_ID", "PRODUCT_ID"]).Y.transform("mean") > 100]
        df_input = df_input[df_input.groupby(
            ["FACTORY_ID", "PRODUCT_ID"]).Y.transform("std") > 1]
        df_input = df_input.sort_values(
            by=self.summary_col).reset_index(drop=True)
        df_group = df_input.groupby(self.group_col, sort=False)
        df_input['Q10'] = df_group[self.target_col].rolling(
            self.history_days, min_periods=21).quantile(0.1).values
        df_input['Q90'] = df_group[self.target_col].rolling(
            self.history_days, min_periods=21).quantile(0.9).values
        df_input['TRUE'] = df_group[self.target_col].shift(
            -self.predict_days).values
        df_input['TRUE_STD'] = (
            df_input['TRUE'] - df_input['Q10']) / (df_input['Q90'] - df_input['Q10'])
        df_input[self.date_col] = df_input[self.date_col] + \
            datetime.timedelta(days=self.predict_days)
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
            by=self.group_col + ["WEEK_ADJ", "DS"]).reset_index(drop=True)
        for day in [1, 2, 3, 4]:
            df_input["T%02d" % day] = df_input.groupby(
                self.group_col + ["WEEK_ADJ"])["TRUE_STD"].shift(day)
        df_input = df_input.sort_values(
            by=self.summary_col).reset_index(drop=True)
        return df_input

    def predict(self, df_input):
        feature = [x for x in df_input.columns if
                   x not in self.summary_col + ['Q10', 'Q90', 'TRUE', 'TRUE_STD', 'Y']]
        df_input = df_input.replace([np.inf, np.nan])
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
        self.update_data(full=False)
        df_input = self.get_data()
        df_input = self.get_holiday(df_input)
        df_input = self.predict(df_input)
        df_input = df_input[["FACTORY_ID", "PRODUCT_ID",
                             "DS", "TRUE", "PREDICT", "HOLIDAY_AFFECT"]]
        df_unit = oracle97.read(self.unit)
        df_yh = df_input.merge(df_unit, how="inner",
                               left_on=self.group_col, right_on=self.group_col)
        df_yh.columns = ["FACTORY_ID", "PLUCODE", "RPTDATE",
                         "TRUE", "PREDICT", "HOLIDAY_AFFECT", "YH_UNIT"]
        df_yh = df_yh[df_yh['RPTDATE'] >
                      df_yh['RPTDATE'].max() - datetime.timedelta(days=94)]
        df_yh["SUPPORT"] = df_yh['RPTDATE'] - pd.to_datetime("1899-12-30")
        df_yh["SUPPORT"] = df_yh["SUPPORT"].dt.days
        df_yh["SUPPORT"] = df_yh["SUPPORT"].astype(str)
        df_yh["SUPPORT"] = df_yh["SUPPORT"] + df_yh["PLUCODE"].astype(str)
        df_yh['PREDICT'] = np.ceil(df_yh['PREDICT'] / df_yh['YH_UNIT'])
        df_yh['TRUE'] = np.ceil(df_yh['TRUE'] / df_yh['YH_UNIT'])
        df_yh["ERROR"] = df_yh["PREDICT"].fillna(0) - df_yh['TRUE'].fillna(0)
        df_yh["ERROR"] = np.where(df_yh['TRUE'] > 10, df_yh["ERROR"], np.nan)
        df_yh["ABS_ERROR"] = np.abs(df_yh["ERROR"])
        df_yh["ERROR_PERCENTAGE"] = df_yh["ERROR"].values / np.where(df_yh['TRUE'].values == 0,
                                                                     df_yh['TRUE'].values + 1,
                                                                     df_yh['TRUE'].values)
        df_yh["ABS_ERROR_PERCENTAGE"] = np.abs(df_yh["ERROR_PERCENTAGE"])
        df_yh["SAFE_STOCK"] = np.abs(
            df_yh.groupby(['FACTORY_ID', 'PLUCODE', 'HOLIDAY_AFFECT'])["ERROR_PERCENTAGE"].transform('mean').fillna(
                0.05))
        df_yh["SAFE_STOCK"] = np.where(
            df_yh["SAFE_STOCK"] > 0.2, 0.2, df_yh["SAFE_STOCK"])
        df_yh["SAFE_STOCK"] = np.where(
            df_yh["SAFE_STOCK"] < 0.05, 0.05, df_yh["SAFE_STOCK"])
        df_yh["SAFE_STOCK"] = df_yh["PREDICT"] / df_yh["YH_UNIT"] * df_yh["HOLIDAY_AFFECT"] * df_yh[
            "SAFE_STOCK"]  # * df_yh['YH_STORE_NUM']
        df_yh["SAFE_STOCK"] = df_yh["SAFE_STOCK"].apply(lambda x: max(x, 1))
        df_yh["SAFE_STOCK"] = 0
        df_yh["ERROR1"] = df_yh["PREDICT"].fillna(
            0) + df_yh["SAFE_STOCK"] - df_yh['TRUE'].fillna(0)
        df_yh["ERROR1"] = np.where(df_yh['TRUE'] > 1, df_yh["ERROR1"], np.nan)
        df_yh["ABS_ERROR1"] = np.abs(df_yh["ERROR1"])
        df_yh["综合波动"] = df_yh["ERROR1"].values / np.where(df_yh['TRUE'].values == 0,
                                                          df_yh['TRUE'].values + 1,
                                                          df_yh['TRUE'].values)
        df_yh = df_yh.replace([np.inf, -np.inf], [0, 0])
        df_yh['EXEDATE'] = pd.to_datetime(
            datetime.datetime.today().strftime('%Y-%m-%d'))
        df_yh = df_yh[df_yh['RPTDATE'] >
                      df_yh['RPTDATE'].max() - datetime.timedelta(days=34)]
        df_label = []
        for g_name, g_data in df_yh[(df_yh["TRUE"] > 1)].groupby(["FACTORY_ID", "PLUCODE"]):
            error_percentage = g_data["综合波动"].values
            error_percentage = np.sort(error_percentage)
            error_percentage = error_percentage[3:-3]
            count_small = np.sum(error_percentage < -0.05) / len(g_data)
            count_large = np.sum(error_percentage > 0.13) / len(g_data)
            error_sum = (np.sum(g_data["PREDICT"] + g_data["SAFE_STOCK"]) - np.sum(g_data["TRUE"])) / np.where(
                np.sum(g_data["TRUE"]) > 0, np.sum(g_data["TRUE"]), 1)
            if (count_small < 0.15) & (count_large < 0.15) & (error_sum > 0) & (error_sum < 0.1):
                label = "A"
            elif (count_small < 0.3) & (count_large < 0.3) & (error_sum > 0.1) & (error_sum < 0.2):
                label = "B"
            else:
                label = "C"
            df_label.append([g_name[0], g_name[1], label])
        df_label = pd.DataFrame(
            df_label, columns=["FACTORY_ID", "PLUCODE", "LABEL"])
        df_yh = df_yh.merge(df_label, how="left", left_on=[
                            "FACTORY_ID", "PLUCODE"], right_on=["FACTORY_ID", "PLUCODE"])
        df_yh['LABEL'] = df_yh['LABEL'].fillna("空")
        df_yh = df_yh[["FACTORY_ID", "PLUCODE", "RPTDATE", "SUPPORT", "TRUE", "PREDICT", "HOLIDAY_AFFECT", "SAFE_STOCK",
                       "EXEDATE", "ERROR", "ABS_ERROR", "ERROR_PERCENTAGE", "ABS_ERROR_PERCENTAGE", "LABEL", "综合波动",
                       "YH_UNIT"]]
        df_factory_yh = df_yh.drop("综合波动", axis=1)
        Oracle().write_dataframe(key_col=["FACTORY_ID", "PLUCODE", "RPTDATE"],
                                 table_name="ZCB_PREDICT_FACTORY_YH",
                                 df=df_factory_yh,
                                 add_ts=False)
        return df_yh
