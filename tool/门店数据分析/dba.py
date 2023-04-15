import codecs
import datetime
import logging
import math
from multiprocessing.spawn import import_main_path
import cx_Oracle
import numpy as np
import pandas as pd
import pyhdb
import pyhdb.cesu8
from soupsieve import select
from tqdm import tqdm
import string


codecs.register(lambda s: (
    pyhdb.cesu8.CESU8_CODEC_INFO
    if s in {'cesu-8', 'cesu_8'}
    else None
))
logger = logging.getLogger(__name__)

base = {'datetime64[ns]': 'DATE',
        'object': 'VARCHAR2(999)',
        'float64': 'NUMBER(19, 3)',
        'int64': 'NUMBER(19, 3)'}


class Oracle:
    def __init__(self, user="zyngprd", password='zyngprd', dsn="10.10.201.70:1521/xsycdb"):
        self.conn = cx_Oracle.connect(user=user, password=password, dsn=dsn,
                                      encoding="UTF-8", nencoding="UTF-8")
        self.cursor = self.conn.cursor()
        self.base = {'datetime64[ns]': 'DATE',
                     'object': 'VARCHAR2(50)', 'float': 'NUMBER(5, 2)'}
        self.info = self.read("SELECT * FROM all_tab_comments")

    def read(self, sql):
        logger.info("Read data with sql")
        logger.info(sql)
        self.cursor.execute(sql)
        var_name = [var[0] for var in self.cursor.description]
        df = self.cursor.fetchall()
        df = pd.DataFrame(list(df), columns=var_name)
        logger.info("finish read total {} rows".format(len(df)))
        return df

    def write_many(self, sql, data):
        # logger.info(sql, data)
        self.cursor.executemany(sql, data)
        self.conn.commit()

    def write_single(self, sql, data):
        self.cursor.execute(sql, data)
        self.conn.commit()

    def execute(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    def create_table(self, df, table_name, key_col):
        logger.info("Create data with sql")
        sql_part = "\n".join(
            ["{}\t\t{},".format(var_name, base[str(var_type)]) for var_name, var_type in df.dtypes.items()])
        sql_create = """
        create table {} (
            {}
            constraint {}{}_pk primary key ({})
        )
        """.format(table_name, sql_part, np.random.choice(list(string.ascii_letters), 1)[0].upper(),
                   datetime.datetime.now().strftime('%Y%m%d%H%M%S'), ",".join(key_col))
        logger.info(sql_create)
        if table_name in self.info.TABLE_NAME.tolist():
            self.execute(sql="drop table {}".format(table_name))
        self.execute(sql_create)
        self.info = self.read("SELECT * FROM all_tab_comments")
        logger.info("create table finish")

    def delete_data(self, table_name, key_col, df):
        logger.info("Delete data with sql")
        df = df[key_col].drop_duplicates().reset_index(drop=True)
        self.create_table(table_name="D{}".format(
            table_name), df=df, key_col=key_col)
        self.insert(table_name="D{}".format(
            table_name), df=df, key_col=key_col)
        sql_part = "AND".join([' A.{} = B.{} '.format(var, var)
                              for var in key_col])
        logger.info("Delete data with sql")
        sql_delete = """
            DELETE FROM {} A
            where exists (
                select 1 from D{} B where {})
            """.format(table_name, table_name, sql_part)
        logger.info(sql_delete)
        self.execute(sql_delete)
        logger.info("Delete data finish")
        self.execute(sql="drop table D{}".format(table_name))

    def insert(self, key_col, table_name, df):
        logger.info("insert data with sql")
        df = df.drop_duplicates(subset=key_col).dropna(
            subset=key_col).reset_index(drop=True)
        df = df.replace([np.inf, -np.inf], [0, 0])
        sql_columns = ", ".join(df.columns)
        sql_fixed = ", ".join([":%d" % (i + 1)
                              for i in range(len(df.columns))])
        sql_write = "INSERT INTO %s(%s) VALUES (%s)" % (
            table_name, sql_columns, sql_fixed)
        logger.info(sql_write)
        batch_size = 10000
        for start_index in tqdm(range(0, len(df), batch_size)):
            end_index = start_index + batch_size
            end_index = min(end_index, len(df))
            if df.shape[1] == 1:
                data_insert = []
                for x in df.iloc[:, 0].to_list():
                    data_insert.append([x])
            else:
                data_insert = df.iloc[start_index:end_index].values.tolist()
                for b in data_insert:
                    for index, value in enumerate(b):
                        if isinstance(value, float) and math.isnan(value):
                            b[index] = None
                        elif isinstance(value, type(pd.NaT)):
                            b[index] = None
            self.write_many(sql_write, data_insert)
        logger.info("insert finish")

    def write_dataframe(self, key_col, table_name, df, add_ts=False):
        if add_ts:
            df['TS'] = (datetime.datetime.now()).strftime('%Y%m%d%H%M%S')
        if table_name not in self.info.TABLE_NAME.tolist():
            self.create_table(table_name=table_name, df=df, key_col=key_col)
        self.delete_data(table_name=table_name, key_col=key_col, df=df)
        self.insert(table_name=table_name, key_col=key_col, df=df)

    def __del__(self):
        self.cursor.close()
        self.conn.close()


# jdbc:sap://10.10.201.37:36015 SAPS4P S4Ppas@135
class Hana:
    def __init__(self,
                 host="10.10.201.37",
                 port=3601  5,
                 user="SAPS4P",
                 password="S4Ppas@135"):
        self.conn = pyhdb.connect(
            host=host, port=port, user=user, password=password)
        self.cursor = self.conn.cursor()

    def read(self, sql):
        logger.info("Read data with sql")
        logger.info(sql)
        self.cursor.execute(sql)
        var_name = [var[0] for var in self.cursor.description]
        df = self.cursor.fetchall()
        df = pd.DataFrame(list(df), columns=var_name)
        logger.info("finish read total {} rows".format(len(df)))
        return df

    def execute(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    def __del__(self):
        self.cursor.close()
        self.conn.close()


# oracle97 = Oracle(user="zyngprd", password='zyngprd',
#                   dsn="10.10.201.97:1521/crmngpsd")
# oracle155 = Oracle()
# hana = Hana()

# oracle_155 = Oracle(user="zyngprd", password='zyngprd', dsn="10.10.201.155:1521/xsycdb")
# df = oracle_155.read("SELECT * FROM CARVIEW")
# Oracle().write_dataframe(df=df, key_col=["CAR_NAME"], table_name="CARVIEW")
#
# oracle_155 = Oracle(user="zyngprd", password='zyngprd', dsn="10.10.201.155:1521/xsycdb")
# df = oracle_155.read("SELECT * FROM ZCB_DAY_INFO")
# Oracle().write_dataframe(df=df, key_col=["RPTDATE"], table_name="ZCB_DAY_INFO")
