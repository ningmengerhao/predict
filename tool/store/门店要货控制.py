# -*- coding: utf-8 -*-
from tool.dba import *
import datetime
import pandas as pd
import os

os.environ['NUMEXPR_MAX_THREADS'] = '16'
# 定义logger日志输出
logger = logging.getLogger(__name__)
# 设置输出格式
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


sql = """
SELECT PRODUCT_STORE_ID, PLUCODE , '*' as YH_PRODUCT_ID, '1' AS YH_MODE, '10063' AS UPDATE_USER, '4' AS UDP1, '4' AS UDP2
 from (
     SELECT ORGCODE PRODUCT_STORE_ID, PLUCODE FROM TRPTSALPLURPT
    WHERE RPTDATE>TRUNC(SYSDATE -31)
    GROUP BY ORGCODE, PLUCODE
                  )
                  """

df = oracle97.read(sql)
df['YH_MODE'] = '1'

for var in ['LAST_UPDATED_STAMP',  'LAST_UPDATED_TX_STAMP', 'CREATED_STAMP', 'CREATED_TX_STAMP']:
    df[var] = pd.to_datetime(datetime.datetime.now())
Oracle().write_dataframe(key_col=['PRODUCT_STORE_ID', 'PLUCODE'],
                         table_name='STORE_PRODUCT_LIST',
                         df=df)
