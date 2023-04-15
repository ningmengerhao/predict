import datetime
import os
from tool.dba import oracle97
import numpy as np
import pandas as pd
import streamlit as st
import string


def spit_dataframe(df_src):
    n_sample = len(df_src)
    n_half = int(np.ceil(n_sample * 0.5))
    df_p1 = df_src.iloc[:n_half, :].reset_index(drop=True)
    df_p2 = df_src.iloc[n_half:, :].reset_index(drop=True)
    df_col_nan = pd.DataFrame([np.nan] * len(df_p1), columns=[''])
    df_out = pd.concat([df_p1, df_col_nan, df_p2], axis=1)
    df_out.columns = ['A', 'B', 'C', 'D', 'E']
    return df_out


def calculate_length(value):
    length = len(value)
    utf8_length = len(value.encode('utf-8'))
    return (utf8_length - length) / 2 + length + 2


def get_windows_sum_file(df_use, option):
    df_label = pd.read_excel("产品类型.xlsx", sheet_name=option)
    df_label = pd.melt(df_label, value_name="产品名称", var_name="产品类型").dropna()
    df_use = df_use.groupby(by=["窗口名称", "产品编号", "产品名称"], as_index=False, sort=True)[
        "要货数量"].sum()
    df_use = df_use.merge(df_label, how="left", left_on=[
                          "产品名称"], right_on=["产品名称"])
    df_prints = {}
    for g_name, g_data in df_use.groupby("窗口名称"):
        df_print = []
        for product_type in ['A', 'B']:
            df_select = g_data[g_data["产品类型"] == product_type][[
                '产品名称', '要货数量']].reset_index(drop=True)
            df_print.append(spit_dataframe(df_select))
            df_row_nan = pd.DataFrame(
                np.array([''] * 5).reshape(-1, 5), columns=['A', 'B', 'C', 'D', 'E'])
            df_print.append(df_row_nan)
        df_print = pd.concat(df_print, ignore_index=True, sort=False)
        df_prints.update({g_name: df_print})

    writer = pd.ExcelWriter(
        f"data/市配/{option}窗口汇总{time_str}.xlsx", engine='xlsxwriter')
    formats = writer.book.add_format({'border': 1, "font_size": 14})
    formats.set_align('center')
    formats.set_align('vcenter')
    for sheet_name, df in df_prints.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        worksheet.merge_range(0, 0, 0, 4, sheet_name)
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max((
                series.astype(str).map(calculate_length).max() + 1,
                len(str(series.name)) + 1
            )) * 14 / 12
            worksheet.set_column(idx, idx, max_len, formats)
        worksheet.conditional_format(
            f'A1:E{len(df) + 1}', {'type': 'no_blanks'})
    writer.save()
    writer.close()
    with open(f"data/市配/{option}窗口汇总{time_str}.xlsx", "rb") as file:
        btn = st.download_button(
            key=f"{option}窗口汇总{time_str}",
            label=f"{option}窗口汇总",
            data=file,
            file_name=f"{option}窗口汇总{time_str}.xlsx"
        )
    return df_prints


def get_store_file(df_use, option):
    df_use = pd.pivot_table(df_use, index=['窗口名称', '编号', '门店编号', '门店名称'], columns=[
                            '产品编号', '产品名称'], values="要货数量")
    df_prints = {}
    for g_name, g_data in df_use.groupby("窗口名称"):
        df_print = g_data.copy()
        df_print = df_print.reset_index()
        df_print = df_print.drop(['窗口名称'], axis=1)
        df_print["编号"] = range(1, len(df_print) + 1)
        row_1 = [x[1] for x in df_print.columns]
        row_2 = [x[0] for x in df_print.columns]
        var_names = list(string.ascii_letters.upper())[:len(row_1)]
        df_print.columns = var_names
        df_row = pd.DataFrame(np.array([row_1, row_2]), columns=var_names)
        df_sum = pd.DataFrame(
            np.array(["", "要货店数:", len(g_data)] +
                     g_data.sum(axis=0).astype(int).tolist()).reshape(1, -1),
            columns=var_names)
        df_print = pd.concat([df_row, df_print, df_sum],
                             ignore_index=True, sort=False)
        df_prints.update({g_name: df_print})
    writer = pd.ExcelWriter(
        f"data/市配/{option}单店鸡鸭鹅{time_str}.xlsx", engine='xlsxwriter')
    formats = writer.book.add_format({'border': 1, "font_size": 12})
    formats.set_align('center')
    formats.set_align('vcenter')
    for sheet_name, df in df_prints.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        worksheet.merge_range(0, 0, 0, df.shape[1] - 1, sheet_name)
        worksheet.merge_range(1, 0, 2, 0, "排序")
        worksheet.merge_range(1, 1, 2, 1, "门店编号")
        worksheet.merge_range(1, 2, 2, 2, "门店名称")
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max((
                series.astype(str).map(calculate_length).max() + 1,
                len(str(series.name)) + 1
            ))
            worksheet.set_column(idx, idx, max_len, formats)
        worksheet.conditional_format(
            f'A1:G{len(df) + 1}', {'type': 'no_blanks'})
    writer.save()
    writer.close()
    with open(f"data/市配/{option}单店鸡鸭鹅{time_str}.xlsx", "rb") as file:
        btn = st.download_button(
            key=f"{option}单店鸡鸭鹅{time_str}",
            label=f"{option}单店鸡鸭鹅",
            data=file,
            file_name=f"{option}单店鸡鸭鹅{time_str}.xlsx"
        )
    return None


def get_rice_file(df_use, option):
    df_use = df_use[df_use["delta"] == 2]
    st.write("米饭要货日期", df_use["要货日期"].min())
    df_use = pd.pivot_table(df_use,
                            index=['窗口名称', '编号', '门店编号', '门店名称'],
                            columns=['产品编号', '产品名称'],
                            values="要货数量",
                            fill_value=0)
    df_prints = {}
    for g_name, g_data in df_use.groupby("窗口名称"):
        df_print = g_data.copy()
        df_print = df_print.reset_index()
        df_print = df_print.drop(['窗口名称'], axis=1)
        df_print["编号"] = range(1, len(df_print) + 1)
        row_1 = [x[1] for x in df_print.columns]
        row_2 = [x[0] for x in df_print.columns]
        df_print.columns = ['A', 'B', 'C', 'D']
        df_row = pd.DataFrame(np.array([row_1, row_2]), columns=[
                              'A', 'B', 'C', 'D'])
        df_print = pd.concat([df_row, df_print])
        df_prints.update({g_name: df_print})
    writer = pd.ExcelWriter(
        f"data/市配/{option}米饭{time_str}.xlsx", engine='xlsxwriter')
    formats = writer.book.add_format({'border': 1, "font_size": 12})
    formats.set_align('center')
    formats.set_align('vcenter')
    for sheet_name, df in df_prints.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        worksheet.merge_range(0, 0, 0, 3, sheet_name)
        worksheet.merge_range(1, 0, 2, 0, "排序")
        worksheet.merge_range(1, 1, 2, 1, "门店编号")
        worksheet.merge_range(1, 2, 2, 2, "门店名称")
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max((
                series.astype(str).map(calculate_length).max() + 1,
                len(str(series.name)) + 1
            ))
            worksheet.set_column(idx, idx, max_len, formats)
        worksheet.conditional_format(
            f'A1:F{len(df) + 1}', {'type': 'no_blanks'})
    writer.save()
    writer.close()
    with open(f"data/市配/{option}米饭{time_str}.xlsx", "rb") as file:
        btn = st.download_button(
            key=f"{option}米饭{time_str}",
            label=f"{option}米饭",
            data=file,
            file_name=f"{option}米饭{time_str}.xlsx"
        )
    return None


# @st.cache(persist=True,  ttl=50*6)
def read_data(time_str):
    df_ps = oracle97.read("""
        SELECT TRUNC(DOC_DATE)        AS 要货日期,
               TRUNC(RS.POSTING_DATE) AS 业务日期,
               DPH.DESCRIPTION        AS 窗口名称,
               RSI.PRODUCT_ID         AS 产品编号,
               PLU.PRODUCT_NAME       AS 产品名称,
               DPI.SEQUENCE_NUM       AS 编号,
               RS.PRODUCT_STORE_ID    AS 门店编号,
               ORG.STORE_NAME         AS 门店名称,
               RSI.QUANTITY      AS 要货数量
        FROM REPLENISHMENT RS 
                 INNER JOIN REPLENISHMENT_ITEM RSI
                            ON RS.DOC_ID = RSI.DOC_ID AND RS.POSTING_DATE >= TRUNC(SYSDATE + 1) 
                            AND DOC_DATE >= TRUNC(SYSDATE - 2) AND RS.POSTING_DATE <= TRUNC(SYSDATE + 2) 
                 LEFT JOIN PRODUCT_STORE ORG ON ORG.PRODUCT_STORE_ID = RS.PRODUCT_STORE_ID
                 LEFT JOIN PRODUCT PLU ON RSI.PRODUCT_ID = PLU.PRODUCT_ID
                 LEFT JOIN DELIVERY_PLAN_ITEM DPI ON DPI.PRODUCT_STORE_ID = RS.PRODUCT_STORE_ID
                 LEFT JOIN DELIVERY_PLAN_HEADER DPH ON DPH.DOC_ID = DPI.DOC_ID
        WHERE 1 = 1
          AND RS.DOC_STATUS NOT IN ('4', '6')
           AND REPLENISH_STORE_ID = '7201' 
           AND DPH.DESCRIPTION is not null
           AND TRUNC(DOC_DATE) != TRUNC(RS.POSTING_DATE)
                 """)
    df_ps = df_ps.groupby(['要货日期', '业务日期', '窗口名称', '产品编号',
                          '产品名称', '编号', '门店编号', '门店名称'], as_index=False).sum()
    return df_ps


def app():
    try:
        os.makedirs("data/市配")
    except:
        d = 1
    df_ps = read_data(time_str=time_str)

    st.write("每次下载需刷新网页\n如遇到问题请电话联系吴佳霖18916220247")
    configs = {"武汉": {"delta": 1,
                      "窗口名称": ['武汉', '成品'],
                      "单品": ['10001', '10002', '10224'],
                      "米饭": ['12235']},
               "深圳": {"delta": 2,
                      "窗口名称": ['深圳配送', '方案'],
                      "单品": ['10001', '10002', '10005', '10224'],
                      "米饭": ['12235']},
               "江西": {"delta": 1,
                      "窗口名称": ['江西', '配送方案'],
                      "单品": ['10001', '10002', '10224'],
                      "米饭": ['12235']},
               }
    t1, t2, t3 = st.columns(3)
    with t1:
        option = st.radio(options=["武汉", "深圳", "江西"], label="地区")
    df_use = df_ps.copy()
    df_use["delta"] = (df_use["业务日期"] - df_use["要货日期"]).dt.days
    df_use["要货日期"] = df_use["要货日期"].dt.date
    df_use["业务日期"] = df_use["业务日期"].dt.date
    with t2:
        date = st.radio(options=df_use["业务日期"].unique(), label="业务日期")
    for windows in configs[option]["窗口名称"]:
        df_use = df_use[df_use["窗口名称"].str.contains(
            windows)].reset_index(drop=True)
    df_use = df_use[df_use["业务日期"] == date]
    st.write(f"{option}数据来源业务日期", date)
    get_windows_sum_file(
        df_use=df_use[df_use["delta"] == configs[option]["delta"]], option=option)
    get_store_file(df_use[df_use["产品编号"].isin(
        configs[option]["单品"])], option=option)
    get_rice_file(df_use[df_use["产品编号"].isin(
        configs[option]["米饭"])], option=option)
    return None


now = datetime.datetime.now()
time_str = now.strftime("%Y%m%d%H%M")
st.set_page_config(
    page_title="武汉工厂市配出货单",
    page_icon="🧊",
)
app()
