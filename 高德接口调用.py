# %%
import tool.dba as database
import hmac
import hashlib
import pandas as pd
import json
import jsonpath
import requests

#  %%
# 精准查找
# params = {
#     'key': 'f9073085d13f659b039bebce663230c8',
#     'address': '紫燕百味鸡(金沙井店)'
# }


# response = requests.get(
#     url='https://restapi.amap.com/v3/geocode/geo', params=params)

# content_json = json.loads(response.text)

params = {
    'key': 'f9073085d13f659b039bebce663230c8',
    'keywords': '肖四女',
    'region': '上海市',
    'page_size': 25
}

response = requests.get(
    url='https://restapi.amap.com/v5/place/text', params=params)
content_json = json.loads(response.text)


# params = {
#     'key': 'f9073085d13f659b039bebce663230c8',
#     'location': '116.481488,39.990464',
#     'poitype': '紫燕百味鸡'
#     'batch': 'true'
# }

# response = requests.get(
#     "https://restapi.amap.com/v3/geocode/regeo", params=params)

# %%
# 高德搜索POI 2.0接口
df = pd.DataFrame()
for i in range(40):
    params = {
        'key': 'f9073085d13f659b039bebce663230c8',
        'keywords': '冯四嬢',
        'region': '乐山市',
        'page_size': 25,
        'page_num': i
    }

    response = requests.get(
        "https://restapi.amap.com/v5/place/text", params=params)

    content_json = json.loads(response.text)
    df = pd.concat([df, pd.DataFrame(content_json['pois'])])

# %%
#  行政区域查询接口

region = ['河北省', '山西省', '辽宁省', '吉林省', '黑龙江省', '江苏省', '浙江省', '安徽省', '福建省', '江西省', '山东省', '河南省', '湖北省', '湖南省', '广东省', '海南省', '四川省', '贵州省',
          '云南省', '陕西省', '甘肃省', '青海省', '内蒙古自治区', '广西壮族自治区', '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区', '北京市', '天津市', '上海市', '重庆市', '香港特别行政区', '澳门特别行政区']

df = pd.DataFrame()
for i in region:

    params = {
        'key': 'f9073085d13f659b039bebce663230c8',
        'keywords': i,
        'subdistrict': 2
    }

    response = requests.get(
        url='https://restapi.amap.com/v3/config/district', params=params)
    content_json = json.loads(response.text)

    # df = pd.concat([df, pd.DataFrame(jsonpath.jsonpath(
    #     content_json, '$.districts[0]..districts[*]'))])
    df = pd.concat([df, pd.json_normalize(
        content_json, ['districts', 'districts'], [['districts', 'name']])])
    # df = pd.concat([df, pd.DataFrame(jsonpath.jsonpath(content_json, '$.districts[0]..districts[*]'))])
    print(i)
df


# 行政区划接口

region = ['河北省', '山西省', '辽宁省', '吉林省', '黑龙江省', '江苏省', '浙江省', '安徽省', '福建省', '江西省', '山东省', '河南省', '湖北省', '湖南省', '广东省', '海南省', '四川省', '贵州省',
          '云南省', '陕西省', '甘肃省', '青海省', '内蒙古自治区', '广西壮族自治区', '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区', '北京市', '天津市', '上海市', '重庆市', '香港特别行政区', '澳门特别行政区']

df = pd.DataFrame()
for i in region:

    params = {
        'key': 'f9073085d13f659b039bebce663230c8',
        'keywords': i,
        'subdistrict': 2
    }

    response = requests.get(
        url='https://restapi.amap.com/v3/config/district', params=params)
    content_json = json.loads(response.text)

    # df = pd.concat([df, pd.DataFrame(jsonpath.jsonpath(
    #     content_json, '$.districts[0]..districts[*]'))])
    df = pd.concat([df, pd.json_normalize(content_json, record_path=['districts', 'districts', 'districts'], meta=[
                   ['districts', 'name'], ['districts', 'districts', 'name']], meta_prefix='province.')])
    # df = pd.concat([df, pd.DataFrame(jsonpath.jsonpath(content_json, '$.districts[0]..districts[*]'))])
    print(i)
df


# %%

params = {
    'key': 'f9073085d13f659b039bebce663230c8',
    'origin': '117.214191,29.351207',
    'destination': '117.218968,29.301112'
}

response = requests.get(
    url='https://restapi.amap.com/v3/direction/driving', params=params)

content_json = json.loads(response.text)
# %%

actually = pd.read_feather('实际要货.feather')
predict = pd.read_feather('预测要货.feather')
df = pd.merge(actually, predict, how='outer', indicator=True)


df = df[(df['RPTDATE'] > '2022-10-08') & (df['RPTDATE'] < '2022-11-09')
        ].sort_values(by=['RPTDATE', 'ORGCODE', 'PLUCODE'], ignore_index=True)

df['PLUCOUNT'].fillna(0, inplace=True)

s = df.groupby('PLUCODE')['YH_COUNT'].count()
l = s[s == 0].index.to_list()


df2 = df[(df['YH_COUNT'].isna()) & (
    ~df['PLUCODE'].isin(l))].reset_index(drop=True)
