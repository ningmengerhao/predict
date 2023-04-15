from ast import If
from operator import ifloordiv
from tarfile import PAX_NAME_FIELDS
import pandas as pd
import numpy as np
import os
import copy
import json
import time
from math import floor
import matplotlib.pyplot as plt
import requests


def get_resourse(k, start_x, start_y, destination_x, destination_y):  # 调用api
    api = 'https://restapi.amap.com/v3/direction/driving?parameters'
    parameters = {
        'key': k,
        'origin': '%s' % start_x + ',' + '%s' % start_y,  # 开始经纬度
        'destination': '%s' % destination_x + ',' + '%s' % destination_y,  # 结束经纬度
        'extensions': 'base'
    }
    r = requests.get(api, params=parameters)
    r = r.text
    return t, d


data = pd.read_csv('收运经纬度.csv', encoding='gbk')
data = pd.read_excel('getlocation_1.xlsx')
# 构建距离0矩阵
distance_maxtri = np.zeros((data.shape[0], data.shape[0]))


# 构建时间0矩阵

time_maxtri = np.zeros((data.shape[0], data.shape[0]))

distance_maxtri = pd.DataFrame(
    distance_maxtri, columns=data['经纬度'], index=data['经纬度'])
time_maxtri = pd.DataFrame(time_maxtri, columns=data['经纬度'], index=data['经纬度'])

for j in range(data.shape[0]):
    if i == j:
        continue
    if i < j:
        start_x1 = data.loc[i, '经度']
        start_y1 = data.loc[i, '纬度']
        destination_x1 = data.loc[j, '经度']
        destination_y1 = data.loc[j, '纬度']
        start_x2 = data.loc[j, '经度']
        start_y2 = data.loc[j, '纬度']
        destination_x2 = data.loc[i, '经度']
        destination_y2 = data.loc[i, '纬度']
        k = 'd4bb672882ccdfe4f49d421551ca4ec6'
        time_maxtri[i, j], distance_maxtri[i, j] = get_resourse(
            k, start_x1, start_y1, destination_x1, destination_y1)
        time_maxtri[j, i], distance_maxtri[j, i] = get_resourse(k, start_x2, start_y2, destination_x2,
                                                                destination_y2)
        print('第', i, '行', distance_maxtri[i, j])
# print(j)
# time_maxtri[j + 1, i], distance_maxtri[j + 1, i] =get_resourse(k,start_x2,start_y2,destination_x2,destination_y2)
pd.DataFrame(distance_maxtri, columns=data['经纬度'], index=data['经纬度']).to_csv(
    'distance_1.csv')
pd.DataFrame(time_maxtri, columns=data['经纬度'],
             index=data['经纬度']).to_csv('time_1.csv')
