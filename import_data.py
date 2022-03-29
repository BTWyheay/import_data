import os
import pandas as pd
import numpy as np
import time
import requests as re
from urllib import error
import datetime
from functools import reduce
import time
from data_format import *
coin_list = ['AAVE_USDT',
 'ADA_USDT',
 'BCH_USDT',
 'BNB_USDT',
 'BSV_USDT',
 'BTC_USDT',
 'DOGE_USDT',
 'DOT_USDT',
 'EOS_USDT',
 'ETC_USDT',
 'ETH_USDT',
 'FIL_USDT',
 'GT_USDT',
 'HT_USDT',
 'LINK_USDT',
 'LTC_USDT',
 'TRX_USDT',
 'UNI_USDT',
 'XRP_USDT',
 'ZEC_USDT']
failed_list = []
url = 'https://download.gatedata.org/spot/deals/{year}{month}/{market}-{year}{month}.csv.gz'

year =['2018','2019','2020','2021','2022']
month = ['01','02','03','04','05','06','07','08','09','10','11','12']

os.makedirs('/home/puma/DATA/1m/', exist_ok=True)

## download all the data ##
for j in year:
    for i in coin_list:
        file_dir = '/home/puma/DATA/{coin}/{year}/'.format(coin = i,year = j)
        # os.makedirs(file_dir,exist_ok=True)
        for k in month:
            url2 = url.format(month=k, market=i,year = j)
            file_name = file_dir + i + '_' + j +'_'+ k +'.csv.gz'
            r = re.get(url2)
            time.sleep(1)
            if r.status_code == 200:
                with open(file_name, "wb") as code:
                    code.write(r.content)
            else:
                print('falied' + j + i)
                failed_list.append(j + i)
##按照年份进行数据整理
for coin in coin_list:
    year_list =[]
    file_coin_list =[]
    for yr in year:
        file_dir = '/home/puma/DATA/{coin}/{year}/'.format(coin = coin,year = year)
        try:

            file_dir_content = os.listdir(file_dir)
            for csv in file_dir_content:
                file_name = file_dir + csv
                try:
                    temp_df = pd.read_csv(file_name, compression='gzip',header=None, names=['timestamp', 'dealid', 'price', 'amount', 'side'])
                    year_list.append(temp_df)
                except FileNotFoundError as ex:
                    print(ex)

                if year_list!= []:
                    temp_raw = pd.concat(year_list, axis=0)
                    temp_raw.timestamp.drop_duplicates(inplace=True)
                    a = minute_format_data(temp_raw)
                    out_data = a.format_data('timestamp', 'price')
                    os.makedirs(file_dir + 'minute/')
                    out_data.to_csv(file_dir + 'minute/' +coin + '.csv', index=False)
                    file_coin_list.append(file_dir + 'minute/'+ coin + '.csv')

        except FileNotFoundError as e:
            print(e)

for coin in coin_list:
    coin_minute=[]
    for yr in year:
        try:
            temp_df = pd.read_csv('/home/puma/DATA/{coin}/{year}/{coin}.csv'.format(coin =coin,year =yr))
            coin_minute.append(temp_df)
        except FileNotFoundError as e:
            print(e)

    if coin_minute!= []:
        all_minute = pd.concat(coin_minute, axis=0)
        all_minute.drop_duplicates(inplace=True)
        all_minute.to_csv('/home/puma/DATA/1m/{}_1M.csv'.format(coin))

































