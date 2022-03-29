import pandas as pd
import numpy as numpy
from functools import reduce
import pandas as pd
import time
from functools import reduce
import numpy as np
from pandas import DataFrame
from pandas import Series
class minute_format_data:
    def __init__(self, raw_data: DataFrame):
        self.guide = '包含 convert_time VWAP minute_format_data函数，\n 用于制作分钟级数，输出Time,OpenPrice, ClosePrice, High_price, Low_price, Vwap, TotalAmount, BuyNum, SellNum, UptickNum,DowntickNum'
        print(self.guide)
        self.data = raw_data

    def convert_time(self, x, normal=True):
        time_local = time.localtime(x)
        if normal:
            timestring = time.strftime("%Y%m%dT%H:%M", time_local)
            result = timestring + ':00'
        else:
            result = time.strftime("%d-%m-%Y", time_local)

        return (result)

    def VWAP(self, x):
        try:
            vwap = np.average(a=x.iloc[:, 0], weights=x.iloc[:, 1])
        except ZeroDivisionError as e:
            vwap = np.average(a=x.iloc[:, 0])
        return (vwap)

    def format_data(self, timestamp, price):
        temp_data = self.data
        temp_data.sort_values(by=timestamp, ascending=True, inplace=True)
        time_string_out = temp_data[timestamp].map(self.convert_time)
        temp_data['Time'] = time_string_out
        print('successfully changed timestamp with formattime')
        diff = np.diff(temp_data[price].values)
        down_or_up = diff > 0
        #### 去掉 number ###
        number = temp_data.shape[0]
        temp_data.index = np.array(range(0, number))
        all_data = temp_data.drop(index=0)
        all_data['diff'] = diff
        all_data['down_or_up'] = down_or_up
        ### obtain the  LastPrice VWAP TotalAmount BuyNum SellNum
        OpenPrice = all_data.groupby("Time")[price].agg(lambda x: x.values[0])
        ClosePrice = all_data.groupby("Time")[price].agg(lambda x: x.values[-1])
        High_price = all_data.groupby("Time")[price].max()
        Low_price = all_data.groupby('Time')[price].min()
        TotalAmount = all_data.groupby('Time')["amount"].sum()
        BuyNum = all_data.groupby('Time')['side'].agg(lambda x: (x.values == 1).sum())
        SellNum = all_data.groupby('Time')['side'].agg(lambda x: (x.values == 2).sum())
        UptickNum = all_data.groupby('Time')["down_or_up"].agg(lambda x: (x.values).sum())
        DowntickNum = all_data.groupby('Time')["down_or_up"].agg(lambda x: (1 - x.values).sum())
        Vwap = all_data.groupby('Time')[[price, "amount"]].apply(self.VWAP)
        ### 为Series添加名###
        OpenPrice.name = 'OpenPrice'
        ClosePrice.name = 'ClosePrice'
        High_price.name = 'HighPrice'
        Low_price.name = 'LowPrice'
        TotalAmount.name = 'TotalAmount'
        BuyNum.name = 'BuyNum'
        SellNum.name = 'SellNum'
        UptickNum.name = 'UptickNum'
        DowntickNum.name = 'DowntickNum'
        Vwap.name = "Vwap"
        data_list = [OpenPrice, ClosePrice, High_price, Low_price, Vwap, TotalAmount, BuyNum, SellNum, UptickNum,
                     DowntickNum]
        minute_data = reduce(lambda x, y: pd.merge(x, y, on='Time'), data_list)
        minute_data.reset_index(inplace=True)
        return (minute_data)

class time_format_data:
    def __init__(self, minute_data: DataFrame):
        self.data = minute_data
        self.data
        print('使用需知：严格依赖数据排序，输入数必须按照时间升序排列')
        print('使用分钟级数据，整合为多重时间的数据')
        print('方法 ： VWAP,time_format_data(timescale)')

    def VWAP(self, x):
        if type(x) == Series:
            try:
                vwap = np.average(a=x.iloc[:,0], weights=x.iloc[:,1])
            except Exception as e:
                vwap = np.average(a=x.iloc[:,0])
        else:
            vwap = np.average(a=x.iloc[:,0])
        return (vwap)

    def time_format_data(self, timescale: str):
        self.data['Time'] =pd.to_datetime(self.data['Time'], format='%Y%m%dT%H:%M:%S')
        looper = self.data.resample(timescale, on='Time')
        dict_list = []
        for i in looper:
            temp_data = i[1]
            if i[1].empty!=True:
                dict_i = {"Time": str(i[0]),
                          #'Vwap': temp_data[['Vwap', 'TotalAmount']].copy().apply(self.VWAP),
                          'Vwap' : sum(temp_data['Vwap']*temp_data['TotalAmount'])/temp_data['TotalAmount'].sum(),
                          'OpenPrice': temp_data['OpenPrice'].iloc[0],
                          'ClosePrice': temp_data['ClosePrice'].iloc[-1],
                          'HighPrice': temp_data['HighPrice'].max(),
                          'LowPrice': temp_data['LowPrice'].min(),
                          'TotalAmount': temp_data['TotalAmount'].sum(),
                          'BuyNum': temp_data['BuyNum'].sum(),
                          'SellNum': temp_data['SellNum'].sum(),
                          'DowntickNum': temp_data['DowntickNum'].sum(),
                          'UptickNum': temp_data['UptickNum'].sum()}
                dict_list.append(dict_i)
            else:
                pass
        re_df = pd.DataFrame(dict_list)
        return (re_df)
