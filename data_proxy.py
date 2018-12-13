# -*- coding: utf-8 -*-
"""
Created on Mon Oct 08 13:36:17 2018

@author: ldh
"""

# data_proxy.py
import datetime as dt
from dateutil import relativedelta
import pandas as pd
from podaci.guosen.data import get_trade_calendar,get_funds_net_value

class DataProxy():
    def __init__(self,data_source):
        self.data_source = data_source
        
    def initilize_data(self,start_date,universe):
        '''
        初始化回测数据
        '''
        self.data_source.initilize_data(start_date,universe)
        
    def get_trade_calendar(self):
        '''
        获取截止今天的交易日历
        
        Returns
        --------
        DataFrame
            column:trade_date
        
        Returns
        -------
        DataFrame with column trade_date
        '''
        return self.data_source.get_trade_calendar()
    
    def get_daily_ret(self,code,date):
        return self.data_source.get_daily_ret(code,date)
    
class PodaciDataSource:
    def __init__(self):
        pass
    
    def initilize_data(self,start_date,universe):
        '''
        初始化回测数据
        '''
        today = dt.datetime.today().strftime('%Y%m%d')
        start_date_adj = (pd.Timestamp(start_date) - relativedelta.relativedelta(months = 1)).strftime('%Y%m%d')
        self.trade_calendar = get_trade_calendar(start_date,today)
    
        self.data = get_funds_net_value(universe,start_date_adj,today)
        self.data = pd.pivot_table(self.data,index=['trade_date'],columns = ['trade_code'],values = ['net_value_adj'])['net_value_adj']
        self.data.sort_index(ascending = True,inplace = True)
        self.data_ret = self.data.pct_change().fillna(0)
        
    def get_trade_calendar(self):
        return self.trade_calendar
    
    def get_daily_ret(self,code,date):
        return self.data_ret.loc[date,code]
