# -*- coding: utf-8 -*-
"""
Created on Mon Oct 08 13:36:17 2018

@author: ldh
"""

# data_proxy.py

from podaci.guosen.data import get_trade_calendar

class DataProxy():
    def __init__(self,data_source):
        self.data_source = data_source
        
    def get_trade_calendar(self):
        '''
        获取截止今天的交易日历。
        
        Returns
        --------
        DataFrame
            column:trade_date
        '''
        return self.data_source.get_trade_calendar()
    

class PodaciDataSource:
    def __init__(self):
        pass
    
    def get_trade_calendar(self):
        return get_trade_calendar()
    
