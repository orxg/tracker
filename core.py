# -*- coding: utf-8 -*-
"""
Created on Fri Aug 24 15:10:17 2018

@author: ldh

定义模拟基类。
该类实现FOF构造的接口,对于特定目的的FOF,继承该类后实现基金调仓逻辑(包括基金筛选)。
该基类负责FOF组合净值计算与存储,FOF组合历史持仓计算与存储,FOF组合调仓计算与存储。
"""

# core.py
from abc import ABCMeta,abstractmethod

#import datetime as dt
#import pandas as pd

class StrategyBuilder:
    __metaclass__ = ABCMeta
    
    def __init__(self,strategy_name,start_date,data_proxy,persister,refresh_rate = 1):
        '''
        回测抽象基类。
        
        Parameters
        ------------
        strategy_name
            str,策略名称
        start_date
            YYYYMMDD,策略起始日期
        data_proxy
            数据代理对象
        persister
            持久化对象
        refresh_rate
            int,持仓更新频率,默认1个交易日
        '''
        self.strategy_name = strategy_name
        self.start_date = start_date
        self.refresh_rate = refresh_rate
        self.refresh_counter = 0
        self.refresh_flag = False
        
        self.data_proxy = data_proxy
        self.persister = persister
        self.trade_calendar = self.data_proxy.get_trade_calendar()
        self.last_trade_date = self.trade_calendar['trade_date'].iloc[-1]
        

        self.daily_ret = None 
        self.position = {}  # 当前持仓
        self.target_position = {} # 目标持仓
        self.rebalance = {} 
        self.current_date = self.start_date
                
        self.daily_ret_hist = []# [trade_date,ret]
        self.position_hist = []# [trade_date,{code:weight}]
        self.rebalance_hist = []# [trade_date,{code:weight_change}]
        
    def run(self):
        self._load_state() # 读取策略最近运行状态
        
        while self.current_date <= self.last_trade_date:
            if self.refresh_counter == 0:
                self.refresh_flag = True
            elif self.refresh_counter > 0 and self.refresh_counter < self.refresh_rate:
                self.refresh_flag = False
            elif self.refresh_flag == self.refresh_rate:
                self.refresh_flag = False
                self.refresh_flag = -1         
            
            if self.refresh_flag is True:
                self.run_strategy() # 更新self.target_position {}
                
            self._update()   # 更新
            self.refresh_counter += 1
            
        self._save_state()
        
    def _load_state(self):
        '''
        读取策略当前状态.
        '''
        self.position,\
        self.current_date, \
        self.refresh_rate,\
        self.refresh_counter = self.persister.load_state()
        
                    
    def _save_state(self):
        '''
        保存策略运行状态.
        '''
        state = {'position_hist':self.position_hist,
                 'rebalance_hist':self.rebalance_hist,
                 'refresh_rate':self.refresh_rate,
                 'refresh_counter':self.refresh_counter,
                 'daily_ret':self.daily_ret_hist}
        self.persister.save_state(state)
    
    def _update(self):
        '''
        更新回测组合position、rebalance、current_date、daily_ret
        position_hist,rebalance_hist,daily_ret_hist
        '''
        if self.refresh_flag is True: # 当日调仓
            target_funds = self.target_position.keys()
            self.rebalance = {}
            for fund in target_funds:
                if self.position.has_key(fund):
                    t_w = self.target_position[fund]
                    c_w = self.position[fund]
                    w_c = t_w - c_w
                    if w_c == 0:
                        continue
                    self.rebalance[fund] = w_c
                else:
                    self.rebalance[fund] = self.target_position[fund]
            
            self.position = self.target_position
            self.position_hist.append([[self.current_date,self.position]])
            self.rebalance.append([[self.current_date,self.rebalance]])
        else: # 当日不调仓
            self.position_hist.append([[self.current_date,self.position]])
            
        
        # 更新current_date
        cur_idx = self.trade_calendar['trade_date'].tolist().index(self.current_date)
        try:
            self.current_date = self.trade_calendar['trade_date'].loc[cur_idx + 1]
        except IndexError:
            pass
        # 更新日收益率(按照当前持仓当日收益率计算,遇到调仓按照调仓后的计算)
        pass
    
    def _get_rebalance(self):
        '''
        计算调仓.
        '''
        pass
    
    @abstractmethod
    def run_strategy(self):
        '''
        抽象方法,生成最新持仓。
        '''
        pass
    

    
    
    
        
        
        