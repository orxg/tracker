# -*- coding: utf-8 -*-
"""
Created on Fri Aug 24 15:10:17 2018

@author: ldh

定义模拟基类。
该类实现FOF构造的接口,对于特定目的的FOF,继承该类后实现基金调仓逻辑(包括基金筛选)。
该基类负责FOF组合净值计算与存储,FOF组合历史持仓计算与存储,FOF组合调仓计算与存储。
"""

# core.py
from __future__ import print_function
from abc import ABCMeta,abstractmethod
import pandas as pd


class StrategyBuilder:
    __metaclass__ = ABCMeta
    def __init__(self,strategy_name,start_date,data_proxy,persister,refresh_rate = 1,universe = None):
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
        universe
            list,交易标的代码池
        '''
        self.strategy_name = strategy_name
        self.start_date = start_date
        self.refresh_rate = refresh_rate
        self.universe = universe
        self.refresh_counter = 0
        self.refresh_flag = False
        
        self.data_proxy = data_proxy
        self.persister = persister
        
        self.daily_ret = None 
        self.position = {}  # 当前持仓
        self.target_position = {} # 目标持仓
        self.rebalance = {} 
        self.current_date = self.start_date
                
        self.daily_ret_hist = []# [trade_date,ret]
        self.position_hist = []# [trade_date,{code:weight}]
        self.rebalance_hist = []# [trade_date,{code:weight_change}]
        
    def run(self):
        self._initilize_data()
        print('Initilize data sucessfully!')
        self.trade_calendar = self.data_proxy.get_trade_calendar()
        print('Get calendar sucessfully!')
        self.last_trade_date = self.trade_calendar['trade_date'].iloc[-1]
        self._load_state() # 读取策略最近运行状态
        print('Load state sucessfully!')
        
        while self.current_date < self.last_trade_date:
            if self.refresh_counter == 0:
                self.refresh_flag = True
            elif self.refresh_counter > 0 and self.refresh_counter < self.refresh_rate:
                self.refresh_flag = False
            elif self.refresh_counter == self.refresh_rate:
                self.refresh_flag = False
                self.refresh_counter = -1         
            
            if self.refresh_flag is True:
                self.run_strategy() # 更新self.target_position {}
#            print 'Going to update'
            state = self._update()   # 更新
            if state == 0:
                break
#            print 'Update Sucessfully'
            self.refresh_counter += 1
#            print(self.current_date)
            
        self._save_state()
        print('Save state sucessfully!')
        
    def _initilize_data(self):
        '''
        初始化策略数据.
        '''
        self.data_proxy.initilize_data(self.start_date,self.universe)
    
    def _load_state(self):
        '''
        读取策略当前状态.
        '''
        try:
            self.position,\
            self.current_date, \
            self.refresh_rate,\
            self.refresh_counter = self.persister.load_state()
        except ValueError:
            self.position = {}
            self.current_date = self.trade_calendar['trade_date'].iloc[0]
            self.refresh_rate = self.refresh_rate
            self.refresh_counter = self.refresh_counter
            
    def _save_state(self):
        '''
        保存策略运行状态.
        '''
        state = {'strategy_name':self.strategy_name,
                'position_hist':self.position_hist,
                 'rebalance_hist':self.rebalance_hist,
                 'refresh_rate':self.refresh_rate,
                 'refresh_counter':self.refresh_counter,
                 'daily_ret':self.daily_ret_hist}
        self.persister.save_state(state)
    
    def _update(self):
        '''
        更新回测组合
        
        Notes
        --------
        调仓日卖出和买入采用当日收盘净值,不考虑赎回申购时间上的滞后影响。
        '''
        # 更新日收益率与新权重
        self.daily_ret = 0
        total_weight = 0
        new_weight = {}

        for key,val in self.position.items():
            ret = self.data_proxy.get_daily_ret(key,self.current_date)
            new_weight[key] = val * (1 + ret)           
            self.daily_ret += val * ret
            total_weight += new_weight[key]            
        for key,val in new_weight.items():
            new_weight[key] /= float(total_weight)
        self.position = new_weight.copy()

        # 当日调仓
        if self.refresh_flag is True: 
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
            # 默认成功调仓
            self.position = self.target_position
            self.rebalance_hist.append([self.current_date,self.rebalance])
            
        # 记录
        self.daily_ret_hist.append([self.current_date,self.daily_ret])
        self.position_hist.append([self.current_date,self.position])
                        
        # 更新current_date
        self.cur_idx = self.trade_calendar['trade_date'].tolist().index(self.current_date)        
        try:
            self.current_date = self.trade_calendar['trade_date'].loc[self.cur_idx + 1]
        except (IndexError,KeyError):
            return 0
        return 1
    
    @abstractmethod
    def run_strategy(self):
        '''
        抽象方法,生成最新持仓。
        '''
        pass
    
    def plot(self):
        ret_df = pd.DataFrame(self.daily_ret_hist,columns = ['trade_date','daily_ret'])
        ret_df['nv'] = (ret_df['daily_ret'] + 1.).cumprod()
        ret_df['trade_date'] = pd.to_datetime(ret_df['trade_date'])
        nv_ser = ret_df[['trade_date','nv']].set_index('trade_date')
        nv_ser.plot(figsize = (12,8))
    
    
    
        
        
        