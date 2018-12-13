# -*- coding: utf-8 -*-
"""
Created on Mon Oct 08 13:46:16 2018

@author: ldh
"""

# persister.py
import datetime as dt
from abc import ABCMeta,abstractmethod
import pandas as pd
from sqlalchemy.types import VARCHAR,DECIMAL,INT
from podaci.guosen.data import get_data,execute_session,save_into_db
from SQLs import (SQL_LOAD_POSITION,SQL_LOAD_REFRESH_STATE,SQL_MERGE_POSITION,
                  SQL_MERGE_REBALANCE,SQL_MERGE_REFRESH_STATE,SQL_MERGE_DAILY_RET)

class Persister:
    __metaclass__ = ABCMeta
    def __init__(self):
        pass
    
    @abstractmethod
    def load_state(self):
        '''
        读取策略状态.
        
        首次创建策略读取无法成功的,报错.
        '''
        pass
    
    @abstractmethod
    def save_state(self):
        '''
        存储策略状态.
        '''
        pass
    
class GuosenPersister(Persister):
    
    def __init__(self,strategy_name):
        self.strategy_name = strategy_name
    
    def load_state(self):
        position = get_data(SQL_LOAD_POSITION.format(strategy_name = self.strategy_name),
                            'xiaoyi')

        # 首次运行策略
        if len(position) == 0:
            raise ValueError('New Strategy')
            
        position_dict = {}
        for idx,row in position.iterrows():
            fund_code = row['fund_code']
            fund_weight = row['fund_weight']
            position_dict[fund_code] = fund_weight
            
        current_date = position['trade_date'].values[0]
        
        refresh_state = get_data(SQL_LOAD_REFRESH_STATE.format(strategy_name = self.strategy_name),
                           'xiaoyi')
        refresh_rate = refresh_state['refresh_rate'].values[0]
        refresh_counter = refresh_state['refresh_counter'].values[0]
        
        return position_dict,current_date,refresh_rate,refresh_counter
    
    def save_state(self,state):
        strategy_name = state['strategy_name']
        position_hist = state['position_hist']
        rebalance_hist = state['rebalance_hist']
        refresh_rate = state['refresh_rate']
        refresh_counter = state['refresh_counter']
        daily_ret = state['daily_ret']
        
        # position
        position_hist_list = []
        for each in position_hist:
            for key,val in each[1].items():
                position_hist_list.append([each[0],strategy_name,key,val])
        position_hist_df = pd.DataFrame(position_hist_list,
                                        columns = ['trade_date','strategy_name',
                                                   'fund_code','fund_weight'])
        
        save_into_db(position_hist_df,'FOF_position_source',{'trade_date':VARCHAR(8),
                                                             'strategy_name':VARCHAR(32),
                                                             'fund_code':VARCHAR(32),
                                                             'fund_weight':DECIMAL(10,6)},
        'xiaoyi','replace')
        execute_session(SQL_MERGE_POSITION,'xiaoyi')
    
        # rebalance
        rebalance_hist_list = []
        for each in rebalance_hist:
            for key,val in each[1].items():
                if val > 0:
                    direction = 'LONG'
                elif val < 0:
                    direction = 'SHORT'
                rebalance_hist_list.append([each[0],strategy_name,key,direction,val])
        rebalance_hist_df = pd.DataFrame(rebalance_hist_list,
                                         columns = ['trade_date','strategy_name',
                                                    'fund_code','direction','weight_change'])
        save_into_db(rebalance_hist_df,'FOF_rebalance_source',{'trade_date':VARCHAR(8),
                                                               'strategy_name':VARCHAR(32),
                                                               'fund_code':VARCHAR(32),
                                                               'direction':VARCHAR(8),
                                                               'weight_change':DECIMAL(10,6)},
        'xiaoyi','replace')
        execute_session(SQL_MERGE_REBALANCE,'xiaoyi')
        
        # refresh
        refresh_state_df = pd.DataFrame([[strategy_name,refresh_rate,refresh_counter,dt.datetime.today().strftime('%Y%m%d')]],
                                          columns = ['strategy_name','refresh_rate','refresh_counter','update_date'])
        save_into_db(refresh_state_df,'FOF_refresh_state_source',{'strategy_name':VARCHAR(32),
                                                           'refresh_rate':INT,
                                                           'refresh_counter':INT,
                                                           'update_date':VARCHAR(32)},
        'xiaoyi','replace')
        execute_session(SQL_MERGE_REFRESH_STATE,'xiaoyi')
        
        # daily_ret
        daily_ret_df = pd.DataFrame(daily_ret,columns = ['trade_date','ret'])
        daily_ret_df.loc[:,'strategy_name'] = strategy_name
        
        save_into_db(daily_ret_df,'FOF_daily_ret_source',{'strategy_name':VARCHAR(32),
                                                          'trade_date':VARCHAR(8),
                                                          'ret':DECIMAL(10,6)},
        'xiaoyi','replace')
        execute_session(SQL_MERGE_DAILY_RET,'xiaoyi')
        
        
        