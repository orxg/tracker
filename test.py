# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 13:52:27 2018

@author: ldh
"""

# test.py

from core import StrategyBuilder
from data_proxy import DataProxy,PodaciDataSource
from persister import GuosenPersister

class TestStrategy(StrategyBuilder):
    def run_strategy(self):
        self.target_position = {u'000011':0.5,u'002360':0.5}
    
if __name__ == '__main__':
    data_proxy = DataProxy(PodaciDataSource())
    persister = GuosenPersister('test')
    test_strategy = TestStrategy('test','20170101',data_proxy,persister,refresh_rate = 20,
                                     universe = [u'000011',u'002360'])
    test_strategy.run()
    
    
'''
delete from FOF_daily_ret 
where strategy_name = 'test'
delete from FOF_position 
where strategy_name = 'test'
delete from FOF_rebalance
where strategy_name = 'test'
delete from FOF_refresh_state
where strategy_name = 'test
'''