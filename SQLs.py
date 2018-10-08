# -*- coding: utf-8 -*-
"""
Created on Mon Oct 08 15:15:54 2018

@author: ldh
"""

# SQLs.py

#%% SAVE & LOAD DATA
SQL_LOAD_POSITION = '''
SELECT *
FROM FOF_position
WHERE
strategy_name = '{strategy_name}'
AND trade_date = 
(SELECT max(trade_date) as trade_date
FROM FOF_position
WHERE strategy_name = '{strategy_name}')
'''

SQL_LOAD_REFRESH_STATE = '''
SELECT *
FROM FOF_refresh_state
WHERE
strategy_name = '{strategy_name}'
AND update_date = 
(SELECT max(update_date) as update_date
FROM FOF_refresh_state
WHERE strategy_name = '{strategy_name}')
'''

SQL_MERGE_POSITION = '''
MERGE INTO FOF_position as T
USING FOF_position_source as S
ON T.strategy_name = S.strategy_name 
AND T.trade_date = S.trade_date
AND T.fund_code = S.fund_code
WHEN NOT MATCHED BY TARGET
THEN INSERT
(trade_date,strategy_name,fund_code,fund_weight)
VALUES
(S.trade_date,S.strategy_name,S.fund_code,S.fund_weight);
'''


SQL_MERGE_REBALANCE = '''
MERGE INTO FOF_rebalance as T
USING FOF_rebalance_source as S
ON T.strategy_name = S.strategy_name 
AND T.trade_date = S.trade_date
AND T.fund_code = S.fund_code
WHEN NOT MATCHED BY TARGET
THEN INSERT
(trade_date,strategy_name,fund_code,direction,weight_change)
VALUES
(S.trade_date,S.strategy_name,S.fund_code,S.direction,S.weight_change);
'''

SQL_MERGE_REFRESH_STATE = '''
MERGE INTO FOF_refresh_state as T
USING FOF_refresh_state_source as S
ON T.strategy_name = S.strategy_name 
AND T.update_date = S.update_date
WHEN NOT MATCHED BY TARGET
THEN INSERT
(strategy_name,refresh_rate,refresh_counter,update_date)
VALUES
(S.strategy_name,S.refresh_rate,S.refresh_counter,S.update_date);
'''

SQL_MERGE_DAILY_RET = '''
MERGE INTO FOF_daily_ret as T
USING FOF_daily_ret_source as S
ON T.strategy_name = S.strategy_name
AND T.trade_date = S.trade_date
WHEN NOT MATCHED BY TARGET
THEN INSERT
(strategy_name,trade_date,ret)
VALUES
(S.strategy_name,S.trade_date,S.ret);
'''

