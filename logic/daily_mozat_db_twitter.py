import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_twitter(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='twitter'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    # new account
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='newly_created_twitter_account_unique'
    sql=r'''
    
    select 
    count(distinct user_name) 
    from motwitter.accounts 
    where 
    created_on>='%s' and created_on<'%s'
    
    ''' \
         % (start_time,end_time)
    print 'Mysql Server:'+sql
    value=helper_mysql.get_one_value_int(sql,config.conn_mozat_twitter)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_twitter(my_date)
