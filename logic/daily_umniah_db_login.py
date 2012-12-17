import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_login(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Umniah'
    stat_category='login'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    # login 1 day
    
    start_time=helper_regex.time_floor(my_date)

    db='umniah_mt'
    key='user_last_login_1_day_unique'
    sql=r'''
    
    select count(*) from mozone_user.dbo.profile with(nolock) where [lastLogin]>=DATEADD(day,-1,Getdate())
    
    '''

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    


    # login 7 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*6)

    db='umniah_mt'
    key='user_last_login_7_day_unique'
    sql=r'''
    
    select count(*) from mozone_user.dbo.profile with(nolock) where [lastLogin]>=DATEADD(day,-7,Getdate())
    
    ''' 

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # login 30 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*29)

    db='umniah_mt'
    key='user_last_login_30_day_unique'
    sql=r'''
    
    select count(*) from mozone_user.dbo.profile with(nolock) where [lastLogin]>=DATEADD(day,-30,Getdate())
    
    ''' 

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_login(my_date)
