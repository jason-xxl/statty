import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_login(my_date): # run on 0:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='login_only_globe'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    # login 1 day
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='user_last_login_1_day_unique'
    sql=r'''
    
    select count(*) 
    from mozone_user.dbo.profile with(nolock) 
    where [lastLogin]>=DATEADD(day,-1,Getdate()) 
    and version_tag='fast_globe'
    and user_name not like '%00000%'
    
    ''' 
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # login 1 day which is also new sub in 24 hour
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='24h_new_sub_user_last_login_1_day_unique'
    sql=r'''
    
    select count(*) 
    from mozone_user.dbo.profile with(nolock) 
    where [lastLogin]>=DATEADD(day,-1,Getdate()) 
    and version_tag='fast_globe'
	and [creationDate]>'2000-01-01'
	and [lastLogin]>'2000-01-01'
    and datediff(hour,[creationDate],[lastLogin])<=24    
    and user_name not like '%00000%'
    ''' 
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    

    # login 7 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*6)

    db=''
    key='user_last_login_7_day_unique'
    sql=r'''
    
    select count(*) 
    from mozone_user.dbo.profile with(nolock) 
    where [lastLogin]>=DATEADD(day,-7,Getdate()) 
    and version_tag='fast_globe'
    and user_name not like '%00000%'

    ''' 
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # login 30 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*29)

    db=''
    key='user_last_login_30_day_unique'
    sql=r'''
    
    select count(*) 
    from mozone_user.dbo.profile with(nolock) 
    where [lastLogin]>=DATEADD(day,-30,Getdate()) 
    and version_tag='fast_globe'
    and user_name not like '%00000%'

    '''
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_login(my_date)
