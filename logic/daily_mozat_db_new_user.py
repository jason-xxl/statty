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

    oem_name='Mozat'
    stat_category='login'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')


    #historical user

    db=''
    sql=r'''
    
    select 
    count([user_id]) as historical_total_user

    from [mozone_user].[dbo].[ProfileSub] with(nolock) 
    where 
    [creationDate]<'%s'

    ''' % (end_time,)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_mozat,sql)

    print values

    key='historical_total_user'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['historical_total_user'])

    

    # register 1 day
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    sql=r'''
    
    select 
    count(*) as new_user
    ,count(case when lastLogin is not null then lastLogin else null end) as logined_user

    from mozone_user.dbo.profile with(nolock) 
    where 
    [creationDate]>='%s'
    and [creationDate]<'%s'
    
    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_mozat,sql)

    print values

    key='user_register_1_day_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['new_user'])

    key='user_register_logined_1_day_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['logined_user'])


    

    # register 1 day (mozat 6)
    
    stat_category='login_mozat_6'

    start_time=helper_regex.time_floor(my_date)

    db=''

    sql=r'''
    
    select 
    count(*) as new_user
    ,count(case when lastLogin is not null then lastLogin else null end) as logined_user

    from [DB81].[mozone_user].[dbo].[Profile] with(nolock)
    where [creationDate]>='%s'
    and [creationDate]<'%s'
    and [user_name] like '%%@morange.com'
    and dbo.find_regular_expression([user_name],N'^M\d+@morange.com',0)=1

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_helper_db,sql)

    print values

    key='user_register_1_day_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['new_user'])

    key='user_register_logined_1_day_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['logined_user'])




if __name__=='__main__':
    
    #track 5 day to see whether user succeeded to login

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_login(my_date)
