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
    stat_category='login_only_umobile'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')


    # register 1 day
    

    db=''

    sql=r'''
    
    select 
    count(*) as new_user
    ,sum(case when lastLogin is not null then 1 else 0 end) as logined_user

    from [mozone_user].[dbo].[Profile] with(nolock)
    where [creationDate]>='%s'
    and [creationDate]<'%s'
    and version_tag='fast_umobile'
    and user_name not like '%%00000%%'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_mozat,sql)

    print values

    key='user_register_1_day_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['new_user'])

    key='user_register_logined_1_day_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['logined_user'])




if __name__=='__main__':
    
    #track 5 day to see whether user succeeded to login

    for i in range(config.day_to_update_stat+4,0,-1):
        my_date=time.time()-3600*24*i
        stat_login(my_date)
