import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config



def stat_register(my_date): # run on 0:00 a.m. , calculate yesterday's data

    oem_name='Zoota'
    stat_category='register'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')
    
    ###new registered users 1 day start###

    db=''
    key='user_new_registered_1_day_unique'
    sql=r'''
    
    select count(*) 
    from mozone_user.dbo.profile with(nolock) 
    where [creationDate]>= '%s'
    and [creationDate]<'%s'
    and version_tag='fast_vivas'
    and user_name not like '%%00000%%'
    
    '''%(start_time, end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_zoota,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_zoota')

    ###new registerd usesrs 1 day end###

    ###total registerd usesrs start###

    db=''
    key='user_total_registered_unique'
    sql=r'''
    
    select count(*) 
    from mozone_user.dbo.profile with(nolock) 
    where [creationDate]<'%s'
    and version_tag='fast_vivas'
    and user_name not like '%%00000%%'
   
    '''%end_time
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_zoota,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_zoota')

    ###total registerd usesrs start###

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_register(my_date)