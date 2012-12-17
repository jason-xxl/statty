import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_math
import config


def stat_login(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Vodafone'
    stat_category='login'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    date_today=start_time.replace(' 06:00:00','')

    # login 1 day
    
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')

    db='shabik_mt'
    key='user_last_login_1_day_unique'

    sql=r'''
    
    SELECT '20'+replace([user_name],'@voda_egypt',''),null
      FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [lastLogin]>=DATEADD(day,-1,Getdate())
    and [user_name] like '%%@voda_egypt'
    and [user_name] not like '%%000000%%'
    and [user_name] not like '%%motest%%'
    --and len('20'+replace([user_name],'@voda_egypt',''))>=12

    ''' 

    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_vodafone_88,sql,oem_name=oem_name, \
                                                               category=stat_category,key='user_last_login_1_day_unique', \
                                                               table_name='raw_data',date=date_today)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # login 7 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 06:00:00')

    db='shabik_mt'
    key='user_last_login_7_day_unique'
    sql=r'''
    
    SELECT '20'+replace([user_name],'@voda_egypt',''),null
      FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [lastLogin]>=DATEADD(day,-7,Getdate())
    and [user_name] like '%%@voda_egypt'
    and [user_name] not like '%%000000%%'
    and [user_name] not like '%%motest%%'
    --and len('20'+replace([user_name],'@voda_egypt',''))>=12

    ''' 

    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_vodafone_88,sql,oem_name=oem_name, \
                                                               category=stat_category,key='user_last_login_7_day_unique', \
                                                               table_name='raw_data',date=date_today)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # login 30 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*29).replace(' 00:00:00',' 06:00:00')

    db='shabik_mt'
    key='user_last_login_30_day_unique'
    sql=r'''
    
    SELECT '20'+replace([user_name],'@voda_egypt',''),null
      FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [lastLogin]>=DATEADD(day,-30,Getdate())
    and [user_name] like '%%@voda_egypt'
    and [user_name] not like '%%000000%%'
    and [user_name] not like '%%motest%%'
    --and len('20'+replace([user_name],'@voda_egypt',''))>=12

    ''' 

    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_vodafone_88,sql,oem_name=oem_name, \
                                                               category=stat_category,key='user_last_login_30_day_unique', \
                                                               table_name='raw_data',date=date_today)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_login(my_date)
