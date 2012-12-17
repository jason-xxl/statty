import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_sub(my_date, sub_user_total_use_real_time=False): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Zoota'
    stat_category='sub'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    
    """
    # sub action total
    
    key='sub_action_total'
    
    db='vivas_mt'
    sql="select count(*) from [vivas_mt].dbo.logs with(nolock) where action=2 and createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_zoota_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # unsub action total
    
    key='unsub_action_total'
    
    db='vivas_mt'
    sql="select count(*) from [vivas_mt].dbo.logs with(nolock) where action=6 and createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_zoota_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    """


    # sub users total
    
    key='sub_user_total'
    
    db='vivas_mt'

    sql="select count(*) from [vivas_mt].dbo.accounts with(nolock) where [subscription_type] in (1,2)" 

    """
    sql=r'''
    
    select top 1 NumOfSubscribers
    from [vivas_mt].[dbo].[num_of_subscribers]
    where [OfDate] > DATEADD(day,1,'%s')
    order by [OfDate] asc
    
    ''' % (date_today,)
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_zoota_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_zoota')


    # unsub users total
    
    key='unsub_user_total'
    
    db='vivas_mt'

    sql="select count(*) from [vivas_mt].dbo.accounts with(nolock) where [subscription_type] in (3,4)" 

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_zoota_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_zoota')
    




    # daily sub user
    
    key='daily_sub_user_unique'
    
    db='vivas_mt'

    sql=r'''
   

    SELECT count(distinct [msisdn])
    from [vivas_mt].[dbo].[logs] with(nolock)
    where [CreatedOn]>='%s' and [CreatedOn]<'%s'
    and action=2

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_zoota_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_zoota')



    # daily unsub user
    
    key='daily_unsub_user_unique'
    
    db='vivas_mt'

    sql=r'''
   

    SELECT count(distinct [msisdn])
    from [vivas_mt].[dbo].[logs] with(nolock)
    where [CreatedOn]>='%s' and [CreatedOn]<'%s'
    and action=6

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_zoota_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_zoota')


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
