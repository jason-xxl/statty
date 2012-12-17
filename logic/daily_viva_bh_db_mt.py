import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_viva_bh
import config


def stat_mt(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Viva_BH'
    stat_category='mt'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    
    # mt total
    
    key='mt_total'
    
    db='bahrain_mt'
    sql="select count(*) from [DB86].[bahrain_mt].dbo.charge_logs with(nolock) where createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva_bh,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # mt success
    
    key='mt_success'
    
    db='bahrain_mt'
    sql="select count(*) from [DB86].[bahrain_mt].dbo.charge_logs with(nolock) where status=1 and createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva_bh,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
   
    # sms
    
    key='mt_sms'
    
    db='bahrain_mt'
    sql="select count(*)  from [DB86].[bahrain_mt].dbo.charge_logs where status=1 and createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva_bh,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    

    # mt revenue
    key='mt_revenue'
    
    db='bahrain_mt'
    sql="select 0.5*count(*) from [DB86].[bahrain_mt].dbo.charge_logs where status=1 and createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva_bh,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_mt(my_date)
