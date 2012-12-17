import helper_sql_server
import helper_mysql
import helper_math
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_sub(my_date): # run on 0:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='sub_only_umobile'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 00:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 00:00:00')
    date_today=start_time.replace(' 00:00:00','')

    # sub users total
    
    key='sub_user_total'
    
    db='umobile_mt'
    sql="select count(*) from [umobile_mt].dbo.accounts with(nolock) where is_deleted=0 and CreatedOn <='"+date_today+"'"
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_umobile')
    
    # unsub users total
    
    key='unsub_user_total'
    
    #sql="select count(*) from [umobile_mt].dbo.accounts with(nolock) where is_deleted=1"
    sql="select count(*) from umobile_mt.dbo.accounts where is_deleted = 1 and deletedon is not null and deletedon <='"+date_today+"'"
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_umobile')
    
if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
