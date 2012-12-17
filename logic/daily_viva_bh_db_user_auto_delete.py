import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config



def stat(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Viva_BH'
    stat_category='project_user_auto_delete'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')

    # deleted total
    
    db=''
    key='daily_deleted_user_unique_total'
    sql=r'''
    select count(distinct msisdn) from [bahrain_mt].dbo.logs where action=60 and createdon>='%s' and createdon<'%s'
    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat(my_date)
