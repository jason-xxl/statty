import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
from datetime import date
import time
import helper_regex
import config
import weekly_zoota_db_billing


def stat_mt(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Zoota'
    stat_category='billing'

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_zoota)
    #exec('from user_id_filter.user_id_filter_zoota_'+current_date.replace('-','_')+' import get_filtered_dict')

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    
    # topup amount
    
    key='topup_total_amount'
    
    db='billing'
    sql=r'''

    select user_id,sum(money) 
    from billing.dbo.addvalue with(nolock) 
    where time_addvalue between '%s' and '%s'
    group by user_id

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_zoota_mt,sql)
    #values=get_filtered_dict(values)
    print sum(values.values())
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,sum(values.values()),table_name='raw_data_zoota')


    
    # topup unique user
    
    key='topup_unique_user'
    
    print len(values)
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,len(values),table_name='raw_data_zoota')


    
    # topup times
    
    key='topup_times_total'
    
    db='billing'
    sql=r'''
    
    select user_id,count(distinct [index]) 
    from billing.dbo.addvalue with(nolock) 
    where time_addvalue between '%s' and '%s'
    group by user_id
    
    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_zoota_mt,sql)
    #values=get_filtered_dict(values)
    print sum(values.values())
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,sum(values.values()),table_name='raw_data_zoota')




    # purchase amount
    
    key='purchase_total_amount'
    
    db='billing'
    sql=r'''

    select user_id,sum(money) 
    from billing.dbo.payment with(nolock) 
    where time_payment between '%s' and '%s'
    group by user_id

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_zoota_mt,sql)
    #values=get_filtered_dict(values)
    print sum(values.values())
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,sum(values.values()),table_name='raw_data_zoota')

    

    # purchase unique user
    
    key='purchase_unique_user'
    
    print len(values)
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,len(values),table_name='raw_data_zoota')


    
    # purchase times total
    
    key='purchase_times_total'
    
    db='billing'
    sql=r'''
    
    select user_id,count(distinct [index]) 
    from billing.dbo.payment with(nolock) 
    where time_payment between '%s' and '%s'
    group by user_id

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_zoota_mt,sql)
    #values=get_filtered_dict(values)
    print sum(values.values())
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,sum(values.values()),table_name='raw_data_zoota')
    


    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        
        my_date=time.time()-3600*24*i
        stat_mt(my_date)
        
        # weekly stat run on each Monday, over the last week from Monday to Sunday
        print my_date
        print 'week_day: '+str(helper_regex.get_weekday_from_time_stamp(my_date))
        
        if helper_regex.get_weekday_from_time_stamp(my_date)==7:
            weekly_zoota_db_billing.stat_mt(my_date)
