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
import weekly_vodafone_db_billing


def stat_mt(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Vodafone'
    stat_category='billing'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    
    # topup amount
    
    key='topup_total_amount'
    
    db='billing'
    sql=r"""
    select sum(/*billing_egypt.dbo.ConvertCurrency(*/money/*,currency,'USD')*/)
    from billing_egypt.dbo.addvalue with(nolock)
    where time_addvalue between '%s' and '%s'
    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_float(config.conn_vodafone,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # topup unique user
    
    key='topup_unique_user'
    
    db='billing'
    sql=r"""
    select count(distinct user_id)
    from billing_egypt.dbo.addvalue with(nolock)
    where time_addvalue between '%s' and '%s'
    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    
    # topup times
    
    key='topup_times_total'
    
    db='billing'
    sql=r"""
    select count(distinct [index])
    from billing_egypt.dbo.addvalue with(nolock)
    where time_addvalue between '%s' and '%s'
    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # purchase amount
    
    key='purchase_total_amount'
    
    db='billing'
    sql=r"""
    select sum(/*billing_egypt.dbo.ConvertCurrency(*/money/*,currency,'USD')*/)
    from billing_egypt.dbo.payment with(nolock)
    where time_payment between '%s' and '%s'
    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_float(config.conn_vodafone,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # purchase unique user
    
    key='purchase_unique_user'
    
    db='billing'
    sql=r"""
    select count(distinct user_id)
    from billing_egypt.dbo.payment with(nolock)
    where time_payment between '%s' and '%s'
    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # purchase times total
    
    key='purchase_times_total'
    
    db='billing'
    sql=r"""
    select count(distinct [index])
    from billing_egypt.dbo.payment with(nolock)
    where time_payment between '%s' and '%s'
    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    


    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        
        my_date=time.time()-3600*24*i
        stat_mt(my_date)
        
        # weekly stat run on each Monday, over the last week from Monday to Sunday
        print my_date
        print 'week_day: '+str(helper_regex.get_weekday_from_time_stamp(my_date))
        
        if helper_regex.get_weekday_from_time_stamp(my_date)==7:
            weekly_vodafone_db_billing.stat_mt(my_date)
            pass
