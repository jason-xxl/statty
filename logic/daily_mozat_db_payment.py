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


def stat_billing(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='payment'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    
    # payment amount by item
    
    key='payment_ammount_by_item'
    
    db='billing'

    sql=r"""
    select item_name, sum(billing.dbo.ConvertCurrency(money,currency,'USD')) as amount
    from billing.dbo.payment with(nolock)
    where time_payment between '%s' and '%s' group by item_name order by item_name
    """ % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_mozat,sql)
    
    for item_name,money_sum in values.iteritems():
        print item_name,money_sum
        helper_mysql.put_raw_data(oem_name,stat_category,key,item_name+'_'+date_today,money_sum)

               
    # payment unique user by item
    
    key='payment_unique_user_by_item'
    
    db='billing'
    sql=r"""
    select item_name, count(distinct user_id) as amount
    from billing.dbo.payment with(nolock)
    where time_payment between '%s' and '%s' group by item_name order by item_name
    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_mozat,sql)
    
    for item_name,money_sum in values.iteritems():
        print item_name,money_sum
        helper_mysql.put_raw_data(oem_name,stat_category,key,item_name+'_'+date_today,money_sum)


               
    # payment count by item
    
    key='payment_count_by_item'
    
    db='billing'
    sql=r"""
    select item_name, count(distinct [index]) as amount
    from billing.dbo.payment with(nolock)
    where time_payment between '%s' and '%s' group by item_name order by item_name
    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_mozat,sql)
    
    for item_name,money_sum in values.iteritems():
        print item_name,money_sum
        helper_mysql.put_raw_data(oem_name,stat_category,key,item_name+'_'+date_today,money_sum)



    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        
        my_date=time.time()-3600*24*i
        stat_billing(my_date)
        
