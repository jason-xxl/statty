import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_mt(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Telk_Armor'
    stat_category='mt'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')
    
    
    # daily mt unique user
    
    key='daily_payment_unique_user'
    
    db='telkomsel_mt'
    sql=r'''
    
    select count(distinct msisdn)
    from (
        SELECT msisdn
          FROM [telkomsel_mt].[dbo].[logs] with(nolock)
        where CreatedOn < '%s'
        group by msisdn
        having max(id*100+[action])-max(id*100) in (1,10)

        and msisdn not in (
            SELECT [msisdn]
            FROM [telkomsel_mt].[dbo].[accounts] with(nolock)
            where [is_deleted]=1 
            or [is_disabled]=1
        )
    ) a

    ''' % (end_time,)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # weekly mt unique user
    
    key='weekly_payment_unique_user'
    
    db='telkomsel_mt'
    sql=r'''
    
    select count(distinct msisdn)
    from (
        SELECT msisdn
          FROM [telkomsel_mt].[dbo].[logs] with(nolock)
        where CreatedOn < '%s'
        group by msisdn
        having max(id*100+[action])-max(id*100) in (2,11)

        and msisdn not in (
            SELECT [msisdn]
            FROM [telkomsel_mt].[dbo].[accounts] with(nolock)
            where [is_deleted]=1 
            or [is_disabled]=1
        )
    ) a

    ''' % (end_time,)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # monthly mt unique user
    
    key='monthly_payment_unique_user'
    
    db='telkomsel_mt'
    sql=r'''
    
    select count(distinct msisdn)
    from (
        SELECT msisdn
          FROM [telkomsel_mt].[dbo].[logs] with(nolock)
        where CreatedOn < '%s'
        group by msisdn
        having max(id*100+[action])-max(id*100) in (3,12)

        and msisdn not in (
            SELECT [msisdn]
            FROM [telkomsel_mt].[dbo].[accounts] with(nolock)
            where [is_deleted]=1 
            or [is_disabled]=1
        )
    ) a

    ''' % (end_time,)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    

    # net increment daily mt unique user
    
    key='daily_payment_unique_user_net_increment'
    
    db='telkomsel_mt'
    sql=r'''
    
    select count(distinct msisdn)
    from (
        SELECT msisdn
          FROM [telkomsel_mt].[dbo].[logs] with(nolock)
        where CreatedOn < '%s'
        group by msisdn
        having max(id*100+[action])-max(id*100) in (1,10)

        and msisdn not in (
            SELECT msisdn
              FROM [telkomsel_mt].[dbo].[logs] with(nolock)
            where CreatedOn < '%s'
            group by msisdn
            having max(id*100+[action])-max(id*100) in (1,10)
        )

        and msisdn not in (
            SELECT [msisdn]
            FROM [telkomsel_mt].[dbo].[accounts] with(nolock)
            where [is_deleted]=1 
            or [is_disabled]=1
        )
    ) a

    ''' % (end_time,start_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # net increment weekly mt unique user
    
    key='weekly_payment_unique_user_net_increment'
    
    db='telkomsel_mt'
    sql=r'''
    
    select count(distinct msisdn)
    from (
        SELECT msisdn
          FROM [telkomsel_mt].[dbo].[logs] with(nolock)
        where CreatedOn < '%s'
        group by msisdn
        having max(id*100+[action])-max(id*100) in (2,11)

        and msisdn not in (
            SELECT msisdn
              FROM [telkomsel_mt].[dbo].[logs] with(nolock)
            where CreatedOn < '%s'
            group by msisdn
            having max(id*100+[action])-max(id*100) in (2,11)
        )

        and msisdn not in (
            SELECT [msisdn]
            FROM [telkomsel_mt].[dbo].[accounts] with(nolock)
            where [is_deleted]=1 
            or [is_disabled]=1
        )
    ) a

    ''' % (end_time,start_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # net increment monthly mt unique user
    
    key='monthly_payment_unique_user_net_increment'
    
    db='telkomsel_mt'
    sql=r'''
    
    select count(distinct msisdn)
    from (
        SELECT msisdn
          FROM [telkomsel_mt].[dbo].[logs] with(nolock)
        where CreatedOn < '%s'
        group by msisdn
        having max(id*100+[action])-max(id*100) in (3,12)

        and msisdn not in (
            SELECT msisdn
              FROM [telkomsel_mt].[dbo].[logs] with(nolock)
            where CreatedOn < '%s'
            group by msisdn
            having max(id*100+[action])-max(id*100) in (3,12)
        )

        and msisdn not in (
            SELECT [msisdn]
            FROM [telkomsel_mt].[dbo].[accounts] with(nolock)
            where [is_deleted]=1 
            or [is_disabled]=1
        )
    ) a

    ''' % (end_time,start_time)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)






    # mt daily succeeded
    
    key='daily_payment_mt_succeeded'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS MT_daily
    FROM [telkomsel_mt].[dbo].[charge_logs]
    WHERE CreatedOn>='%s' AND CreatedOn<'%s'
    AND sub_type=1
    AND error LIKE '1:%%' 

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # mt daily total
    
    key='daily_payment_mt_total'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS MT_daily
    FROM [telkomsel_mt].[dbo].[charge_logs]
    WHERE CreatedOn>='%s' AND CreatedOn<'%s'
    AND sub_type=1
    AND NOT error LIKE 'Cancel %%' AND NOT error LIKE '5'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # mt daily failure of no enough credits
    
    key='daily_payment_mt_failed_no_enough_credits'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS not_enough_credits 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '3:3:21%%' 
    And sub_type=1 

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_daily_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # mt daily failure of failed charging
    
    key='daily_payment_mt_failed_failed_charging'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS failed_charging 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND (error LIKE '3:107%%' or error LIKE '3:101%%')
    And sub_type=1

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_daily_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # mt daily failure of timeout
    
    key='daily_payment_mt_failed_timeout'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS timeout 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '3:999%%' 
    And sub_type=1

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_daily_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # mt daily failure of not subscriber
    
    key='daily_payment_mt_failed_not_subscriber'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS not_subscriber 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '5%%' 
    And sub_type=1

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_daily_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # mt daily failure of quarantined
    
    key='daily_payment_mt_failed_quarantined'
    
    db='telkomsel_mt'
    sql=r'''
        
    SELECT COUNT(*) AS quarantined 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '3:6:3:21%%' 
    And sub_type=1

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_daily_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # mt daily failure of internal problem
    
    key='daily_payment_mt_failed_internal_problem'
    
    db='telkomsel_mt'
    sql=r'''
        
    SELECT COUNT(*) AS internal_problem 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '0:999%%' 
    And sub_type=1

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_daily_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)








    # mt weekly total
    
    key='weekly_payment_mt_total'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS MT_daily
    FROM [telkomsel_mt].[dbo].[charge_logs]
    WHERE CreatedOn>='%s' AND CreatedOn<'%s'
    AND sub_type=2
    AND NOT error LIKE 'Cancel %%' AND NOT error LIKE '5'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # mt weekly succeeded
    
    key='weekly_payment_mt_succeeded'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS MT_daily_succeed
    FROM [telkomsel_mt].[dbo].[charge_logs]
    WHERE CreatedOn>='%s' AND CreatedOn<'%s'
    AND sub_type=2 AND error LIKE '1:%%'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_weekly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)





    # mt weekly failure of no enough credits
    
    key='weekly_payment_mt_failed_no_enough_credits'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS not_enough_credits 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '3:3:21%%' 
    And sub_type=2 

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_weekly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # mt weekly failure of failed charging
    
    key='weekly_payment_mt_failed_failed_charging'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS failed_charging 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND (error LIKE '3:107%%' or error LIKE '3:101%%')
    And sub_type=2

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_weekly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # mt weekly failure of timeout
    
    key='weekly_payment_mt_failed_timeout'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS timeout 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '3:999%%' 
    And sub_type=2

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_weekly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # mt weekly failure of not subscriber
    
    key='weekly_payment_mt_failed_not_subscriber'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS not_subscriber 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '5%%' 
    And sub_type=2

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_weekly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # mt weekly failure of quarantined
    
    key='weekly_payment_mt_failed_quarantined'
    
    db='telkomsel_mt'
    sql=r'''
        
    SELECT COUNT(*) AS quarantined 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '3:6:3:21%%' 
    And sub_type=2

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_weekly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # mt weekly failure of internal problem
    
    key='weekly_payment_mt_failed_internal_problem'
    
    db='telkomsel_mt'
    sql=r'''
        
    SELECT COUNT(*) AS internal_problem 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '0:999%%' 
    And sub_type=2

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_weekly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)







    # mt monthly total
    
    key='monthly_payment_mt_total'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS MT_daily
    FROM [telkomsel_mt].[dbo].[charge_logs]
    WHERE CreatedOn>='%s' AND CreatedOn<'%s'
    AND sub_type=3
    AND NOT error LIKE 'Cancel %%' AND NOT error LIKE '5'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # mt monthly succeeded
    
    key='monthly_payment_mt_succeeded'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS MT_daily_succeed
    FROM [telkomsel_mt].[dbo].[charge_logs]
    WHERE CreatedOn>='%s' AND CreatedOn<'%s'
    AND sub_type=3 AND error LIKE '1:%%'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_monthly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)







    # mt monthly failure of no enough credits
    
    key='monthly_payment_mt_failed_no_enough_credits'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS not_enough_credits 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '3:3:21%%' 
    And sub_type=3 

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_monthly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # mt monthly failure of failed charging
    
    key='monthly_payment_mt_failed_failed_charging'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS failed_charging 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND (error LIKE '3:107%%' or error LIKE '3:101%%')
    And sub_type=3

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_monthly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # mt monthly failure of timeout
    
    key='monthly_payment_mt_failed_timeout'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS timeout 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '3:999%%' 
    And sub_type=3

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_monthly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # mt monthly failure of not subscriber
    
    key='monthly_payment_mt_failed_not_subscriber'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT COUNT(*) AS not_subscriber 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '5%%' 
    And sub_type=3

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_monthly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # mt monthly failure of quarantined
    
    key='monthly_payment_mt_failed_quarantined'
    
    db='telkomsel_mt'
    sql=r'''
        
    SELECT COUNT(*) AS quarantined 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '3:6:3:21%%' 
    And sub_type=3

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_monthly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)




    # mt monthly failure of internal problem
    
    key='monthly_payment_mt_failed_internal_problem'
    
    db='telkomsel_mt'
    sql=r'''
        
    SELECT COUNT(*) AS internal_problem 
    FROM telkomsel_mt.dbo.charge_logs with(nolock) 
    WHERE CreatedOn>='%s' AND CreatedOn<'%s' 
    AND error LIKE '0:999%%' 
    And sub_type=3

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    mt_monthly_succeeded=value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    





    # daily total user including unsub
    
    key='daily_total_unique_user_with_unsub'
    
    db='telkomsel_mt'
    sql=r'''
    
    SELECT count(distinct msisdn)
    FROM [telkomsel_mt].[dbo].[logs] with(nolock)
    where CreatedOn < '%s'

    ''' % (end_time,)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)






    '''
    # daily mt revenue
    key='daily_mt_revenue_in_operator_currency'
    
    db='telkomsel_mt'
    value=mt_daily_succeeded*1100+mt_weekly_succeeded*6600+mt_monthly_succeeded*27500

    print 'daily_mt_revenue_in_operator_currency:'+str(value)
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    '''
    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_mt(my_date)
