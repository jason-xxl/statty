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



def stat_finance(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Telk_Armor'
    stat_category='finance'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')


    #Total MT
    
    db=''
    key='mt_total'
    sql=r"""SELECT COUNT(*) AS total_MT FROM telkomsel_mt.dbo.charge_logs WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND not error LIKE 'Cancel %%'""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Total Success
    
    db=''
    key='mt_success'
    sql=r"""SELECT COUNT(*) AS total_success FROM telkomsel_mt.dbo.charge_logs WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND error LIKE '1:%%' """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #3:3:21 No enough credits
    
    db=''
    key='mt_not_enough_credit'
    sql=r"""SELECT COUNT(*) AS not_enough_credits FROM telkomsel_mt.dbo.charge_logs WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND error LIKE '3:3:21%%' """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    
    #3:107 Failed charging
    
    db=''
    key='mt_charge_failed'
    sql=r"""SELECT COUNT(*) AS failed_charging FROM telkomsel_mt.dbo.charge_logs WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND error LIKE '3:107%%' """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #3:999 timeout
    
    db=''
    key='mt_timeout'
    sql=r"""SELECT COUNT(*) AS timeout FROM telkomsel_mt.dbo.charge_logs WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND error LIKE '3:999%%' """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #5 Not subscriber
    
    db=''
    key='mt_not_in_sub'
    sql=r"""SELECT COUNT(*) AS not_subscriber FROM telkomsel_mt.dbo.charge_logs WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND error LIKE '5%%' """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #3:6:3:21 quarantined
    
    db=''
    key='mt_quarantined'
    sql=r"""SELECT COUNT(*) AS quarantined FROM telkomsel_mt.dbo.charge_logs WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND error LIKE '3:6:3:21%%' """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #0:999 Internal Problem
    
    db=''
    key='mt_internal_error'
    sql=r"""SELECT COUNT(*) AS internal_problem FROM telkomsel_mt.dbo.charge_logs WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND error LIKE '0:999%%' """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    
    
    
    






    
    #MT Daily (startDate and endDate)
    
    db=''
    key='mt_'
    sql=r"""SELECT COUNT(*) AS MT_daily FROM [telkomsel_mt].[dbo].[charge_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND sub_type=1 AND NOT error LIKE 'Cancel %%' AND NOT error LIKE '5'""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #MT Daily Succeed (startDate and endDate)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS MT_daily_succeed FROM [telkomsel_mt].[dbo].[charge_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND sub_type=1 AND error LIKE '1:%%'""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Daily Unsub (startDate and endDate)
    #The same as Page 1: User  Daily unsubscription: 
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS daily_unsubscription FROM [telkomsel_mt].[dbo].[sms_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND (UPPER(msg) LIKE 'UNREG%%' OR UPPER(msg) LIKE 'OFF%%' OR UPPER(msg) LIKE 'STOP%%') AND msisdn IN (SELECT msisdn FROM [telkomsel_mt].[dbo].[accounts] WHERE subscription_type=1 )""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    
    #MT Weekly (startDate and endDate)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS MT_weekly FROM [telkomsel_mt].[dbo].[charge_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND sub_type=2 AND NOT error LIKE 'Cancel %%' AND NOT error LIKE '5'""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #MT Weekly Succeed (startDate and endDate)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS MT_weekly_succeed FROM [telkomsel_mt].[dbo].[charge_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND sub_type=2 AND error LIKE '1:%%' """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Weekly Unsub (startDate and endDate)
    #The same as Page 1: User  Weekly unsubscription: 
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS weekly_unsubscription FROM [telkomsel_mt].[dbo].[sms_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND  (UPPER(msg) LIKE 'UNREG%%' OR UPPER(msg) LIKE 'OFF%%' OR UPPER(msg) LIKE 'STOP%%') AND msisdn IN (SELECT msisdn FROM [telkomsel_mt].[dbo].[accounts] WHERE subscription_type=2 )""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    
    #MT Monthly (startDate and endDate)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS MT_monly FROM [telkomsel_mt].[dbo].[charge_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND sub_type=3 AND NOT error LIKE 'Cancel %%' AND NOT error LIKE '5'""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #MT Monthly Succeed (startDate and endDate)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS MT_monly_succeed FROM [telkomsel_mt].[dbo].[charge_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND sub_type=3 AND error LIKE '1:%%'""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Monthly Unsub (startDate and endDate)
    #The same as Page 1: User  Monthly unsubscription: 
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS monthly_unsubscription FROM [telkomsel_mt].[dbo].[sms_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND  (UPPER(msg) LIKE 'UNREG%%' OR UPPER(msg) LIKE 'OFF%%' OR UPPER(msg) LIKE 'STOP%%') AND msisdn IN (SELECT msisdn FROM [telkomsel_mt].[dbo].[accounts] WHERE subscription_type=3 )""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    








    
    #Total Users Now: (No parameters)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS total_user FROM [telkomsel_mt].[dbo].[accounts] WHERE is_deleted=0""" # % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Total Effective Subscribers: (No parameters)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS total_effective_subscribers FROM [telkomsel_mt].[dbo].[accounts] WHERE is_deleted=0 AND is_disabled=0""" # % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Effective Daily Subscribers: (No parameters)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS effective_daily_subscribers FROM [telkomsel_mt].[dbo].[accounts] WHERE is_deleted=0 AND is_disabled=0 AND subscription_type=1 """ #% (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Effective Weekly Subscribers: (No parameters)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS effective_weekly_subscribers FROM [telkomsel_mt].[dbo].[accounts] WHERE is_deleted=0 AND is_disabled=0 AND subscription_type=2 """ #% (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Effective Monthly Subscribers: (No parameters)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS effective_monthly_subscribers FROM [telkomsel_mt].[dbo].[accounts] WHERE is_deleted=0 AND is_disabled=0 AND subscription_type=3""" #% (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #New daily subscriptions: (startDate and endDate) 
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS new_daily_subscriptions FROM [telkomsel_mt].[dbo].[sms_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND  UPPER(msg) LIKE 'REG%%' AND msisdn IN (SELECT msisdn FROM [telkomsel_mt].[dbo].[accounts] WHERE subscription_type=1 )""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Daily unsubscription: (startDate and endDate)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS daily_unsubscription FROM [telkomsel_mt].[dbo].[sms_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND (UPPER(msg) LIKE 'UNREG%%' OR UPPER(msg) LIKE 'OFF%%' OR UPPER(msg) LIKE 'STOP%%') AND msisdn IN (SELECT msisdn FROM [telkomsel_mt].[dbo].[accounts] WHERE subscription_type=1 )""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #New weekly subscriptions: (startDate and endDate) 
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS new_weekly_subscriptions FROM [telkomsel_mt].[dbo].[sms_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND  UPPER(msg) LIKE 'REG%%' AND msisdn IN (SELECT msisdn FROM [telkomsel_mt].[dbo].[accounts] WHERE subscription_type=2 )""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Weekly unsubscription: (startDate and endDate)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS weekly_unsubscription FROM [telkomsel_mt].[dbo].[sms_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND  (UPPER(msg) LIKE 'UNREG%%' OR UPPER(msg) LIKE 'OFF%%' OR UPPER(msg) LIKE 'STOP%%') AND msisdn IN (SELECT msisdn FROM [telkomsel_mt].[dbo].[accounts] WHERE subscription_type=2 )""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #New monthly subscriptions: (startDate and endDate) 
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS new_monthly_subscriptions FROM [telkomsel_mt].[dbo].[sms_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND UPPER(msg) LIKE 'REG%%' AND msisdn IN (SELECT msisdn FROM [telkomsel_mt].[dbo].[accounts] WHERE subscription_type=3 )""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    #Monthly unsubscription: (startDate and endDate)
    
    db=''
    key=''
    sql=r"""SELECT COUNT(*) AS monthly_unsubscription FROM [telkomsel_mt].[dbo].[sms_logs] WHERE  CreatedOn>='%s' AND CreatedOn<'%s' AND  (UPPER(msg) LIKE 'UNREG%%' OR UPPER(msg) LIKE 'OFF%%' OR UPPER(msg) LIKE 'STOP%%') AND msisdn IN (SELECT msisdn FROM [telkomsel_mt].[dbo].[accounts] WHERE subscription_type=3 )""" % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    



    
    
    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat_finance(my_date)
        #time.sleep(10)
