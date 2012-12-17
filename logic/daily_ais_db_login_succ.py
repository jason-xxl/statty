import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_login(my_date): # run on 0:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='login_rate_only_ais'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    # total trials through header
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='trials_header'
    sql=r'''
    
    select count(client_id) from [ais_th_mt].dbo.moweb_nologin_auth 
    where x_msisdn is not null and createdon >='%s' and createdon<'%s'

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')

    # total trials through header with password filled 
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='trials_header_with_passwd'
    sql=r'''
    
    select count(client_id) from [ais_th_mt].dbo.moweb_nologin_auth 
    where x_msisdn is not null and createdon >='%s' and createdon<'%s' and passwd is not null

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')
    
    # total trials through header and successfully logged in through header
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='trials_header_logged_in'
    sql=r'''
    
    select count(a.client_id) from [ais_th_mt].dbo.moweb_nologin_auth as a inner join db132.mozone_user.dbo.profile as b on RTrim(a.x_msisdn)+'@fast_ais'=b.user_name 
    where a.x_msisdn is not null and a.createdon >='%s' and a.createdon<'%s' and b.lastLogin>a.createdon

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')
    
    # distinct clients through header
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='clients_header'
    sql=r'''
    
    select count(distinct client_id) from [ais_th_mt].dbo.moweb_nologin_auth 
    where x_msisdn is not null and createdon >='%s' and createdon<'%s'

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')

    # distinct clients who have successfully logged in through header
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='clients_header_logged_in'
    sql=r'''
    
    select count(distinct a.client_id) from [ais_th_mt].dbo.moweb_nologin_auth as a inner join db132.mozone_user.dbo.profile as b on RTrim(a.x_msisdn)+'@fast_ais'=b.user_name 
    where a.x_msisdn is not null and a.createdon >='%s' and a.createdon<'%s' and b.lastLogin>a.createdon

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')
    
    # total trials through sms
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='trials_sms'
    sql=r'''
    
    select count(client_id) from [ais_th_mt].dbo.moweb_nologin_auth 
    where x_msisdn is null and createdon >='%s' and createdon<'%s'

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')
    
    # total trials through sms with SMS received
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='trials_sms_received'
    sql=r'''
    
    select count(client_id) from [ais_th_mt].dbo.moweb_nologin_auth 
    where x_msisdn is null and createdon >='%s' and createdon<'%s' and acc is not null

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')

    # total trials through sms with account and password filled 
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='trials_sms_with_passwd'
    sql=r'''
    
    select count(client_id) from [ais_th_mt].dbo.moweb_nologin_auth 
    where x_msisdn is null and createdon >='%s' and createdon<'%s' and acc is not null and passwd is not null

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')

    # total trials through SMS and logged in through sms 
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='trials_sms_logged_in'
    sql=r'''
    
    select count(a.client_id) from [ais_th_mt].dbo.moweb_nologin_auth as a inner join db132.mozone_user.dbo.profile as b on RTrim(substring(a.acc,2,11))+'@fast_ais'=b.user_name 
    where a.x_msisdn is null and a.createdon >='%s' and a.createdon<'%s' and b.lastLogin>a.createdon

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')

    # distinct clients through sms 
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='clients_sms'
    sql=r'''
    
    select count(distinct client_id) from [ais_th_mt].dbo.moweb_nologin_auth 
    where x_msisdn is null and createdon >='%s' and createdon<'%s'

    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')

    # distinct clients who have successfully logged in through sms
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    key='clients_sms_logged_in'
    sql=r'''
    
    select count(distinct a.client_id) from [ais_th_mt].dbo.moweb_nologin_auth as a inner join db132.mozone_user.dbo.profile as b on RTrim(substring(a.acc,2,11))+'@fast_ais'=b.user_name 
    where a.x_msisdn is null and a.createdon >='%s' and a.createdon<'%s' and b.lastLogin>a.createdon
    
    '''  % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_ais')


if __name__=='__main__':

    for i in range(config.day_to_update_stat+3,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_login(my_date)
