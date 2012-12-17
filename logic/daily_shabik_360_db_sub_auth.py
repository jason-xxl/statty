import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_sub(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Shabik_360'
    stat_category='sub'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    

    ## auth
    
    db='stc_integral'
    sql='''
    
    select 
    count(*) as sub_auth_total,
    count(acc) as sub_auth_success,
    count(distinct acc) as sub_auth_unique
    from [shabik_mt].[dbo].[moweb_nologin_auth] with(nolock)
    where created_on>='%s' and created_on<'%s'

    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_mt,sql)
    print values

    key='sub_auth_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_shabik_360')
    key='sub_auth_success'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_shabik_360')
    key='sub_auth_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_shabik_360')


    # new sub and auth UV
    
    key='sub_auth_new_sub'
    
    db='shabik_mt'
    sql='''
    
    select count(distinct acc)
    from [shabik_mt].[dbo].[moweb_nologin_auth] a with(nolock)

    right join [shabik_mt].[dbo].[logs] b with(nolock)
    on '+966'+substring(a.acc,2,9)=b.msisdn

    where a.created_on>='%s' 
    and a.created_on<'%s' 
    and a.acc is not null 

    and b.CreatedOn>='%s' 
    and b.CreatedOn<'%s' 
    and b.action=10 

    ''' % (start_time,end_time,start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_shabik_360')


    # new sub and re-sub UV
    
    key='sub_auth_existing_sub'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['sub_auth_unique']-value,table_name='raw_data_shabik_360')
    

    # new sub and logined UV
    
    key='sub_auth_logined_new_sub'
    
    db='stc_integral'
    sql='''
    
    select count(user_id)
    from mozone_user.dbo.profile with(nolock)
    where user_name in (
        select distinct '0'+replace(msisdn,'+966','')+'@shabik.com' 
        from [DB86].[shabik_mt].[dbo].[logs] with(nolock)
        where CreatedOn >= '%s' AND CreatedOn < '%s' and action=10
    ) and user_name in (
        select distinct replace(acc,' ','')+'@shabik.com'
        from [DB86].[shabik_mt].[dbo].[moweb_nologin_auth] with(nolock)
        where created_on >= '%s' AND created_on < '%s' and acc is not null
    ) and lastLogin is not null

    ''' % (start_time,end_time,start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_stc,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_shabik_360')


    # existing sub and logined UV
    
    key='sub_auth_logined_existing_sub'
    
    db='stc_integral'
    sql='''
    
    select count(user_id)
    from mozone_user.dbo.profile with(nolock)
    where user_name in (
        select distinct replace(acc,' ','')+'@shabik.com'
        from [DB86].[shabik_mt].[dbo].[moweb_nologin_auth] with(nolock)
        where created_on >= '%s' AND created_on < '%s' and acc is not null
    ) and user_name not in (
        select distinct '0'+replace(msisdn,'+966','')+'@shabik.com' 
        from [DB86].[shabik_mt].[dbo].[logs] with(nolock)
        where CreatedOn >= '%s' AND CreatedOn < '%s' and action=10
    ) and lastLogin is not null

    ''' % (start_time,end_time,start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_stc,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name='raw_data_shabik_360')




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
