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

    oem_name='Mozat'
    stat_category='sub_only_globe'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    start_time_1_day_before=helper_regex.time_floor(my_date-3600*24*1)


    # authenticate

    db=''

    key='agreement_page_visitor_user_unique'

    sql='''
    
    SELECT 
    distinct [real_cid],null
    FROM [globe_ph_mt].[dbo].[moweb_session]
    where auth_id is not null
    and [start_time]>='%s' and [start_time]<'%s'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_mt,sql,oem_name=oem_name, \
                                                               category=stat_category,key=key, \
                                                               table_name='raw_data',date=date_today)
    print value

    key='received_sms_user_unique'

    sql='''
    
    SELECT 
    distinct replace([acc],'+',''),null
    FROM [globe_ph_mt].[dbo].[moweb_session] with(nolock)
    where [start_time]>='%s' and [start_time]<'%s'
    and [sms_time] is not null

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_mt,sql,oem_name=oem_name, \
                                                               category=stat_category,key=key, \
                                                               table_name='raw_data',date=date_today)
    print value


    key='try_fetch_password_user_unique'

    sql='''
    
    SELECT 
    distinct [real_cid],null
    FROM [globe_ph_mt].[dbo].[moweb_session]
    where auth_id is not null
    and poll_time is not null 
    and [start_time]>='%s' and [start_time]<'%s'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_mt,sql,oem_name=oem_name, \
                                                               category=stat_category,key=key, \
                                                               table_name='raw_data',date=date_today)
    print value

    
    key='auth_succ_user_unique'

    sql='''
    
    SELECT 
    distinct replace([acc],'+',''),null
    FROM [globe_ph_mt].[dbo].[moweb_session] with(nolock)
    where [start_time]>='%s' and [start_time]<'%s'
    and [sms_time] is not null
    and [acc] is not null

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_mt,sql,oem_name=oem_name, \
                                                               category=stat_category,key=key, \
                                                               table_name='raw_data',date=date_today)
    print value





    # profile created, loginned


    db=''

    helper_sql_server.execute(config.conn_mozat,sql=r'''
    
    SELECT 
    distinct replace([acc],'+','')+'@fast_globe' as acc
    into #t
    FROM [DB86].[globe_ph_mt].[dbo].[moweb_session] a with(nolock)
    where [start_time]>='%s' and [start_time]<'%s'

    ''' % (start_time,end_time))


    key='auth_created_account_unique'
    sql='''

    select 
    
    replace(user_name,'@fast_globe','') as account,null

    from mozone_user.dbo.profile with(nolock) 
    where version_tag='fast_globe'
    and user_name not like '%00000%'
    and user_name in (
        select acc from #t
    )
    '''
    
    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_mozat,sql,oem_name=oem_name, \
                                                               category=stat_category,key=key, \
                                                               table_name='raw_data',date=date_today)
    print value


    key='auth_created_loginned_account_unique'
    sql='''

    select 
    
    replace(user_name,'@fast_globe','') as account,null

    from mozone_user.dbo.profile with(nolock) 
    where version_tag='fast_globe'
    and user_name not like '%00000%'
    and lastLogin is not null
    and user_name in (
        select acc from #t
    )

    '''
    
    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_mozat,sql,oem_name=oem_name, \
                                                               category=stat_category,key=key, \
                                                               table_name='raw_data',date=date_today)
    print value


    key='auth_account_changed_nickname_unique'
    sql='''

    select 
    
    replace(user_name,'@fast_globe','') as account,null

    from mozone_user.dbo.profile with(nolock) 
    where version_tag='fast_globe'
    and user_name not like '%00000%'
    and displayName not like 'F4%'
    and user_name in (
        select acc from #t
    )
    '''
    
    print 'SQL Server:'+sql
    value=element_count=helper_sql_server.fetch_dict_into_collection(config.conn_mozat,sql,oem_name=oem_name, \
                                                               category=stat_category,key=key, \
                                                               table_name='raw_data',date=date_today)
    print value

    helper_sql_server.execute(config.conn_mozat,sql=r'''
    drop table #t
    ''')
    
    
    return




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
