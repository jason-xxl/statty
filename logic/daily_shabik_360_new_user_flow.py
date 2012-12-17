import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
from datetime import date, datetime, timedelta

config.collection_cache_enabled=True

def format_msisdn(user_name):
    user_name=str(user_name.strip().lstrip('+0'))
    if user_name[0:3]=='966':
        return user_name
    return '966'+user_name


def stat(my_date):
    
    #raise Exception('expired logic')

    oem_name='Shabik_360'
    stat_category='new_user'
    db_name='raw_data_device_shabik_360'
    db_name_original='raw_data_device_shabik_360'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
 

    user_sets={}
    filters={}


    # succeeded download msisdn

    collection_succeeded_download=helper_mysql.get_raw_collection_from_key(oem_name=oem_name,category='website', \
                                    key='filtered_10.file-type-end_6.done_daily_msisdn_unique',sub_key='client-end-file_Completed', \
                                    date=date_today,table_name='raw_data_device_shabik_360')
    
    collection_succeeded_download=set([format_msisdn(i) for i in collection_succeeded_download])
    print len(collection_succeeded_download)
    
    filters['daily_succeeded_download']=collection_succeeded_download


    # sub msisdn

    key='daily_sub_msisdn_total_unique'
    db='stc_integral'

    sql=r'''

    select distinct msisdn,null
    from shabik_mt.dbo.logs with(nolock)
    where CreatedOn>='%s' and CreatedOn<'%s'
    and action=10

    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    collection_sub=helper_sql_server.fetch_dict(conn_config=config.conn_stc_mt,sql=sql)
    collection_sub=set([format_msisdn(i) for i in collection_sub])

    helper_mysql.put_collection(collection=collection_sub,oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    filters['daily_sub']=collection_sub

    
    # total auth

    sql='''
    
    select 
    distinct acc as sub_auth_success,null
    from [shabik_mt].[dbo].[moweb_nologin_auth] with(nolock)
    where created_on>='%s' and created_on<'%s'

    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_mt,sql)

    values=[format_msisdn(i) for i in values.keys()]
    filters['daily_successful_auth']=set(values)



    # first sub msisdn

    key='daily_first_sub_msisdn_total_unique'
    db='stc_integral'

    sql=r'''

    select msisdn,null
    from shabik_mt.dbo.logs with(nolock)
    where CreatedOn<'%s'
    group by msisdn
    having min(CreatedOn)>='%s'

    ''' % (end_time,start_time)
    
    print 'SQL Server:'+sql
    collection_first_sub=helper_sql_server.fetch_dict(conn_config=config.conn_stc_mt,sql=sql)
    collection_first_sub=set([format_msisdn(i) for i in collection_first_sub.keys()])

    helper_mysql.put_collection(collection=collection_first_sub,oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    filters['daily_first_sub']=collection_first_sub


    # finally unsub msisdn

    key='daily_finally_unsub_msisdn_total_unique'
    db='stc_integral'

    sql=r'''

    select msisdn,null
    from shabik_mt.dbo.logs with(nolock)
    where CreatedOn>='%s' and CreatedOn<'%s'
    group by msisdn
    having max(id*100+[action])-max(id*100)=11

    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    collection_finally_unsub=helper_sql_server.fetch_dict(conn_config=config.conn_stc_mt,sql=sql)
    collection_finally_unsub=set([format_msisdn(i) for i in collection_finally_unsub.keys()])

    helper_mysql.put_collection(collection=collection_finally_unsub,oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    user_sets[key]=collection_finally_unsub




    

    # download msisdn 360
    
    initial_date='2012-01-01'
    key='daily_download_from_mshabiksa_total_msisdn_unique'
    collection_download=helper_mysql.get_raw_collection_from_key_date_range(oem_name=oem_name,category='website', \
                                    key='filtered_sms_broadcast_daily_msisdn_unique',sub_key='', \
                                    begin_date=initial_date,end_date=date_today,table_name='raw_data_device_shabik_360')
    
    collection_download=set([format_msisdn(i) for i in collection_download])
    print 'daily_download_from_mshabiksa_total_msisdn_unique:',len(collection_download)
    
    user_sets[key]=collection_download

    

    # download redirected msisdn 360
    
    initial_date='2012-01-01'
    key='daily_download_from_mshabiksa_redirected_total_msisdn_unique'
    collection_download=helper_mysql.get_raw_collection_from_key_date_range(oem_name=oem_name,category='website', \
                                    key='filtered_sms_broadcast_directed_daily_msisdn_unique',sub_key='', \
                                    begin_date=initial_date,end_date=date_today,table_name='raw_data_device_shabik_360')
    
    collection_download=set([format_msisdn(i) for i in collection_download])
    print 'daily_download_from_mshabiksa_redirected_total_msisdn_unique:',len(collection_download)
    
    user_sets[key]=collection_download

    user_sets['daily_download_from_mshabiksa_redirected_total_msisdn_unique'] &= user_sets['daily_download_from_mshabiksa_total_msisdn_unique']





    # succeeded download msisdn 360

    initial_date='2012-01-01'
    key='daily_succeeded_download_total_msisdn_unique'
    collection_succeeded_download=helper_mysql.get_raw_collection_from_key_date_range(oem_name=oem_name,category='website', \
                                    key='filtered_10.file-type-end_6.done_daily_msisdn_unique',sub_key='client-end-file_Completed', \
                                    begin_date=initial_date,end_date=date_today,table_name='raw_data_device_shabik_360')
    
    collection_succeeded_download=set([format_msisdn(i) for i in collection_succeeded_download])
    print len(collection_succeeded_download)
    
    user_sets[key]=collection_succeeded_download








    # profile created

    key='daily_created_total_profile_unique'
    sql=r'''

    select 
    distinct replace(user_name,'@shabik.com','') as msisdn
    ,case when lastLogin is not null then 1 else 0 end as logined
    from mozone_user.dbo.profile with(nolock)
    where CreationDate>='%s' and CreationDate<'%s'
    and user_name like '%%@shabik.com'

    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(conn_config=config.conn_stc,sql=sql)
    values=dict((format_msisdn(k),v) for k,v in values.iteritems())
    print len(values)
    user_sets[key]=set(values.keys())

    


    # profile logined

    key='daily_logined_created_total_profile_unique'
    values=dict((k,v) for k,v in values.iteritems() if v==1)
    user_sets[key]=set(values.keys())
    
    
    

    # login svc uv

    key='daily_login_service_total_unique'
    collection_login_service=helper_mysql.get_raw_collection_from_key(oem_name='STC',category='login_service', \
                                    key='login_response_daily_result_type_peer_id_unique',sub_key='0', \
                                    date=date_today,table_name='raw_data')

    collection_login_service=helper_sql_server.fetch_dict_map_to_collection(collection_login_service,sql_template=r'''
        select 
        user_id
        ,replace(user_name,'@shabik.com','') as msisdn
        from mozone_user.dbo.profile with(nolock) 
        where user_id in (%s)
    ''',conn_config=config.conn_stc,step=1000)

    print key,len(collection_login_service.keys()),len(set(collection_login_service.values()))

    user_sets[key]=set([format_msisdn(i) for i in collection_login_service.values()])

    
    

    # moagent 360 uv

    key='daily_moagent_360_total_unique'
    collection_moagent=helper_mysql.get_raw_collection_from_key(oem_name=oem_name,category='moagent', \
                                    key='app_page_daily_visitor_unique',sub_key='', \
                                    date=date_today,table_name='raw_data_shabik_360')

    collection_moagent=helper_sql_server.fetch_dict_map_to_collection(collection_moagent,sql_template=r'''
        select 
        user_id
        ,replace(user_name,'@shabik.com','') as msisdn
        from mozone_user.dbo.profile with(nolock) 
        where user_id in (%s)
    ''',conn_config=config.conn_stc,step=1000)

    print key,len(collection_moagent.keys()),len(set(collection_moagent.values()))

    user_sets[key]=set([format_msisdn(i) for i in collection_moagent.values()])



    # total moagent 360 home page uv

    key='daily_moagent_360_homepage_total_unique'
    collection_moagent_homepage=helper_mysql.get_raw_collection_from_key(oem_name=oem_name,category='moagent', \
                                    key='app_page_by_app_daily_visitor_unique',sub_key='homepage', \
                                    date=date_today,table_name='raw_data_shabik_360')
    
    collection_moagent_homepage=helper_sql_server.fetch_dict_map_to_collection(collection_moagent_homepage,sql_template=r'''
        select 
        user_id
        ,replace(user_name,'@shabik.com','')  as msisdn
        from mozone_user.dbo.profile with(nolock) 
        where user_id in (%s)
    ''',conn_config=config.conn_stc,step=1000)

    print key,len(collection_moagent_homepage.keys()),len(set(collection_moagent_homepage.values()))

    user_sets[key]=set([format_msisdn(i) for i in collection_moagent_homepage.values()])
    
    

    # moagent uv

    key='daily_moagent_total_unique'
    collection_moagent=helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_daily_visitor_unique',sub_key='', \
                                    date=date_today,table_name='raw_data')

    collection_moagent=helper_sql_server.fetch_dict_map_to_collection(collection_moagent,sql_template=r'''
        select 
        user_id
        ,replace(user_name,'@shabik.com','') as msisdn
        from mozone_user.dbo.profile with(nolock) 
        where user_id in (%s)
    ''',conn_config=config.conn_stc,step=1000)

    print key,len(collection_moagent.keys()),len(set(collection_moagent.values()))

    user_sets[key]=set([format_msisdn(i) for i in collection_moagent.values()])

    

    # moagent 5 uv

    key='daily_moagent_5_total_unique'
    collection_moagent_5=helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_only_shabik_5_daily_visitor_unique',sub_key='', \
                                    date=date_today,table_name='raw_data')

    collection_moagent_5=helper_sql_server.fetch_dict_map_to_collection(collection_moagent_5,sql_template=r'''
        select 
        user_id
        ,replace(user_name,'@shabik.com','') as msisdn
        from mozone_user.dbo.profile with(nolock) 
        where user_id in (%s)
    ''',conn_config=config.conn_stc,step=1000)

    print key,len(collection_moagent_5.keys()),len(set(collection_moagent_5.values()))

    user_sets[key]=set([format_msisdn(i) for i in collection_moagent_5.values()])




    # total moagent home page uv

    key='daily_moagent_homepage_total_unique'
    collection_moagent_homepage=helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_by_app_daily_visitor_unique',sub_key='homepage_old_version', \
                                    date=date_today,table_name='raw_data')
    
    collection_moagent_homepage=helper_sql_server.fetch_dict_map_to_collection(collection_moagent_homepage,sql_template=r'''
        select 
        user_id
        ,replace(user_name,'@shabik.com','') as msisdn
        from mozone_user.dbo.profile with(nolock) 
        where user_id in (%s)
    ''',conn_config=config.conn_stc,step=1000)

    print key,len(collection_moagent_homepage.keys()),len(set(collection_moagent_homepage.values()))

    user_sets[key]=set([format_msisdn(i) for i in collection_moagent_homepage.values()])

    user_sets['daily_moagent_homepage_total_unique'] &= user_sets['daily_moagent_5_total_unique']




    # total mosysinfo

    key='daily_mosysinfo_total_unique'
    sql=r'''

    select 

    distinct replace(user_name,'@shabik.com','') as msisdn
    ,case when u.id is not null then 1 else 0 end as send_mosysinfo

    from [MYSQL_MOSYSINFO]...[UserStatus] u with(nolock)
    left join DB85.mozone_user.dbo.profile p with(nolock)
    on u.id=p.user_id

    where modified_on>='%s' and modified_on<'%s'
    and user_name like '%%@shabik.com'

    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(conn_config=config.conn_helper_db,sql=sql)

    values=[format_msisdn(i) for i in values.keys()]
    user_sets[key]=set(values)




    
    # total auth

    key='daily_auth_total_unique'
    sql='''
    
    select 
    distinct acc as sub_auth_success,null
    from [shabik_mt].[dbo].[moweb_nologin_auth] with(nolock)
    where created_on>='%s' and created_on<'%s'

    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_mt,sql)

    values=[format_msisdn(i) for i in values.keys()]
    user_sets[key]=set(values)



    print 'user_sets=',repr(user_sets)
    print 'filters=',repr(filters)

    """
    """
    for collection_key,collection in user_sets.iteritems():

        helper_mysql.put_collection(collection=collection,oem_name=oem_name,category=stat_category, \
                            key=collection_key,sub_key='',table_name=db_name,date=date_today)

        for filter_key,filter_collection in filters.iteritems():
            
            filtered_key=collection_key.replace('_total_','_'+filter_key+'_')
            filtered_collection=collection & filter_collection

            helper_mysql.put_collection(collection=filtered_collection,oem_name=oem_name,category=stat_category, \
                                key=filtered_key,sub_key='',table_name=db_name,date=date_today)

            filtered_key=collection_key.replace('_total_','_'+filter_key+'_lost_')
            filtered_collection= filter_collection - collection

            helper_mysql.put_collection(collection=filtered_collection,oem_name=oem_name,category=stat_category, \
                                key=filtered_key,sub_key='',table_name=db_name,date=date_today)


    return








if __name__=='__main__':
    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat(my_date)
