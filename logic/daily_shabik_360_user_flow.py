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
import common_shabik_360
config.collection_cache_enabled=True



def stat(my_date):
    

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
 
    # total unsub
    
    _start = helper_regex.date_add(date_today,-3)+' 05:00:00'
    _end = helper_regex.date_add(date_today,3)+' 05:00:00'
    
    sql = r'''
        select msisdn
        from shabik_mt.dbo.logs with(nolock)
        where CreatedOn>='{s}' and CreatedOn<'{e}'
        group by msisdn
        having max(id*100+[action])-max(id*100)=11
    '''.format(s=_start,e=_end)
    print(sql)
    total_unsub_user_msisdn_set = helper_sql_server.fetch_set(conn_config=config.conn_stc_mt,sql=sql)
    print(total_unsub_user_msisdn_set)
    dct = common_shabik_360.get_msisdn_to_id_dict_from_msisdn_set(total_unsub_user_msisdn_set)
    total_unsub_user_set = set(str(v) for v in dct.itervalues() if v)
    print(total_unsub_user_set)
    exit()
    
#    sql=r'''
#
#    select user_id 
#    from db85.mozone_user.dbo.profile with(nolock)
#    where user_name in(
#    
#        select '0'+replace(msisdn,'+966','')+'@shabik.com'
#        from shabik_mt.dbo.logs with(nolock)
#        where CreatedOn>='%s' and CreatedOn<'%s'
#        group by msisdn
#        having max(id*100+[action])-max(id*100)=11
#
#    )
#    ''' % (_start,_end)
#    
#    print 'SQL Server:'+sql
#
#    total_unsub_user_set=helper_sql_server.fetch_set(conn_config=config.conn_stc_mt,sql=sql)
#    common_shabik_360.get_msisdn_to_id_dict_from_msisdn_set(msisdn_set)
#    total_unsub_user_set = set([str(i) for i in total_unsub_user_set])


    #total login

    _start = helper_regex.date_add(date_today,-3)+' 05:00:00'
    _end = helper_regex.date_add(date_today,0)+' 05:00:00'
    
    sql=r'''

    select user_id 
    from db85.mozone_user.dbo.profile with(nolock)
    where creationDate>='%s'
    and creationDate<'%s'
    and user_name like '%%@shabik.com'

    ''' % (_start,_end)
    
    print 'SQL Server:'+sql

    total_new_user_set=helper_sql_server.fetch_set(conn_config=config.conn_stc,sql=sql)
    total_new_user_set = set([str(i) for i in total_new_user_set])



    # shabik 5

    oem_name='STC'
    stat_category='user_flow'
    db_name='raw_data'


    last_active_user_set=set([])

    for i in range(-1,-4,-1):
        date_temp=helper_regex.date_add(date_today,i)
        last_active_user_set |= helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_only_shabik_5_daily_visitor_unique',sub_key='', \
                                    date=date_temp,table_name='raw_data')

    current_active_user_set=set([])

    for i in range(0,3,1):
        date_temp=helper_regex.date_add(date_today,i)
        current_active_user_set |= helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_only_shabik_5_daily_visitor_unique',sub_key='', \
                                    date=date_temp,table_name='raw_data')

    current_active_user_set_migrated=set([])

    for i in range(0,3,1):
        date_temp=helper_regex.date_add(date_today,i)
        current_active_user_set_migrated |= helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_daily_visitor_unique',sub_key='', \
                                    date=date_temp,table_name='raw_data_shabik_360')





    key='daily_-3d_active_user_unique'
    helper_mysql.put_raw_data(value=len(last_active_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    key='daily_-3d_new_user_unique'
    new_user_set = last_active_user_set & total_new_user_set
    helper_mysql.put_raw_data(value=len(new_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)


    key='daily_-3d_3d_unsub_user_unique'
    unsub_user_set = total_unsub_user_set & last_active_user_set
    helper_mysql.put_raw_data(value=len(unsub_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)


    key='daily_3d_retained_user_unique'
    retained_user_set = current_active_user_set & last_active_user_set - unsub_user_set - current_active_user_set_migrated
    helper_mysql.put_raw_data(value=len(retained_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)


    key='daily_3d_migrated_user_unique'
    migrated_user_set = current_active_user_set_migrated & last_active_user_set - unsub_user_set - current_active_user_set
    helper_mysql.put_raw_data(value=len(migrated_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    key='daily_3d_both_version_user_unique'
    both_version_user_set = last_active_user_set & (current_active_user_set & current_active_user_set_migrated)
    helper_mysql.put_raw_data(value=len(both_version_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    key='daily_3d_in_sub_no_login_user_unique'
    no_login_user_set = last_active_user_set - unsub_user_set - retained_user_set - migrated_user_set - both_version_user_set
    helper_mysql.put_raw_data(value=len(no_login_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)



    # shabik 360




    oem_name='Shabik_360'
    stat_category='user_flow'
    db_name='raw_data'


    last_active_user_set=set([])

    for i in range(-1,-4,-1):
        date_temp=helper_regex.date_add(date_today,i)
        last_active_user_set |= helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_daily_visitor_unique',sub_key='', \
                                    date=date_temp,table_name='raw_data_shabik_360')

    current_active_user_set=set([])

    for i in range(0,3,1):
        date_temp=helper_regex.date_add(date_today,i)
        current_active_user_set |= helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_daily_visitor_unique',sub_key='', \
                                    date=date_temp,table_name='raw_data_shabik_360')

    current_active_user_set_migrated=set([])

    for i in range(0,3,1):
        date_temp=helper_regex.date_add(date_today,i)
        current_active_user_set_migrated |= helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_only_shabik_5_daily_visitor_unique',sub_key='', \
                                    date=date_temp,table_name='raw_data')



    key='daily_-3d_active_user_unique'
    helper_mysql.put_raw_data(value=len(last_active_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    key='daily_-3d_new_user_unique'
    new_user_set = last_active_user_set & total_new_user_set
    helper_mysql.put_raw_data(value=len(new_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    key='daily_-3d_3d_unsub_user_unique'
    unsub_user_set = total_unsub_user_set & last_active_user_set
    helper_mysql.put_raw_data(value=len(unsub_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    key='daily_3d_retained_user_unique'
    retained_user_set = current_active_user_set & last_active_user_set - unsub_user_set - current_active_user_set_migrated
    helper_mysql.put_raw_data(value=len(retained_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    key='daily_3d_migrated_user_unique'
    migrated_user_set = current_active_user_set_migrated & last_active_user_set - unsub_user_set - current_active_user_set
    helper_mysql.put_raw_data(value=len(migrated_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    key='daily_3d_both_version_user_unique'
    both_version_user_set = last_active_user_set & (current_active_user_set & current_active_user_set_migrated)
    helper_mysql.put_raw_data(value=len(both_version_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)

    key='daily_3d_in_sub_no_login_user_unique'
    no_login_user_set = last_active_user_set - unsub_user_set - retained_user_set - migrated_user_set - both_version_user_set
    helper_mysql.put_raw_data(value=len(no_login_user_set),oem_name=oem_name,category=stat_category, \
                                key=key,sub_key='',date=date_today,table_name=db_name)






if __name__=='__main__':
    (days,day_end)  = (40,38)
#    (days,day_end)  = (4,0)
    for i in range(config.day_to_update_stat+days,day_end,-1): 
        my_date=time.time()-3600*24*i
        stat(my_date)
