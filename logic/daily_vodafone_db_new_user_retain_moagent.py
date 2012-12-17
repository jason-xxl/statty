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


def stat_login_rate(my_date):

    oem_name='Vodafone'
    category='trend'
    table_name='raw_data_trend'

    current_date=helper_regex.get_date_from_timestamp(my_date)
    collection_subscriber=helper_sql_server.fetch_set(conn_config=config.conn_vodafone_88,sql='''
    
    select user_id
    from mozone_user.dbo.profile with(nolock)
    where creationDate>='%s' and creationDate<'%s'
    and [user_name] like '%%@voda%%'

    ''' % (current_date+' 06:00:00',helper_regex.date_add(current_date,1)+' 06:00:00'))

    collection_subscriber=set([str(i) for i in collection_subscriber])
    helper_mysql.put_raw_data(oem_name=oem_name,category=category,key='daily_new_subscriber_unique',value=len(collection_subscriber), \
                                date=current_date,table_name=table_name)


    for i in range(0,8):
        step=1
        collection_active_user_temp=set([])
        #day_range=range(-1,step) if i==1 else range(0,step)
        day_range=range(0,step)
        
        for j in day_range:
            date_temp=helper_regex.date_add(current_date,i+j)
            collection_active_user=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                                                key='app_page_daily_visitor_unique_collection_id',date=date_temp)
            if not collection_active_user:
                print 'error active user collection empty:',current_date

            collection_active_user_temp|=collection_active_user

        collection_active_new_user = collection_active_user_temp & collection_subscriber
        helper_mysql.put_raw_data(oem_name=oem_name,category=category,key='daily_'+str(step)+'d_active_new_subscriber_unique',sub_key=i, \
                                value=len(collection_active_new_user),date=current_date,table_name=table_name)


    for i in range(0,55):
        step=3
        collection_active_user_temp=set([])
        #day_range=range(-1,step) if i==1 else range(0,step)
        day_range=range(0,step)
        
        for j in day_range:
            date_temp=helper_regex.date_add(current_date,i+j)
            collection_active_user=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                                                key='app_page_daily_visitor_unique_collection_id',date=date_temp)
            if not collection_active_user:
                print 'error active user collection empty:',current_date

            collection_active_user_temp|=collection_active_user

        collection_active_new_user = collection_active_user_temp & collection_subscriber
        helper_mysql.put_raw_data(oem_name=oem_name,category=category,key='daily_'+str(step)+'d_active_new_subscriber_unique',sub_key=i, \
                                value=len(collection_active_new_user),date=current_date,table_name=table_name)

    collection_total_active_user=set([])
    for i in range(0,55):
        date_temp=helper_regex.date_add(current_date,i)
        collection_active_user=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                                            key='app_page_daily_visitor_unique_collection_id',date=date_temp)
        if not collection_active_user:
            print 'error active user collection empty:',current_date

        collection_total_active_user|=collection_active_user

    collection_total_active_new_user = collection_total_active_user & collection_subscriber
    helper_mysql.put_raw_data(oem_name=oem_name,category=category,key='daily_total_active_new_subscriber_unique', \
                            value=len(collection_total_active_new_user),date=current_date,table_name=table_name)


if __name__=='__main__':

    for i in range(config.day_to_update_stat+60,0,-1):
        my_date=time.time()-3600*24*i
        stat_login_rate(my_date)
