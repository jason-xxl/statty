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
    stat_category='sub_only_umobile'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    # login 1 day
    
    active_user_set_moagent=helper_mysql.get_raw_collection_from_key(oem_name='Mozat', \
                        category='moagent',key='app_page_daily_visitor_unique',sub_key='', \
                        date=date_today, \
                        table_name='raw_data',db_conn=None)
    
    start_time=helper_regex.time_floor(my_date)

    db=''
    sql=r'''
    
    select user_id
    from mozone_user.dbo.profile with(nolock) 
    where 
    [creationDate]>='%s'
    and [creationDate]<'%s'
    and version_tag='fast_umobile'
    and user_name not like '%%00000%%'
    and user_name not like 'circle_%%'
    
    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    new_user_user_set=helper_sql_server.fetch_set(config.conn_mozat,sql)
    new_user_user_set=set([str(user_id) for user_id in new_user_user_set])

    logined_new_user_user_set=new_user_user_set & active_user_set_moagent

    key='user_new_1_day_unique'
    print len(new_user_user_set)
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,len(new_user_user_set),table_name='raw_data_umobile')

    key='user_new_logined_1_day_unique'
    print len(logined_new_user_user_set)
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,len(logined_new_user_user_set),table_name='raw_data_umobile')




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_login(my_date)
