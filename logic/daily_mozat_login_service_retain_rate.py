import os
import helper_sql_server

from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mysql
import helper_math


# to be continue

def stat_login_service(my_date):


    oem_name='Mozat'
    stat_category='login_service'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    stat_plan=Stat_plan()

    ##### Mozat Begin #####

    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    
    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_retain_rate_by_date={'peer_id':{'key':r'peerId=(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                   where={'login_succeeded':r'(l)ogin result: result=0'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

        
    for i in [1,2,3,4,5,6,7,14,21,28]:

        new_user_filter=helper_sql_server.get_filter_for_fetch_dict(config.conn_mozat,r"""

            select 
            distinct user_id,null
            from mozone_user.dbo.profile with(nolock) 
            where [creationDate]>='%s' and [creationDate]<'%s'

        """ % ( helper_regex.time_add(end_time,-i*2),helper_regex.time_add(end_time,-i))
        ,r'peerId=(\d+)') 

        stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                       select_retain_rate_by_date={'peer_id':{'key':r'peerId=(\d+)','date_units':[i]}}, \
                                       where={'login_succeeded':r'(l)ogin result: result=0', \
                                              'only_new_user':new_user_filter}, \
                                       group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

    

    stat_plan.add_log_source(r'\\192.168.0.110\logs_login_svc\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.run()    


    # 21\22\23th day login rate for new user daily

    current_logined_collection=helper_mysql.get_raw_collection({
        'oem_name':'Mozat',
        'category':'login_service',
        'key':'login_succeeded_daily_peer_id_collection_id',
        'sub_key':'',
        'date':date_today,
        'table_name':'raw_data'
    })

    sql_tpl=r"""
    select user_id,null 
    from mozone_user.dbo.profile with(nolock) 
    where [creationDate]>='%s' and [creationDate]<'%s'
    """
    
    for day in range(21,24):

        registered_collection_n_days_before=helper_sql_server.get_set_for_fetch_dict(config.conn_mozat,sql_tpl % (helper_regex.time_add(end_time,-day),helper_regex.time_add(end_time,-day+1)))

        base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size \
            =helper_math.calculate_collection_retain_rate( \
            registered_collection_n_days_before, \
            current_logined_collection)

        print "rate:",base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size

        base_date=helper_regex.time_add(end_time,-day).replace(' 00:00:00','')
        
        helper_mysql.put_raw_data(oem_name,stat_category,str(day)+"_days_before_registered_user_base_size",base_date,base_size)
        helper_mysql.put_raw_data(oem_name,stat_category,str(day)+"_days_before_registered_user_retain_rate",base_date,retain_rate)

    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):#7+
        my_date=time.time()-3600*24*i
        stat_login_service(my_date)


