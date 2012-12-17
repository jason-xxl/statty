import os
import helper_sql_server
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

synchronize_last_login_time_temp={}

def synchronize_last_login_time(line='',exist='',group_key=''):

    global synchronize_last_login_time_temp

    user_name=helper_regex.extract(line,r' - (.*?)(?: login)').lower()
    login_time=helper_regex.extract(line,r'(\d+\-\d+\-\d+ \d+:\d+:\d+)')

    if not synchronize_last_login_time_temp.has_key(user_name):
        synchronize_last_login_time_temp[user_name]=login_time
    elif synchronize_last_login_time_temp[user_name]<login_time:
        synchronize_last_login_time_temp[user_name]=login_time
        


def stat_login_service(my_date):

    global synchronize_last_login_time_temp

    oem_name='Telk_Armor'
    stat_category='login_service'
    stat_plan=Stat_plan()

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    
    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   process_exist={'synchronize_last_login_time': \
                                        {'pattern':r'MoPeerLoginService - (.*?) login', \
                                         'process':synchronize_last_login_time}},
                                   where={'login_response_succeeded':r'(l)ogin result: result=0'}))
        

    stat_plan.add_log_source(r'\\192.168.1.36\logs_login_svc\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.100.20\morange\moLoginSvr\logs\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    

    stat_plan.run()    

    # get filter of up-to-date users

    sql=r"""
        select 
        lower(user_name),convert(varchar, lastLogin, 121) 
        from mozone_user.dbo.profile with(nolock) 
        where 
        [lastLogin] is not null 
        and [lastLogin]>='%s'
    """ % (start_time,)

    up_to_date_users=helper_sql_server.fetch_dict(config.conn_telk_armor,sql) 

    print sql
    print 'up_to_date_users:'+str(len(up_to_date_users))
    print up_to_date_users

    # do update
    
    synchronize_last_login_time_sql_tpl=r"""
        UPDATE [mozone_user].[dbo].[Profile]
        SET [lastLogin] = '%s'
        WHERE 
        [user_name]='%s'
        and (
            [lastLogin] is null 
            or [lastLogin]<'%s'
        )
    """

    affected_user_names={}

    for uses_name,last_login_time in synchronize_last_login_time_temp.iteritems():
        if up_to_date_users.has_key(uses_name) and last_login_time<up_to_date_users[uses_name]:
            continue
        sql=synchronize_last_login_time_sql_tpl % (last_login_time,uses_name,last_login_time)
        affected=helper_sql_server.execute(config.conn_telk_armor,sql)
        if affected>0:
            affected_user_names[uses_name]=last_login_time

    print 'synchronize_last_login_time affected: '+str(len(affected_user_names))
    print 'synchronize_last_login_time user_names: '+str(affected_user_names)


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_login_service(my_date)


