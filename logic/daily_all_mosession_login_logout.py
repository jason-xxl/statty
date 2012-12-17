import config
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_stc
from user_id_filter import user_id_filter_viva
from user_id_filter import user_id_filter_viva_bh
import re


def get_login_time_timestamp(line):
    #line="NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 23 35 29,2010 05 24 0 15 15,54046,422473,95.170.221.101,4018"
    login_time=helper_regex.extract(line,r',(\d{4} \d+ \d+ \d+ \d+ \d+)')
    return str(time.mktime(time.strptime(login_time,'%Y %m %d %H %M %S')))

def get_logout_time_timestamp(line):
    #line="NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 23 35 29,2010 05 24 0 15 15,54046,422473,95.170.221.101,4018"
    logout_time=helper_regex.extract(line,r',\d{4} \d+ \d+ \d+ \d+ \d+,(\d{4} \d+ \d+ \d+ \d+ \d+)')
    return time.mktime(time.strptime(logout_time,'%Y %m %d %H %M %S'))


def is_valid_session(line):
    is_log=helper_regex.extract(line,r'(NORMAL)')
    if not is_log: 
        return ""
    #line="NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 23 35 29,2010 05 24 0 15 15,54046,422473,95.170.221.101,4018"
    login_time=helper_regex.extract(line,r',(\d{4} \d+ \d+ \d+ \d+ \d+)')
    logout_time=helper_regex.extract(line,r',\d{4} \d+ \d+ \d+ \d+ \d+,(\d{4} \d+ \d+ \d+ \d+ \d+)')

    return login_time and logout_time

def stat_mosession(my_date):
 
    oem_name='All'
    stat_category='mosession'
    
    date_str=datetime.fromtimestamp(my_date).strftime('%Y_%m_%d')

    # NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 23 35 29,2010 5 24 0 15 15,54046,422473,95.170.221.101,4018
    # log time, monetid, username, in time, out time, inbytes, outbytes, ip, port
    
    stat_plan=Stat_plan(plan_name='daily-mosession-all')



    #STC begin

    stat_sql_login_logout_time_stc_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_int_value={'logout_time':get_logout_time_timestamp}, \
                                   where={'only_stc':user_id_filter_stc.is_valid_user, \
                                          'session':is_valid_session}, \
                                   group_by={'by_user_id':r'\d+:\d+:\d+: (\d+),', \
                                             'from_login_time':get_login_time_timestamp}, \
                                   db_name='raw_data_login_log_mobile', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_login_logout_time_stc_daily)

    #STC end

    #Viva_BH begin

    stat_sql_login_logout_time_viva_bh_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_int_value={'logout_time':get_logout_time_timestamp}, \
                                   where={'only_viva_bh':user_id_filter_viva_bh.is_valid_user, \
                                          'session':is_valid_session}, \
                                   group_by={'by_user_id':r'\d+:\d+:\d+: (\d+),', \
                                             'from_login_time':get_login_time_timestamp}, \
                                   db_name='raw_data_login_log_mobile', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_login_logout_time_viva_bh_daily)

    #Viva_BH end

    stat_plan.add_log_source(r'\\192.168.0.79\monet_log\MoSession_'+date_str) #stc
    stat_plan.add_log_source(r'\\192.168.0.100\logMonet\MoSession_'+date_str)
    stat_plan.add_log_source(r'\\192.168.0.107\logMonet\MoSession_'+date_str)
    stat_plan.add_log_source(r'\\192.168.0.108\logMonet\MoSession_'+date_str)
    stat_plan.add_log_source(r'\\192.168.0.117\logMonet\MoSession_'+date_str)
    stat_plan.add_log_source(r'\\192.168.0.118\logMonet\MoSession_'+date_str)
    stat_plan.add_log_source(r'\\192.168.0.75\logMonet\MoSession_'+date_str)
    stat_plan.add_log_source(r'\\192.168.0.103\logMonet\MoSession_'+date_str)
    stat_plan.add_log_source(r'\\192.168.0.140\MoNET\bin\Log\MoSession_'+date_str) #baharin
    stat_plan.add_log_source(r'\\192.168.0.122\monet\MoNET\bin\Log\MoSession_'+date_str) #kwait
    stat_plan.add_log_source(r'\\192.168.0.185\log_mosession_shabik_360\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.0.196\log_mosession_shabik_360\MoSession_'+date_str) 

    # todo: telk:175.103.45.20
    
    stat_plan.run()    


if __name__=='__main__':
    
    for i in range(config.day_to_update_stat,0,-1):
        stat_mosession(time.time()-3600*24*i)

