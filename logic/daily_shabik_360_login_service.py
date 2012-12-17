import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_stc
import config

def get_peer_name(line):
    try:
        name=helper_regex.extract(line,r'peername=([^,]+),')
        return helper_regex.regex_replace('[^\w_\.@]','',name)
    except:
        print 'error log (peername): ',line
        return ''


def stat_login_service(my_date):


    oem_name='Shabik_360'
    stat_category='login_service'
    table_name='raw_data_shabik_360'

    stat_plan=Stat_plan()

    #Daily

    ##### All Begin #####

    # request
    # 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71
    # 2012-06-07 19:00:01,509 [INFO] MoPeerLoginService - login request: peername=motest20013@shabik.com, pwd=f7235a61fdc3adc78d866fd8085d44db, version=1, deviceType=0, ip=/212.100.219.210

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}, \
                                   db_name=table_name))

    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    # 2012-06-07 19:00:01,509 [INFO] MoPeerLoginService - motest20013@shabik.com login result: result=0,peerId=20013,flag=0
    
    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'result=([^,]+),'}, \
                                   db_name=table_name))



    # result
    # 2010-06-04 20:00:59,875 [INFO] MoPeerLoginService - gooffy_go@morange.com login failure : wrong username

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_result':r'(l)ogin failure :'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'login failure : (.*)'}, \
                                   db_name=table_name))

    ##### All End #####
    

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path( \
                                        my_date,r'\\192.168.0.107\logs_login\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.108\logs_login_service_stc\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.117\logs_login_service_stc\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.118\logs_login_service_stc\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.185\logs_mologin_shabik_360\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.195\logs_login_service_stc\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.196\logs_mologin_shabik_360\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.75\logs_login_service_stc\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
    
    stat_plan.run()    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_login_service(my_date)
