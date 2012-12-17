import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

# to be continue

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date


def stat_login_service(my_date):


    global current_date
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_vodafone)

    oem_name='Vodafone'
    stat_category='login_service'

    stat_plan=Stat_plan()

    #Daily

    ##### All Begin #####

    # request
    # old format: 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71
    # 2010-07-27 15:00:53,723 [ INFO]      processLoginPkt - login request: peernnamePre=81281000969@telk_armor,peername=81281000969@telk_armor,pwd=ba6622414cd86ced4ae40451c1c2bacf,version=1,deviceType=0,ip=/203.78.122.13
    # 2010-12-07 10:00:05,148 [ INFO]      processLoginPkt - ayie.ruzl IM3 login ret 0


    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'peername=.*?(\d+)'}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'daily':get_current_date}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'peername=.*?(\d+)'}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'daily':get_current_date, \
                                             'device_type':r'deviceType=(\d+),'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'peername=.*?(\d+)'}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'daily':get_current_date, \
                                             'protocol_version':r'version=(\d+),'}))


    # response
    # old format:2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    # 2010-07-27 15:15:43,911 [ INFO]      processLoginPkt - 81366635970@telk_armor login result: result=0,peerId=100513,flag=4


    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - .*?(\d+).*? login'}, \
                                   where={'login_response':r'(l)ogin result:'}, \
                                   group_by={'daily':get_current_date, \
                                             'result_type':r'result=([^,]+),'}))
        

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - .*?(\d+).*? login'}, \
                                   where={'login_response':r'(l)ogin result:'}, \
                                   group_by={'daily':get_current_date, \
                                             'result_flag':r'flag=(\d+)'}))
        


    # result
    # old format: 2010-06-04 20:00:59,875 [INFO] MoPeerLoginService - gooffy_go@morange.com login failure : wrong username
    # 2010-07-27 15:18:55,473 [ INFO]      processLoginPkt - ink_giel91@yahoo.com@telk_armor login failure : wrong username

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - .*?(\d+).*? login'}, \
                                   where={'login_result':r'(l)ogin failure :'}, \
                                   group_by={'daily':get_current_date, \
                                             'result_type':r'login failure : (.*)'}))


    ##### All End #####

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.52\logs_login_service\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.37\logs_login_service_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\logs_login_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\logs_login_svc_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.73\logs_login_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_login_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_login_svc_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.78\logs_login_svc_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\log_login_svc_buzz\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.run()    

    stat_plan.dump_sources()

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_login_service(my_date)
