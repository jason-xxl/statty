import helper_sql_server
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mysql
import common_vodafone

helper_mysql.quick_insert=True

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date

def stat_moagent(my_date):

    global current_date
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_vodafone)

    oem_name='Vodafone'
    stat_category='moagent'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880	390	15	32	15	328	3648	http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-vodafone')
    
    # app uv
    
    stat_sql_app_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   #select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                   #                'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                   #                'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                   #                'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                   #                'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                   #                'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':get_current_date,'by_app':helper_regex.recognize_app_from_moagent_log_line})

    stat_plan.add_stat_sql(stat_sql_app_daily)

    # uv

    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   #select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                   #                'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                   #                'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                   #                'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                   #                'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                   #                'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_daily)

    # morange version type
    
    stat_sql_version_type_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_morange_version_type':helper_regex.extract_client_morange_version_type_with_s3_s5, \
                                             'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_version_type_daily)




    """
    # add filter for recent new user
    
    filter_2d_new_user=common_vodafone.get_new_user_2d_filter(current_date,pattern=r'monetid=(\d+)')
    stat_plan.add_stat_brunch_filters({'only_2d_new_user':filter_2d_new_user})
    """


    # add log source

    # add log source


    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.37\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.67\logs_vodafone_moagent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.52\logs_vodafone_moagent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.60\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.68\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))  ## available

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.73\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.73\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))  ## available

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))  ## available

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.75\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.75\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.78\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d')) ## available

    stat_plan.run()    

    stat_plan.dump_sources()





if __name__=='__main__':

    t=time.time()

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(t-3600*24*i)
