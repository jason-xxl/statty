import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_mysql
import helper_ip
import config
import common_loops_by_country

helper_mysql.quick_insert=True

from user_id_filter import user_id_filter_mozat_client_6
from user_id_filter import user_id_filter_globe
from user_id_filter import user_id_filter_umobile
from user_id_filter import user_id_filter_ais

def is_mozat_client_6(line):
    return helper_regex.recognize_client_version_number(line)=='6'

def get_country_name(line):
    ip=helper_regex.extract(line,r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')
    if not ip:
        return 'ip_empty'
    try:
        country=helper_ip.get_country_name_from_ip(ip)
    except:
        print 'error IP: '+ip
        country='Error IP'

    if country=='Unknown IP':
        print 'Unknown IP: '+ip
    return country


current_date=''

def get_current_date(line):
    global current_date
    return current_date

def stat_moagent(my_date):
    global current_date
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    oem_name='Mozat'
    stat_category='moagent'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-mozat')
    
    # app uv
    
    stat_sql_app_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   #select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                   #                'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                   #                'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                   #                'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                   #                'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                   #                'page_size':r'(?:[^\t]+\t){6}(\d+)', \
                                   #                }, \
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

    # by url key

    stat_sql_url_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct={'visitor':r'monetid=(\d+)'}, \
                                   select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                                   'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                   #                'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                   #                'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                   #                'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                   #                'page_size':r'(?:[^\t]+\t){6}(\d+)', \
                                                   },
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_url_pattern':helper_regex.get_simplified_url_unique_key,'daily':get_current_date}, \
                                   db_name='data_url_pattern_mozat', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_url_daily)


    # morange version
    
    stat_sql_version_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_morange_version':helper_regex.extract_client_morange_version,'daily':get_current_date})

    #stat_plan.add_stat_sql(stat_sql_version_daily)


    # screen size
    
    stat_sql_screen_size_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_client_screen_size':helper_regex.extract_client_screen_size,'daily':get_current_date})

    #stat_plan.add_stat_sql(stat_sql_screen_size_daily)
    

    # by country

    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern,
                                          'provided_ip':r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'}, \
                                   group_by={'daily':get_current_date, \
                                             'by_country':get_country_name}, \
                                   db_name='raw_data_country')

    stat_plan.add_stat_sql(stat_sql_daily)


    ##user device

    # morange version by user

    stat_sql_version_by_user=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_int_value={'client_version':helper_regex.recognize_client_version_number, \
                                                           'client_type':helper_regex.recognize_client_version_type_int_value, \
                                                           'platform_type':helper_regex.recognize_phone_platform_int_value}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_user_id':r'monetid=(\d+)'}, \
                                   db_name='raw_data_user_device', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    #stat_plan.add_stat_sql(stat_sql_version_by_user)


    stat_sql_user_agent_str=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_text_value={'user_agent':r'userAgent=(.*?)(?:&|\s*$)'}, \
                                   where={'from_app_request':r'(u)serAgent=[^&$]'}, \
                                   group_by={'by_monet_id':r'monetid=(\d+)'}, \
                                   db_name='data_int_user_info_mozat', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)
    
    #stat_plan.add_stat_sql(stat_sql_user_agent_str)

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143

    stat_sql_user_agent_str=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct={'visitor':r'monetid=(\d+)'}, \
                                   where={'from_app_request':r'(u)serAgent=[^&$]'}, \
                                   group_by={'by_phone_model':lambda line:helper_regex.extract(line.replace('%2F','/').replace('+',' '),r'userAgent=(.{20})')}, \
                                   db_name='data_int_user_info_mozat', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)
    
    #stat_plan.add_stat_sql(stat_sql_user_agent_str)
    
    
    
    # morange version type
    
    stat_sql_version_type_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_morange_version_type':helper_regex.extract_client_morange_version_type_with_s3_s5, \
                                             'daily':lambda line:current_date},
                                   db_name='raw_data')

    stat_plan.add_stat_sql(stat_sql_version_type_daily)
    
    
    stat_sql_build_info_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_build_info':helper_regex.extract_client_build_info, \
                                             'daily':lambda line:current_date},
                                   db_name='raw_data_mozat')

    #stat_plan.add_stat_sql(stat_sql_build_info_daily)
    


    # uv

    stat_sql_uv_hourly_ais=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern, \
                                          'only_ais':user_id_filter_ais.is_valid_user}, \
                                   group_by={'hourly':lambda line:helper_regex.format_date_time_moagent(line)[0:13]})

    stat_plan.add_stat_sql(stat_sql_uv_hourly_ais)

    # ais special
    
    stat_sql_app_daily_by_operator_id=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':get_current_date, \
                                             'by_app':helper_regex.recognize_app_from_moagent_log_line, \
                                             'by_op':common_loops_by_country.get_operator_id})
    
    stat_plan.add_stat_sql(stat_sql_app_daily_by_operator_id)
    
    stat_sql_app_daily_by_country=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':get_current_date, \
                                             'by_app':helper_regex.recognize_app_from_moagent_log_line, \
                                             'by_country':common_loops_by_country.get_country_name})
    
    stat_plan.add_stat_sql(stat_sql_app_daily_by_country)
    
    stat_sql_app_daily_by_country_by_operator_id=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':get_current_date, \
                                             'by_app':helper_regex.recognize_app_from_moagent_log_line, \
                                             'by_country':common_loops_by_country.get_country_name, \
                                             'by_op':common_loops_by_country.get_operator_id})
    
    stat_plan.add_stat_sql(stat_sql_app_daily_by_country_by_operator_id)

    # add filters
    
    excluded_stat_sql=[stat_sql_uv_hourly_ais,
                       stat_sql_app_daily_by_operator_id,
                       stat_sql_app_daily_by_country,
                       stat_sql_app_daily_by_country_by_operator_id
                       ]

    #stat_plan.add_stat_brunch_filters({'only_globe':user_id_filter_globe.is_valid_user},excluded_stat_sqls=excluded_stat_sql)
    #stat_plan.add_stat_brunch_filters({'only_umobile':user_id_filter_umobile.is_valid_user},excluded_stat_sqls=excluded_stat_sql)
    
    stat_plan.add_stat_brunch_filters({'only_ais':user_id_filter_ais.is_valid_user},excluded_stat_sqls=excluded_stat_sql)


    #stat_plan.add_log_source(r'\\192.168.0.113\logs_moagent\internal_perf.log.' \
    #       +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    #stat_plan.add_log_source(r'\\192.168.0.104\logs_moagent\internal_perf.log.' \
    #       +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    #stat_plan.add_log_source(r'\\192.168.0.111\logs_moagent\internal_perf.log.' \
    #       +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    #stat_plan.add_log_source(r'\\192.168.0.162\logs_moagent_mozat\internal_perf.log.' \
    #       +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    
    stat_plan.add_log_source(r'\\192.168.0.147\logs_moagent_mozat\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.157\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.181\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.182\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.79\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.135\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.137\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.dump_sources()

    stat_plan.run()



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)



