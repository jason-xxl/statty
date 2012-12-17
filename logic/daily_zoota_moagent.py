import common_shabik_360
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mysql
import helper_user_filter

#from user_id_filter import user_id_filter_stc_male
#from user_id_filter import user_id_filter_stc_female


helper_mysql.quick_insert=True

def stat_moagent(my_date):

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_zoota)
    get_current_date=lambda line:current_date

    oem_name='Zoota'
    stat_category='moagent'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-zoota')

    # uv

    stat_sql_unique_user_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':{'key':r'monetid=(\d+)','date_units':[],'with_value':True}}, \
                                   #select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                   #                'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                   #                'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                   #                'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                   #                'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                   #                'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':lambda line:current_date},
                                   db_name='raw_data_zoota')

    stat_plan.add_stat_sql(stat_sql_unique_user_daily)


    # app uv
    
    stat_sql_app_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':{'key':r'monetid=(\d+)','date_units':[],'with_value':True}}, \
                                   #select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                   #                'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                   #                'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                   #                'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                   #                'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                   #                'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':lambda line:current_date,'by_app':helper_regex.recognize_app_from_moagent_log_line},
                                   db_name='raw_data_zoota')
    
    stat_plan.add_stat_sql(stat_sql_app_daily)

    stat_plan.add_log_source(r'\\192.168.12.101\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.run()

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)


