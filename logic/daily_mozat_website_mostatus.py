import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date

def extract_url_key(line):
    url_key=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+) ')
    url_key=helper_regex.regex_replace(r'\d+','...',url_key)
    return url_key

def stat_website(my_date):

    global current_date
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    #Fields: date time cs-method cs-uri-stem cs-uri-query c-ip cs-version cs(User-Agent) cs(Referer) sc-status sc-bytes time-taken 
    #2010-12-28 09:51:44 POST /Status/Create/ - 192.168.0.110 HTTP/1.1 Jakarta+Commons-HttpClient/3.1-rc1 - 200 774 78

    oem_name='Mozat'
    stat_category='website_mostatus'

    stat_plan=Stat_plan()
    
    stat_sql_client_download_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'action':r'(.)'}, \
                                   select_average={'process_time':r' (\d+)$'}, \
                                   where={'request':r' (GET|POST) '}, \
                                   group_by={'daily':get_current_date, \
                                             'by_url_pattern':extract_url_key}, \
                                   db_name='raw_data_url_pattern', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)
    
    stat_plan.add_stat_sql(stat_sql_client_download_daily)

    stat_plan.add_log_source(r'\\192.168.0.104\log_mostatus\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'.log')

    stat_plan.run()    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_website(my_date)
