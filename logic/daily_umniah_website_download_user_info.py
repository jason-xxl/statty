import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mysql

helper_mysql.quick_insert=True

number_table={
    'z': '0',
    'a': '1',
    'y': '2',
    'b': '3',
    'x': '4',
    'c': '5',
    'w': '6',
    'd': '7',
    'v': '8',
    'e': '9'
    }

def translate_phone_number(line):
    global number_table
    number=helper_regex.extract(line,r'GET /download\.aspx ([zaybxcwdve]+)')
    for i,j in number_table.iteritems():
        number=number.replace(i,j)
    return number
    

def stat_website(my_date):

    oem_name='Umniah'
    stat_category='website'
    db_name='raw_data_user_info_periodical'

    # 2010-09-20 16:06:19 GET /download.aspx ewydvvevebvz 192.168.1.42 HTTP/1.0 NokiaX3-00/5.0+(04.11)+Profile/MIDP-2.1+Configuration/CLDC-1.1+Mozilla/5.0+AppleWebKit/420++(KHTML,+like+Gecko)+Safari/420+ - 200 1295 0

    stat_plan=Stat_plan(plan_name='daily-website-viva-bh')
    
    # download uv
    
    stat_sql_user_agent_str=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_text_value={'native_user_agent':r'/download\.aspx \w+ [0-9\.]+ HTTP[^\s]+ ([^\s]+) \- '}, \
                                   select_first_int_value={'ip':helper_regex.ip_to_number}, \
                                   where={'from_download_link':r'(GET /download\.aspx [zaybxcwdve]+)'}, \
                                   group_by={'by_phone_number':translate_phone_number, \
                                             'by_date':r'(\d{4}\-\d{2}\-\d{2})'}, \
                                   db_name=db_name)

    
    stat_plan.add_stat_sql(stat_sql_user_agent_str)
    
    stat_plan.add_log_source(r'\\192.168.1.40\w3svc1082734153\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'.log')

    stat_plan.run()



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_website(time.time()-3600*24*i)


