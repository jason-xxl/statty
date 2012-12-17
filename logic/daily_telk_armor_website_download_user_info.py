import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

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
    number=helper_regex.extract(line,r'GET /dl\.aspx ([zaybxcwdve]+)')
    for i,j in number_table.iteritems():
        number=number.replace(i,j)
    return number
    

def stat_website(my_date):

    oem_name='Telk_Armor'
    stat_category='website'
    db_name='raw_data_user_info_periodical'

    # 2010-09-14 18:05:31 W3SVC1451704712 192.202.100.21 GET /dl.aspx wyvabvebyybyb 80 - 114.127.246.109 Nokia2330c-2/2.0+(06.46)+Profile/MIDP-2.1+Configuration/CLDC-1.1 302 0 0

    stat_plan=Stat_plan(plan_name='daily-website-telk-armor')
    
    # download uv
    
    stat_sql_user_agent_str=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_text_value={'native_user_agent':r'80 \- \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} ([^\s]+) '}, \
                                   select_first_int_value={'ip':helper_regex.ip_to_number}, \
                                   where={'from_download_link':r'(GET /dl\.aspx [zaybxcwdve]+)'}, \
                                   group_by={'by_phone_number':translate_phone_number, \
                                             'by_date':r'(^[\d\-]{10})'}, \
                                   db_name=db_name)
    
    stat_plan.add_stat_sql(stat_sql_user_agent_str)
    
    # sms re-download [2010-08-09]

    # 2010-09-08 02:51:29 W3SVC1451704712 192.202.100.21 GET /dl.aspx wyvcyadbbwdxd&x 80 - 114.127.246.47 SIE-C65/16+UP.Browser/7.0.0.1.c.3+(GUI)+MMP/2.0+Profile/MIDP-2.0+Configuration/CLDC-1.1 302 0 0

    oem_name='Telk_Armor'
    stat_category='website'
    db_name='raw_data'
    
    stat_sql_re_download=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'([zaybxcwdve]+&x)'}, \
                                   where={'from_download_link':r'(GET /dl\.aspx [zaybxcwdve]+&x)'}, \
                                   group_by={'by_date':r'(^[\d\-]{10})'}, \
                                   db_name=db_name)
    
    stat_plan.add_stat_sql(stat_sql_re_download)
    
    stat_plan.add_log_source(r'\\192.168.100.21\w3svc1451704712\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'.log')

    stat_plan.run()



if __name__=='__main__':
    #print helper_regex.extract('2010-09-08 02:51:29 W3SVC1451704712 192.202.100.21 GET /dl.aspx wyvcyadbbwdxd&x 80 - 114.127.246.47 SIE-C65/16+UP.Browser/7.0.0.1.c.3+(GUI)+MMP/2.0+Profile/MIDP-2.0+Configuration/CLDC-1.1 302 0 0',r'(GET /dl.aspx [zaybxcwdve]+&x)')

    for i in range(config.day_to_update_stat,0,-1):
        stat_website(time.time()-3600*24*i)


