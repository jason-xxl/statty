import config
config.conn_stat_portal=config._conn_stat_portal_158_2

import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_ip

user_name_to_ip_temp={}
def process_request(line='',exist='',group_key=''):
    global user_name_to_ip_temp
    user_name=helper_regex.extract(line,r'peername=(.*?),').replace(' ','')
    ip=helper_regex.extract(line,r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')
    user_name_to_ip_temp[user_name]=ip

def extract_ip(line):
    global user_name_to_ip_temp
    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8

    user_name=helper_regex.extract(line,r'MoPeerLoginService \- (.*?) login result: result=0')
    if user_name_to_ip_temp.has_key(user_name):
        return user_name_to_ip_temp[user_name]
    else:
        return ""

def extract_ip_number(line):
    return helper_regex.ip_to_number(extract_ip(line))

def extract_country(line):
    return helper_ip.get_country_name_from_ip(extract_ip(line))

def stat_login_service(my_date):

    oem_name='Mozat'
    stat_category='login_service'
    stat_plan=Stat_plan()

    # request
    # 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   process_exist={'process_ip':{'pattern':'(ip=)','process':process_request}}, \
                                   where={'login_request':r'(l)ogin request:'}))

    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_int_value={'last_login_ip':extract_ip_number}, \
                                   select_first_text_value={'last_login_country':extract_country}, \
                                   where={'login_response':r'(M)oPeerLoginService \- (.*?) login result: result=0,',
                                          'provided_ip':extract_ip}, \
                                   group_by={'by_user_id':r'peerId=(\d+)'}, \
                                   db_name='raw_data_user_info'), \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_log_source(r'V:\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.run()    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_login_service(my_date)


