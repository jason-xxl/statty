import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import re
import helper_ip
import helper_mysql

#line= r'NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 23 35 29,2010 5 24 0 15 15,54046,422473,95.170.221.101,4018'
#log time,monetid,username,login time,logout time,inbytes,outbytes,ip,connect port

def get_country_name(line):
    ip=helper_regex.extract(line,r'(\d+\.\d+\.\d+\.\d+)')
    if not ip:
        return 'ip_empty'
    return helper_ip.get_country_name_from_ip(ip)

date_str=''

def get_date(line):
    return date_str

def stat_mosession(my_date):
    global date_str
    date_str=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    print date_str

    collection_id=helper_mysql.get_raw_data(oem_name = "Mozat",category = "mosession",key = "login_by_country_daily_monet_id_unique_collection_id",sub_key="Iraq",date=date_str)
    if collection_id>0:
        print 'exist:',date_str
        return 
    
    oem_name='Mozat'
    stat_category='mosession'

    # NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 23 35 29,2010 5 24 0 15 15,54046,422473,95.170.221.101,4018
    # log time, monetid, username, in time, out time, inbytes, outbytes, ip, port
    
    stat_plan=Stat_plan(plan_name='daily-mosession-mozat')

    #All begin

    stat_sql_login_ip_daily_by_ip_country=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'\d+:\d+:\d+: (\d+)'}, \
                                   where={'login':r'(NORMAL)'}, \
                                   group_by={'daily':get_date,'by_country':get_country_name})

    stat_plan.add_stat_sql(stat_sql_login_ip_daily_by_ip_country)

    #All end
    
    date_str_log=datetime.fromtimestamp(my_date).strftime('%Y_%m_%d')

    stat_plan.add_log_source(r'\\192.168.0.110\log_monet\MoSession_'+date_str_log) #mozat

    
    stat_plan.run()    

    
    
if __name__=='__main__':
    

    for i in range(1+config.day_to_update_stat,1,-1):
        stat_mosession(time.time()-3600*24*i)

