import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import re



# online time in mossesion is tricky bcz the online span can cross more than 1 day.
# so now stat 2 days before and take 2 days' data as one day, then apply filter

group_key_current_day=''

current_day_for_mosession_refined=0
current_day_for_mosession_refined_ceil=0
current_day_for_mosession_refined_floor=0

# mosession begin

    

re_get_data_usage=re.compile(r',(\d{4} \d{1,2} \d{1,2} \d{1,2} \d{1,2} \d{1,2}),(\d{4} \d{1,2} \d{1,2} \d{1,2} \d{1,2} \d{1,2}),(\d+),(\d+)')

def get_data_usage(line): #

    #line= r'NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange..com,2010 6 7 22 00 00,2010 6 7 1 00 00,500000,500000,95.170.221.101,4018'
    #log time,monetid,username,login time,logout time,inbytes,outbytes,ip,connect port

    m=re_get_data_usage.search(line)
    if m and m.group(3) and m.group(4): 
        return int(m.group(3))+int(m.group(4))
    else:
        return None


re_get_online_time=re.compile(r',(\d{4} \d{1,2} \d{1,2} \d{1,2} \d{1,2} \d{1,2}),(\d{4} \d{1,2} \d{1,2} \d{1,2} \d{1,2} \d{1,2})')

def get_online_time(line):
    #line= r'NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange..com,2010 6 7 22 00 00,2010 6 8 1 00 00,500000,500000,95.170.221.101,4018'
    #log time,monetid,username,login time,logout time,inbytes,outbytes,ip,connect port

    m=re_get_online_time.search(line)
    if m and m.group(1) and m.group(2): 
        ts_start=time.mktime(time.strptime(m.group(1),'%Y %m %d %H %M %S'))
        ts_end=time.mktime(time.strptime(m.group(2),'%Y %m %d %H %M %S'))
        return ts_end-ts_start
    else:
        return None
    
def get_current_date(line):
    global group_key_current_day
    return group_key_current_day
    
def get_login_time_date_mosession(line):
    global group_key_current_day
    return group_key_current_day

def is_valid_date_mosession_refined(line):
    return True

def is_valid_login_date_mosession_refined(line):
    return True

def get_online_time_level_mosession(value):
    hour=int(value)/3600
    if hour>24:
        hour=24
    return int(hour)        

# mosession end


def stat_mosession(my_date):
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor

    oem_name='Vodafone'
    stat_category='mosession'

    group_key_current_day=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    current_day_for_mosession_refined=my_date
    current_day_for_mosession_refined_ceil=helper_regex.time_ceil_timestamp(current_day_for_mosession_refined)
    current_day_for_mosession_refined_floor=helper_regex.time_floor_timestamp(current_day_for_mosession_refined)
    
    date_str=datetime.fromtimestamp(my_date).strftime('%Y_%m_%d')
    day_str=datetime.fromtimestamp(my_date).strftime('%d')


    # NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 23 35 29,2010 5 24 0 15 15,54046,422473,95.170.221.101,4018
    # log time, monetid, username, in time, out time, inbytes, outbytes, ip, port
    
    stat_plan=Stat_plan(plan_name='daily-mosession-vodafone')

    #All begin

    stat_sql_data_usage_total_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_sum={'bytes':get_data_usage}, \
                                   select_sum_top={'top_max_100':{'key':r'\d{2}: (\d+)','value':get_data_usage,'limit':100}}, \
                                   where={'refined_data_usage':is_valid_date_mosession_refined}, \
                                   group_by={'daily':get_current_date}, \
                                   db_name='raw_data')

    stat_plan.add_stat_sql(stat_sql_data_usage_total_daily)

    stat_sql_online_time_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r': (\d+),'}, \
                                   select_average={'per_session':get_online_time}, \
                                   select_sum_top={'top_max_100_in_sec':{'key':r'\d{2}: (\d+)', \
                                                                  'value':get_online_time,'limit':100}}, \
                                   select_sum_average={'daily_online':{'key':r'\d{2}: (\d+)','value':get_online_time},
                                                       'daily_online_distribution':{'key':r'\d{2}: (\d+)', \
                                                                                    'value':get_online_time,
                                                                                    'sec_group_key':get_online_time_level_mosession}}, \
                                   where={'online_time_refined':is_valid_login_date_mosession_refined}, \
                                   group_by={'daily':get_login_time_date_mosession}, \
                                   db_name='raw_data')

    stat_plan.add_stat_sql(stat_sql_online_time_daily)

    #All end

    stat_plan.add_log_source(r'\\192.168.1.52\log_vodafone_mosession\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.37\log_vodafone_mosession\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.71\log_vodafone_mosession\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.73\log_vodafone_mosession\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.74\log_monet_vodafone\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.78\log_mosession_vodafone\MoSession_'+date_str) 
    
    stat_plan.run()    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_mosession(time.time()-3600*24*i)

