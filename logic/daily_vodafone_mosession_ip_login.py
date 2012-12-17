import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import re
from datetime import timedelta



# online time in mossesion is tricky bcz the online span can cross more than 1 day.
# so now stat 2 days before and take 2 days' data as one day, then apply filter

group_key_current_day=''

current_day_for_mosession_refined=0
current_day_for_mosession_refined_ceil=0
current_day_for_mosession_refined_floor=0

# mosession begin

def get_data_usage_mosession(line):
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor
    
    #line= r'NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 23 35 29,2010 5 24 0 15 15,54046,422473,95.170.221.101,4018'
    #log time,monetid,username,login time,logout time,inbytes,outbytes,ip,connect port

    m=re_get_data_usage_mosession.search(line)
    if m and m.group(1) and m.group(2):
        return int(m.group(1))+int(m.group(2))
    else:
        return None
    
#match: 2010 5 23 23 35 29,2010 5 24 0 15 15,
re_get_online_time_mosession=re.compile(r'(\d{4}(?: \d{1,2}){5}),(\d{4}(?: \d{1,2}){5})')

def get_online_time_mosession(line):
    #line= r'NORMAL Fri Jun 04 11:27:59: 14079741,0509940382@shabik.com,2010 6 4 11 27 31,2010 6 4 11 27 59,429,163,212.118.143.147,4018'
    #line=r'NORMAL Sat Jun 05 00:24:51: 14169357,0535840134@shabik.com,2010 6 5 0 24 51,2010 6 5 0 24 51,161,72,212.118.142.75,4018'
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor

    m=re_get_online_time_mosession.search(line)
    if m and m.group(1) and m.group(2):
        #print m.group(1)
        #print m.group(2)
        return time.mktime(time.strptime(m.group(2), "%Y %m %d %H %M %S"))- \
               time.mktime(time.strptime(m.group(1), "%Y %m %d %H %M %S"))
    else:
        return None

re_get_data_usage_mosession_refined=re.compile(r',(\d{4} \d{1,2} \d{1,2} \d{1,2} \d{1,2} \d{1,2}),(\d{4} \d{1,2} \d{1,2} \d{1,2} \d{1,2} \d{1,2}),(\d+),(\d+)')

def get_data_usage_mosession_refined(line):
    #line= r'NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange..com,2010 6 7 22 00 00,2010 6 7 1 00 00,500000,500000,95.170.221.101,4018'
    #log time,monetid,username,login time,logout time,inbytes,outbytes,ip,connect port

 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor

    #only used in stat of 2days before    
    m=re_get_data_usage_mosession_refined.search(line)
    if m and m.group(1) and m.group(2) and m.group(3) and m.group(4): 
        ts_start=time.mktime(time.strptime(m.group(1),'%Y %m %d %H %M %S'))
        ts_end=time.mktime(time.strptime(m.group(2),'%Y %m %d %H %M %S'))
        duration_total=ts_end-ts_start
        if ts_start<current_day_for_mosession_refined_floor:
            ts_start=current_day_for_mosession_refined_floor
        if ts_end>current_day_for_mosession_refined_ceil:
            ts_end=current_day_for_mosession_refined_ceil
        duration_available=ts_end-ts_start
        if duration_total==0:
            return int(m.group(3))+int(m.group(4))
        else:
            return int((int(m.group(3))+int(m.group(4))) \
            *(duration_available/duration_total))
    else:
        return None


re_get_online_time_mosession_refined=re.compile(r',(\d{4} \d{1,2} \d{1,2} \d{1,2} \d{1,2} \d{1,2}),(\d{4} \d{1,2} \d{1,2} \d{1,2} \d{1,2} \d{1,2})')

def get_online_time_mosession_refined(line):
    #line= r'NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange..com,2010 6 7 22 00 00,2010 6 8 1 00 00,500000,500000,95.170.221.101,4018'
    #log time,monetid,username,login time,logout time,inbytes,outbytes,ip,connect port

    #only used in stat of 2days before    
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor

    m=re_get_online_time_mosession_refined.search(line)
    if m and m.group(1) and m.group(2): 
        ts_start=time.mktime(time.strptime(m.group(1),'%Y %m %d %H %M %S'))
        ts_end=time.mktime(time.strptime(m.group(2),'%Y %m %d %H %M %S'))
        if ts_start<current_day_for_mosession_refined_floor:
            ts_start=current_day_for_mosession_refined_floor
        if ts_end>current_day_for_mosession_refined_ceil:
            ts_end=current_day_for_mosession_refined_ceil
        return ts_end-ts_start
    else:
        return None

    
def get_current_date_mosession_refined(line):
    #line= r'NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange..com,2010 6 7 22 00 00,2010 6 8 1 00 00,500000,500000,95.170.221.101,4018'
    #log time,monetid,username,login time,logout time,inbytes,outbytes,ip,connect port
 
    global group_key_current_day

    #only used in stat of 2days before
    return group_key_current_day


def is_valid_date_mosession_refined(line):
    #line= r'NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange..com,2010 6 7 22 00 00,2010 6 8 1 00 00,500000,500000,95.170.221.101,4018'
    #log time,monetid,username,login time,logout time,inbytes,outbytes,ip,connect port
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor


    #only used in stat of 2days before    
    m=re_get_online_time_mosession_refined.search(line)
    if m and m.group(1) and m.group(2): 
        ts_start=time.mktime(time.strptime(m.group(1),'%Y %m %d %H %M %S'))
        ts_end=time.mktime(time.strptime(m.group(2),'%Y %m %d %H %M %S'))
        if ts_start>current_day_for_mosession_refined_ceil:
            return False
        if ts_end<current_day_for_mosession_refined_floor:
            return False
        return True
    else:
        return False

def is_valid_login_date_mosession_refined(line):
    #line= r'NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange..com,2010 6 7 22 00 00,2010 6 8 1 00 00,500000,500000,95.170.221.101,4018'
    #log time,monetid,username,login time,logout time,inbytes,outbytes,ip,connect port
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor


    #only used in stat of 2days before    
    m=re_get_online_time_mosession_refined.search(line)
    if m and m.group(1) and m.group(2): 
        ts_start=time.mktime(time.strptime(m.group(1),'%Y %m %d %H %M %S'))
        
        if ts_start>=current_day_for_mosession_refined_ceil:
            return False
        if ts_start<current_day_for_mosession_refined_floor:
            return False
        return True
    else:
        return False

def get_online_time_level_mosession(value):
    #line= r'NORMAL Fri Jun 04 11:27:59: 14079741,0509940382@shabik.com,2010 6 4 11 27 31,2010 6 4 11 27 59,429,163,212.118.143.147,4018'
    #line=r'NORMAL Sat Jun 05 00:24:51: 14169357,0535840134@shabik.com,2010 6 5 0 24 51,2010 6 5 0 24 51,161,72,212.118.142.75,4018'
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor

    hour=int(value)/3600
    #if hour>24:
    #    hour=24
    return int(hour)        


def is_zero_online_time_mosession(line):
    #line= r'NORMAL Fri Jun 04 11:27:59: 14079741,0509940382@shabik.com,2010 6 4 11 27 31,2010 6 4 11 27 59,429,163,212.118.143.147,4018'
    #line=r'NORMAL Sat Jun 05 00:24:51: 14169357,0535840134@shabik.com,2010 6 5 0 24 51,2010 6 5 0 24 51,161,72,212.118.142.75,4018'
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor

    m=re_get_online_time_mosession.search(line)
    if m and m.group(1) and m.group(2):
        #print m.group(1)
        #print m.group(2)
        #return time.mktime(time.strptime(m.group(2), "%Y %m %d %H %M %S"))== \
        #       time.mktime(time.strptime(m.group(1), "%Y %m %d %H %M %S"))
        return m.group(2)==m.group(1)
    else:
        return True
    
def get_login_time_date_mosession(line):
    #line= r'NORMAL Fri Jun 04 11:27:59: 14079741,0509940382@shabik.com,2010 6 4 11 27 31,2010 6 4 11 27 59,429,163,212.118.143.147,4018'
    #line=r'NORMAL Sat Jun 05 00:24:51: 14169357,0535840134@shabik.com,2010 6 5 0 24 51,2010 6 5 0 24 51,161,72,212.118.142.75,4018'
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor

    m=re_get_online_time_mosession.search(line)
    if m and m.group(1):
        return (datetime.strptime(m.group(1), "%Y %m %d %H %M %S")+timedelta(0,0,0,0,0,config.timezone_offset_vodafone)).strftime('%Y-%m-%d')
    else:
        return None
    
def get_login_time_date_hour_mosession(line):
    #line= r'NORMAL Fri Jun 04 11:27:59: 14079741,0509940382@shabik.com,2010 6 4 11 27 31,2010 6 4 11 27 59,429,163,212.118.143.147,4018'
    #line=r'NORMAL Sat Jun 05 00:24:51: 14169357,0535840134@shabik.com,2010 6 5 0 24 51,2010 6 5 0 24 51,161,72,212.118.142.75,4018'
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor

    m=re_get_online_time_mosession.search(line)
    if m and m.group(1):
        return (datetime.strptime(m.group(1), "%Y %m %d %H %M %S")+timedelta(0,0,0,0,0,config.timezone_offset_vodafone)).strftime('%Y-%m-%d %H')
    else:
        return None


def get_ip_range(line):
    ip1=helper_regex.extract(line,r'(\d+)\.\d+\.\d+\.\d+,').zfill(3)
    ip2=helper_regex.extract(line,r'\d+\.(\d+)\.\d+\.\d+,').zfill(3)
    return ip1+'.'+ip2+'.'


# mosession end



def stat_mosession(my_date):
 
    global group_key_current_day
    global current_day_for_mosession_refined
    global current_day_for_mosession_refined_ceil
    global current_day_for_mosession_refined_floor

    oem_name='Vodafone'
    stat_category='mosession'
    db_name='raw_data_ip'

    group_key_current_day=datetime.fromtimestamp(my_date+3600*config.timezone_offset_vodafone).strftime('%Y-%m-%d')

    current_day_for_mosession_refined=my_date+3600*config.timezone_offset_vodafone
    current_day_for_mosession_refined_ceil=helper_regex.time_ceil_timestamp(current_day_for_mosession_refined,-config.timezone_offset_vodafone)
    current_day_for_mosession_refined_floor=helper_regex.time_floor_timestamp(current_day_for_mosession_refined,-config.timezone_offset_vodafone)
    
    date_str=datetime.fromtimestamp(my_date).strftime('%Y_%m_%d')
    date_next_day_str=datetime.fromtimestamp(my_date+3600*24).strftime('%Y_%m_%d')
    day_str=datetime.fromtimestamp(my_date).strftime('%d')

    print 'group_key_current_day:'+group_key_current_day
    print current_day_for_mosession_refined
    print current_day_for_mosession_refined_ceil
    print current_day_for_mosession_refined_floor
    print date_str
    print date_next_day_str
    print day_str

    print is_valid_login_date_mosession_refined(r'NORMAL Mon May 16 00:15:15: 3008863,mama4love@morange..com,2011 5 15 22 00 00,2010 6 9 1 23 00,500000,500000,95.170.221.101,4018')
    print is_valid_login_date_mosession_refined(r'NORMAL Mon May 16 06:15:15: 3008863,mama4love@morange..com,2011 5 17 05 00 00,2010 6 9 1 23 00,500000,500000,95.170.221.101,4018')
    print is_valid_login_date_mosession_refined(r'NORMAL Mon May 16 00:15:15: 3008863,mama4love@morange..com,2011 5 17 22 00 00,2010 6 10 23 00 00,500000,500000,95.170.221.101,4018')
    
    line='NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 05 35 29,2010 5 24 0 15 15,54046,422473,95.170.221.101,4018'
    print get_login_time_date_hour_mosession(line)
    print get_login_time_date_mosession(line)
    #return

    # NORMAL Mon May 24 00:15:15: 3008863,mama4love@morange.com,2010 5 23 23 35 29,2010 5 24 0 15 15,54046,422473,95.170.221.101,4018
    # log time, monetid, username, in time, out time, inbytes, outbytes, ip, port
    
    stat_plan=Stat_plan(plan_name='daily-mosession-all')

    #All begin

    stat_sql_login_ip_daily_by_ip_range=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'login':r'(NORMAL)'}, \
                                   select_count_distinct_collection={'monet_id':r'NORMAL[^:]+:\d+:\d+: (\d+)'}, \
                                   where={'filtered':is_valid_login_date_mosession_refined}, \
                                   group_by={'daily':get_login_time_date_mosession,'by_ip_range':get_ip_range}, \
                                   db_name=db_name)

    stat_plan.add_stat_sql(stat_sql_login_ip_daily_by_ip_range)

    stat_sql_login_ip_hourly_by_ip_range=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'login':r'(NORMAL)'}, \
                                   select_count_distinct_collection={'monet_id':r'NORMAL[^:]+:\d+:\d+: (\d+)'}, \
                                   where={'filtered':is_valid_login_date_mosession_refined}, \
                                   group_by={'hourly':get_login_time_date_hour_mosession,'by_ip_range':get_ip_range}, \
                                   db_name=db_name)

    stat_plan.add_stat_sql(stat_sql_login_ip_hourly_by_ip_range)

    #All end

    stat_plan.add_log_source(r'\\192.168.1.52\log_vodafone_mosession\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.52\log_vodafone_mosession\MoSession_'+date_next_day_str) 

    stat_plan.add_log_source(r'\\192.168.1.37\log_vodafone_mosession\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.37\log_vodafone_mosession\MoSession_'+date_next_day_str) 

    stat_plan.add_log_source(r'\\192.168.1.71\log_vodafone_mosession\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.71\log_vodafone_mosession\MoSession_'+date_next_day_str) 

    stat_plan.add_log_source(r'\\192.168.1.73\log_vodafone_mosession\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.73\log_vodafone_mosession\MoSession_'+date_next_day_str) 
    

    stat_plan.add_log_source(r'\\192.168.1.74\log_monet_vodafone\MoSession_'+date_str) 
    stat_plan.add_log_source(r'\\192.168.1.74\log_monet_vodafone\MoSession_'+date_next_day_str) 
    
    # todo: telk:175.103.45.20
    
    stat_plan.run()    


if __name__=='__main__':
    
    for i in range(1+config.day_to_update_stat,1,-1):
        stat_mosession(time.time()-3600*24*i)

