import common_shabik_360
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_math
import config
import helper_mysql
import helper_user_filter

helper_mysql.quick_insert=True

temp={}
user_agent={}

def process_line(line='',exist='',group_key=''):
    global temp,user_agent
    user_id=int(helper_regex.extract(line,r'monetid=(\d+)'))
    type=1 if helper_regex.extract(line,r'(stats\.htm|tab_home)') == 'tab_home' else 2
    t=helper_regex.get_time_stamp(helper_regex.format_date_time_moagent(line))
    if t==-1:
        print 'error line:',line
        return
    temp.setdefault(user_id,[])
    temp[user_id].append((t,type))
    
    ua=helper_regex.extract(line,r'userAgent=(.*?)(?:&|\s*$)')
    if ua:
        user_agent[user_id]=ua

    pass

def stat_moagent(my_date):
    global temp,user_agent
    
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    get_current_date=lambda line:current_date

    oem_name='Vodafone'
    stat_category='moagent'
    table_name='raw_data_device'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-shabik-360')

    # uv

    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   process_exist={'process_line': {'pattern':r'(.)','process':process_line}},                                   
                                   where={'homepage':r'(stats\.htm|tab_home)'}, \
                                   group_by={'daily':lambda line:current_date})

    stat_plan.add_stat_sql(stat_sql_daily)



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
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.73\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.73\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.75\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.75\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.78\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    """
    stat_plan.add_log_source(r'\\192.168.0.108\moAgent\internal_perf.log.2012-01-04-20')
    """

    stat_plan.run()

    print repr(temp)
    print repr(user_agent)

    # calculate 

    total=0
    count=0
    sorted_list=[]

    for user_id,sequence in temp.iteritems():
        temp[user_id]=sorted(temp[user_id])
        t=get_avg_time(sequence)
        if t>-1:
            total+=t
            count+=1
            sorted_list.append((t,user_id))

    if count==0:
        count=1
    
    final_avg=1.0*total/count
    print 'final AVG loading time:',final_avg


    key='client_homepage_loding_time_user_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,current_date,count,table_name=table_name)

    key='client_homepage_loding_time_user_unique_average'
    helper_mysql.put_raw_data(oem_name,stat_category,key,current_date,final_avg,table_name=table_name)


    # calculate <30s


    total=0
    count=0

    for user_id,sequence in temp.iteritems():
        temp[user_id]=sorted(temp[user_id])
        t=get_avg_time(sequence)
        if t>30:
            continue
        if t>-1:
            total+=t
            count+=1

    if count==0:
        count=1
    
    final_avg=1.0*total/count
    print 'final AVG loading time(<=30s):',final_avg


    key='client_homepage_loding_time_le30s_user_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,current_date,count,table_name=table_name)

    key='client_homepage_loding_time_le30s_user_unique_average'
    helper_mysql.put_raw_data(oem_name,stat_category,key,current_date,final_avg,table_name=table_name)


    # dispersion

    
    user_to_time=dict((i[1],int(i[0])) for i in sorted_list)
    dispersion=helper_math.get_simple_dispersion(user_to_time,step=10)
    

    key='client_homepage_loding_time_step_10_dispersion_user_unique'
    for k,unique_user in dispersion[0].iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k,final_avg,table_name=table_name,date=current_date)

        


    # export

    sorted_list=sorted(sorted_list,reverse=True)

    for i in sorted_list:
        user_id=i[1]
        t=i[0]
        print user_id,':',t,',',user_agent.get(user_id,''),temp[user_id]
    


def get_avg_time(sequence):
    start_time=0
    total=0
    count=0
    for i in sequence:
        if start_time==0 and i[1]==1:
            start_time=i[0]
        if start_time>0 and i[1]==1:
            start_time=i[0]
        if start_time==0 and i[1]==2:
            pass
        if start_time>0 and i[1]==2:
            total+=i[0]-start_time
            count+=1
            start_time=0
    return -1 if count==0 else 1.0*total/count        
    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)


