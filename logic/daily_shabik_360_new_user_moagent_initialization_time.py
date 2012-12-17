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
import datetime

helper_mysql.quick_insert=True

def is_initializing_link(url_key):
    if url_key.find('evflg')>-1 and url_key.find('isprefetch_1')==-1:
        return False
    if url_key.find('isprefetch_1')>-1 or url_key.find('get_invitee_list.ashx')>-1 \
            or url_key.find('action_get_reqs')>-1 or url_key.find('tab-overdue')>-1 \
            or url_key.find('tab_home.aspx')>-1 or url_key.find('tab_apps.aspx')>-1:
        return True
    return False


def process_line(line='',exist='',group_key=''):

    global first_access_time,first_user_triggered_urls,last_initializing_triggered_urls,first_user_trigger_time
    monet_id=helper_regex.extract(line,r'monetid=(\d+)')
    url_key=intern(str(helper_regex.get_simplified_url_unique_key(line.replace('isprefetch=','isprefetch_')).encode('UTF-8')))
    t=helper_regex.extract(line,r'([^,]+)')

    if is_initializing_link(url_key):
        first_access_time.setdefault(monet_id,t)
        if t<first_access_time[monet_id]:
            first_access_time[monet_id]=t

        first_user_triggered_urls.setdefault(monet_id,{})
        if len(first_user_triggered_urls[monet_id])<10:
            first_user_triggered_urls[monet_id][t]=url_key
        
    else:
        first_user_trigger_time.setdefault(monet_id,t)
        if t<first_user_trigger_time[monet_id]:
            first_user_trigger_time[monet_id]=t

        last_initializing_triggered_urls.setdefault(monet_id,{})
        if len(last_initializing_triggered_urls[monet_id])<10:
            last_initializing_triggered_urls[monet_id][t]=url_key

def get_user_id_init_time(line):

    #user:first access:first trigger 40078422 24 Nov 02:47:50 24 Nov 02:53:48
    
    user_id=helper_regex.extract(line,r'first trigger (\d+)')
    
    time_1=helper_regex.extract(line,r'first trigger \d+ (\d+ \w+ \d+:\d+:\d+)')
    time_2=helper_regex.extract(line,r'first trigger \d+ (?:\d+ \w+ \d+:\d+:\d+|\-) (\d+ \w+ \d+:\d+:\d+)')

    if time_1 and time_2:

        # 24 Nov 00:35:42 2011
        time_1=datetime.datetime.strptime(time_1+' '+datetime.datetime.fromtimestamp(time.time()-3600*24).strftime('%Y'),'%d %b %H:%M:%S %Y')
        time_2=datetime.datetime.strptime(time_2+' '+datetime.datetime.fromtimestamp(time.time()-3600*24).strftime('%Y'),'%d %b %H:%M:%S %Y')
        init_time=(time_2-time_1).seconds
    
    elif time_1:
        init_time=-10000
    else: 
        init_time=10000

    return user_id,init_time

def stat_moagent(my_date):

    global first_access_time,first_user_triggered_urls,first_user_trigger_time,last_initializing_triggered_urls
    
    first_access_time={}
    first_user_triggered_urls={}
    first_user_trigger_time={}
    last_initializing_triggered_urls={}

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    exec('from user_id_filter.user_id_filter_shabik_360_'+current_date.replace('-','_')+' import is_valid_user') in locals(), globals()
    
    #print current_date
    #common_shabik_360.init_user_id_range(current_date)

    oem_name='Shabik_360'
    stat_category='moagent'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880	390	15	32	15	328	3648	http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-shabik-360')

    start_time=helper_regex.date_add(current_date,0)+' 05:00:00'
    end_time=helper_regex.date_add(current_date,1)+' 05:00:00'

    if_new_user=common_shabik_360.get_new_user_by_time_range(start_time,end_time,r'monetid=(\d+)')

    get_url_key=lambda line:helper_regex.get_simplified_url_unique_key(line.replace('isprefetch=','isprefetch_'))

    stat_sql_url_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   process_exist={'process_line': {'pattern':r'(.)','process':process_line}},                                   
                                   where={'if_new_user':if_new_user}, \
                                   db_name='data_url_pattern_shabik_360', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_url_daily)
    

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.174\logs_moagent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.177\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.117\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.118\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.75\logs_moagent_shabik_360\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.107\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.108\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))



    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.195\logs_moagent_shabik_360\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.185\logs_moagent_shabik_360\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.196\logs_moagent_shabik_360\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    """
    stat_plan.add_log_source_list([r'\\192.168.0.108\moAgent\internal_perf.log.2011-12-04-12'])
    
    prefixs=[
        r'\\192.168.0.174\logs_moagent\internal_perf.log.',
        r'\\192.168.0.177\moAgent\internal_perf.log.',
        r'\\192.168.0.117\moAgent\internal_perf.log.',
        r'\\192.168.0.118\moAgent\internal_perf.log.',
        r'\\192.168.0.75\logs_moagent_shabik_360\internal_perf.log.',
        r'\\192.168.0.107\moAgent\internal_perf.log.',
        r'\\192.168.0.108\moAgent\internal_perf.log.',
    ]

    for prefix in prefixs:
        for i in ['11-30-23','12-01-00','12-01-01','12-01-02','12-01-03','12-01-04','12-01-05',]:
            stat_plan.add_log_source(prefix+'2011-'+i)
    """

    stat_plan.dump_sources()

    stat_plan.run()

    #calculate

    init_time_dict={}

    for user_id in first_access_time.keys():
        line='user:first access:first trigger ' + user_id + ' ' + (first_access_time[user_id] if first_access_time.has_key(user_id) else '-')  \
            + ' ' + (first_user_trigger_time[user_id] if first_user_trigger_time.has_key(user_id) else '-') \
            + ' ' + 'first_user_triggered_urls: '+str(first_user_triggered_urls[user_id] if first_user_triggered_urls.has_key(user_id) else '-') \
            + ' ' + 'last_initializing_triggered_urls: ' \
            + ' ' + str(last_initializing_triggered_urls[user_id] if last_initializing_triggered_urls.has_key(user_id) else '-')

        print line
        user_id,init_time=get_user_id_init_time(line)
        init_time_dict[user_id]=init_time


    result=sorted([(init_time_dict[user_id],user_id) for user_id in init_time_dict.keys() if user_id and int(user_id)>100000])
    
    #for i in result:
    #    print i

    result_groups={
        'lt0':set([user_id for init_time,user_id in result if init_time<0]),
        'ge0_lt30':set([user_id for init_time,user_id in result if init_time>=0 and init_time<=30]),
        'ge30_lt120':set([user_id for init_time,user_id in result if init_time>30 and init_time<=120]),
        'ge120_lt1000':set([user_id for init_time,user_id in result if init_time>120 and init_time<=1000]),
        'ge1000':set([user_id for init_time,user_id in result if init_time>1000]),
    }

    #put to collection

    for k,v in result_groups.iteritems():
        helper_mysql.put_collection(collection=v,oem_name=oem_name,category=stat_category, \
                                    key='new_user_by_init_time_unique',sub_key=k, \
                                    table_name='raw_data_shabik_360',date=current_date)
    
    for k,v in result_groups.iteritems():
        print k,len(v)
    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)


