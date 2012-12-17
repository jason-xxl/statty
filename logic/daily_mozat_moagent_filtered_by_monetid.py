import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_mysql
import helper_ip
import config

helper_mysql.quick_insert=True

from user_id_filter import user_id_filter_ais

current_date=''

def get_current_date(line):
    global current_date
    return current_date

def stat_moagent(my_date):
    global current_date
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    oem_name='Mozat'
    stat_category='moagent'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-mozat')
    
    target_monet_id_set=set(['46843477','46843475','46843474','46843467','46843463','46843461','46843459','46843455','46843453','46843432','46843412','46843410','46843402','13878472'])
    target_log=[]
    
    def log_record(line):
        monet_id=helper_regex.extract(line,r'monetid=(\d+)')
        if monet_id in target_monet_id_set:
            target_log.append(monet_id+' '+line)
        return 1

    # uv
    
    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'action':log_record}, \
                                   group_by={'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_daily)
    
    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct={'monet_id':r'monetid=(\d+)'}, \
                                   where={'by_special_ais_tag':r'(AddFriendsGuide|FriendExplorer|DoAddByPIN|DoAddByMobile|DoAddById)'}, \
                                   group_by={'daily':get_current_date, \
                                             'by_arg':r'(AddFriendsGuide|FriendExplorer|DoAddByPIN|DoAddByMobile|DoAddById)'})

    stat_plan.add_stat_sql(stat_sql_daily)

    #stat_plan.add_log_source(r'\\192.168.0.113\logs_moagent\internal_perf.log.' \
    #       +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    #stat_plan.add_log_source(r'\\192.168.0.104\logs_moagent\internal_perf.log.' \
    #       +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    #stat_plan.add_log_source(r'\\192.168.0.111\logs_moagent\internal_perf.log.' \
    #       +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    #stat_plan.add_log_source(r'\\192.168.0.162\logs_moagent_mozat\internal_perf.log.' \
    #       +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    """
    stat_plan.add_log_source(r'\\192.168.0.147\logs_moagent_mozat\internal_perf.log.2012-06-20-01')
    """

    stat_plan.add_log_source(r'\\192.168.0.147\logs_moagent_mozat\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.157\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.181\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.182\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.79\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.135\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.137\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.dump_sources()

    stat_plan.run()

    target_log=sorted(target_log)
    for i in target_log:
        print i.replace('\n','')

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)



