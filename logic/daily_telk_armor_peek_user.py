import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import re


group_key_current_day=''

def is_target_date(line):
    global group_key_current_day
    if helper_regex.extract(line,r'^('+group_key_current_day.replace(r'-',r'\-')+')'):
        return True
    return False

def get_target_date(line):
    global group_key_current_day
    return group_key_current_day

def stat_peek_online_user(my_date):
 
    global group_key_current_day

    oem_name='Telk_Armor'
    stat_category='peek_online_user'

    group_key_current_day=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    """
    stat_plan=Stat_plan(plan_name='daily-peek-online-user-telk-armor')

    stat_sql_peek_user=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                           select_max={'peek_online_user':r'"TelkomselOnlineUsers":\s+(\d+)'}, \
                           where={'filtered':is_target_date}, \
                           group_by={'daily':get_target_date})

    stat_plan.add_stat_sql(stat_sql_peek_user)

    stat_plan.add_log_source(r'\\192.168.1.37\checkbot\reports.log') 

    # todo: telk:175.103.45.20

    stat_plan.run()
    return

    """
    for i in range(1,11):

        print 'Try '+str(i)
        try:
            
            stat_plan=Stat_plan(plan_name='daily-peek-online-user-telk-armor')

            stat_sql_peek_user=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_max={'peek_online_user':r'"TelkomselOnlineUsers":\s+(\d+)'}, \
                                   select_average={'average_online_user':r'"TelkomselOnlineUsers":\s+(\d+)'}, \
                                   where={'filtered':is_target_date}, \
                                   group_by={'daily':get_target_date})

            stat_plan.add_stat_sql(stat_sql_peek_user)

            stat_plan.add_log_source(r'\\192.168.1.37\checkbot\reports.log.bak') 

            # todo: telk:175.103.45.20

            stat_plan.run()

        except Exception as e:
            print str(e)
            time.sleep(4)
            continue

        break
    

if __name__=='__main__':
        
    for i in range(config.day_to_update_stat,0,-1):
        stat_peek_online_user(time.time()-3600*24*i)

