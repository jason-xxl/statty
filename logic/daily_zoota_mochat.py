import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_mochat(my_date):
    
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    get_current_date=lambda line:current_date

    oem_name='Zoota'
    stat_category='mochat'
    
    stat_plan=Stat_plan()

    ##### All Begin #####

    stat_sql_send_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'from(\d+)'}, \
                                     where={'send_msg_text':r'(processSendMessagePkt0)'}, \
                                     group_by={'daily':get_current_date}, \
                                     db_name='raw_data_zoota')

    stat_sql_fail_to_send_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                             select_count_distinct_collection={'monet_id':r'id=(\d+)'}, \
                                             where={'fail_to_send_msg':r'(processOnDeliveryFailurePkt)'}, \
                                             group_by={'daily':get_current_date}, \
                                             db_name='raw_data_zoota')
    

    stat_plan.add_stat_sql(stat_sql_send_msg_daily)
    stat_plan.add_stat_sql(stat_sql_fail_to_send_msg_daily)

    ##### All End #####

    stat_plan.add_log_source(r'\\192.168.12.105\logs_mochat\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_mochat(time.time()-3600*24*i)


