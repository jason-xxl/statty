import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

from user_id_filter import user_id_filter_mozat_client_6
from user_id_filter import user_id_filter_globe
from user_id_filter import user_id_filter_umobile
from user_id_filter import user_id_filter_ais

def stat_mochat(my_date):
    
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    get_current_date=lambda line:current_date

    oem_name='Mozat'
    stat_category='mochat'
    
    stat_plan=Stat_plan()

    ##### All Begin #####

    stat_sql_send_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'from(\d+)'}, \
                                     where={'send_msg_text':r'(processSendMessagePkt0|PMessagePkt0)'}, \
                                     group_by={'daily':get_current_date})

    stat_sql_fail_to_send_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                             select_count_distinct_collection={'monet_id':r'id=(\d+)'}, \
                                             where={'fail_to_send_msg':r'(processOnDeliveryFailurePkt)'}, \
                                             group_by={'daily':get_current_date})
    
    #stat_sql_send_msg_daily_retain_rate=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
    #                                 select_retain_rate_by_date={'monet_id':{'key':r'from(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28],'with_average_life_cycle':True}}, \
    #                                 where={'send_msg_text':r'(processSendMessagePkt0|PMessagePkt0)'}, \
    #                                 group_by={'daily':get_current_date})


    stat_plan.add_stat_sql(stat_sql_send_msg_daily)
    stat_plan.add_stat_sql(stat_sql_fail_to_send_msg_daily)
    #stat_plan.add_stat_sql(stat_sql_send_msg_daily_retain_rate)

    ##### All End #####
    


    stat_plan.add_log_source(r'\\192.168.0.110\logs_mochat\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    stat_plan.add_log_source(r'\\192.168.0.175\logs_mochat_mozat\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    stat_plan.add_log_source(r'\\192.168.0.145\logs_mochat\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    stat_plan.add_log_source(r'\\192.168.0.147\log_mochat_mozat\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    # add mozat 6 filters

    #stat_plan.add_stat_brunch_filters({'client_version_6':user_id_filter_mozat_client_6.is_valid_user})
    #stat_plan.add_stat_brunch_filters({'client_version_before_6':user_id_filter_mozat_client_6.is_not_valid_user})
    stat_plan.add_stat_brunch_filters({'only_globe':user_id_filter_globe.is_valid_user})
    stat_plan.add_stat_brunch_filters({'only_umobile':user_id_filter_umobile.is_valid_user})
    stat_plan.add_stat_brunch_filters({'only_ais':user_id_filter_ais.is_valid_user})

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_mochat(time.time()-3600*24*i)


