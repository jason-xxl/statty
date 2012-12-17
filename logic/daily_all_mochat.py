import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_stc
from user_id_filter import user_id_filter_viva
from user_id_filter import user_id_filter_viva_bh
import config

def stat_mochat(my_date):
    
    oem_name='All'
    stat_category='mochat'
    
    stat_plan=Stat_plan()

    ##### All Begin #####
    
    """
    stat_sql_send_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'from(\d+)'}, \
                                     select_retain_rate_by_date={'monet_id':{'key':r'from(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                     where={'send_msg_text':r'(processSendMessagePkt0)'}, \
                                     group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})
    
    
    stat_sql_fail_to_send_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                             select_count_distinct_collection={'monet_id':r'id=(\d+)'}, \
                                             where={'fail_to_send_msg':r'(processOnDeliveryFailurePkt)'}, \
                                             group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

    stat_plan.add_stat_sql(stat_sql_send_msg_daily)
    stat_plan.add_stat_sql(stat_sql_fail_to_send_msg_daily)
    """

    ##### All End #####
    

    ##### STC Begin #####
    
    stat_sql_send_msg_daily_stc=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'from(\d+)'}, \
                                     select_retain_rate_by_date={'monet_id':{'key':r'from(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                     where={'send_msg_text':r'(processSendMessagePkt0|PMessagePkt0)','only_stc':user_id_filter_stc.is_valid_user}, \
                                     group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})
    
    
    
    stat_plan.add_stat_sql(stat_sql_send_msg_daily_stc)

    ##### STC End #####

    ##### Viva Begin #####

    
    stat_sql_send_msg_daily_viva=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'from(\d+)'}, \
                                     select_retain_rate_by_date={'monet_id':{'key':r'from(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                     where={'send_msg_text':r'(processSendMessagePkt0|PMessagePkt0)','only_viva':user_id_filter_viva.is_valid_user}, \
                                     group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})


    stat_plan.add_stat_sql(stat_sql_send_msg_daily_viva)

    ##### Viva End #####

    ##### Viva Bh Begin #####
    
    stat_sql_send_msg_daily_viva_bh=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'from(\d+)'}, \
                                     select_retain_rate_by_date={'monet_id':{'key':r'from(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                     where={'send_msg_text':r'(processSendMessagePkt0|PMessagePkt0)','only_viva_bh':user_id_filter_viva_bh.is_valid_user}, \
                                     group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})
    

    stat_plan.add_stat_sql(stat_sql_send_msg_daily_viva_bh)

    ##### Viva Bh End #####
    

    stat_plan.add_log_source(r'\\192.168.0.103\mochat_logs\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    stat_plan.add_log_source(r'\\192.168.0.177\logs_mochat_stc\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    stat_plan.add_log_source(r'\\192.168.0.191\logs_mochat_stc\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_mochat(time.time()-3600*24*i)



