import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
'''

Log format:
1 send messafge 
00:00 PMessagePkt0 from30184371to45048090ID1341493099 m: message content

'''

def stat_mochat(my_date):
    
    oem_name='All'
    stat_category='mochat'
    
    stat_plan=Stat_plan()

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)

    #exec('from user_id_filter.user_id_filter_shabik_360_'+current_date.replace('-','_')+' import is_valid_user') in locals(), globals()
    is_valid_user=lambda line:True

    ##### Shabik_360 Begin #####
    
    # 'send_msg_text':r'(processSendMessagePkt0|PMessagePkt0)'

    stat_sql_send_msg_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'from(\d+)'}, \
                                     where={'send_msg_text':r'(PMessagePkt0)','only_shabik_360':is_valid_user}, \
                                     group_by={'daily':lambda line:current_date}, \
                                     db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql_send_msg_daily_shabik_360)

    """
    stat_sql_receive_msg_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'to(\d+)'}, \
                                     where={'receive_msg_text':r'(processSendMessagePkt0|PMessagePkt0)','only_shabik_360':is_valid_user}, \
                                     group_by={'daily':lambda line:current_date}, \
                                     db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql_receive_msg_daily_shabik_360)
    """

    # 'send_msg_text':r'(processSendMessagePkt0|PMessagePkt0)'

    get_from_id_to_id=lambda line:helper_regex.extract(line,r'from(\d+to\d+)')
    
    stat_sql_send_msg_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'from_id_to_id':get_from_id_to_id}, \
                                     where={'send_msg_text':r'(PMessagePkt0)','only_shabik_360':is_valid_user}, \
                                     group_by={'daily':lambda line:current_date}, \
                                     db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql_send_msg_daily_shabik_360)
    
    ##### Shabik_360 End #####
    # removed
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.103\mochat_logs\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    # removed
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.177\logs_mochat_stc\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.191\logs_mochat_stc\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.81\log_mochat_shabik_360\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.run()    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_mochat(time.time()-3600*24*i)



