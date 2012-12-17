import common_shabik_360 
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import config
import helper_regex 


# note: the chatroom node is for stc but contains VIVA partly, need to fileter monet_id

def get_current_date(line):
    global current_date
    return current_date

def stat_chatroom(my_date):
    global current_date
    
    # INFO 2010-04-08 00:00:00 - [          workThread] (        CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    exec('from user_id_filter.user_id_filter_shabik_360_'+current_date.replace('-','_')+' import is_valid_user') in locals(), globals()

    common_shabik_360.init_user_id_range(current_date)

    oem_name='Shabik_360' 
    stat_category='chatroom'

    stat_plan=Stat_plan()

    stat_sql_enter_room_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct={'monet_id':r'monetId: (\d+)'}, \
                        #select_retain_rate_by_date={'monet_id':{'key':r'monetId: (\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                        where={'enter_room':r'(\[doEnterChatroom\])','only_shabik_360':is_valid_user}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_user_group':lambda line:common_shabik_360.get_user_group_name_by_user_id(helper_regex.extract(line,r'monetId: (\d+)'))},
                        db_name='raw_data_shabik_360')

    stat_sql_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'send_msg':r'(\[send_a_msg\])','only_shabik_360':is_valid_user}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_user_group':lambda line:common_shabik_360.get_user_group_name_by_user_id(helper_regex.extract(line,r'iMonetId: (\d+)'))},
                        db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_enter_room_daily)
    stat_plan.add_stat_sql(stat_sql_msg_daily)

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.103\ChatroomShabik\logs\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.103\chatroom-shabik\logs\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.118\logs_chatroom_stc\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.run()


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_chatroom(time.time()-3600*24*i)

    