import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import config
import helper_regex 

raise DeprecationWarning('no longer used')

def get_current_date(line):
    global current_date
    return current_date

def stat_chatroom(my_date):
    global current_date
    
    # INFO 2010-04-08 00:00:00 - [          workThread] (        CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    is_valid_user=lambda line:True

    oem_name='Shabik_360' 
    stat_category='chatroom'
    table_name = 'raw_data_shabik_360'
    
    stat_plan=Stat_plan()

    stat_sql_enter_room_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'new_chatroom_enter_room':r'(\[doEnterChatroom\])','only_shabik_360':is_valid_user}, \
                        group_by={'daily':lambda line:current_date},
                        db_name=table_name)

    stat_sql_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'new_chatroom_send_msg':r'(\[send_a_msg\])','only_shabik_360':is_valid_user}, \
                        group_by={'daily':lambda line:current_date},
                        db_name=table_name)
   
    stat_sql_enter_room_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'new_chatroom_enter_room':r'(\[doEnterChatroom\])'}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_room_id':r'roomId: (\d+);'},
                        db_name=table_name)

    stat_sql_msg_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'new_chatroom_send_msg':r'(\[send_a_msg\])'}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_room_id':r'iRoomId:(\d+)'},
                        db_name=table_name)

    stat_sql_room_full_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId:\s*(\d+)'}, \
                        where={'new_chatroom_room_full':r'(\[memberFull\])'}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_room_id':r'iRoomId:\s*(\d+)'},
                        db_name=table_name)

    stat_plan.add_stat_sql(stat_sql_enter_room_daily)
    stat_plan.add_stat_sql(stat_sql_msg_daily)
    stat_plan.add_stat_sql(stat_sql_enter_room_by_room_id_daily)
    stat_plan.add_stat_sql(stat_sql_msg_by_room_id_daily)
    stat_plan.add_stat_sql(stat_sql_room_full_by_room_id_daily)

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.177\shabik_newchatroom_logs\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
#    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
#                                        my_date,r'.\log\service.log.%(date)s-%(hour)s', \
#                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
    #stat_plan.add_log_source(r'\\192.168.0.177\shabik_newchatroom_logs\service.log.'+datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    stat_plan.run()


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_chatroom(time.time()-3600*24*i)

    