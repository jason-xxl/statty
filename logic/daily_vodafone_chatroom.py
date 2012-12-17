import common_vodafone
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import config
import helper_regex

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date

def stat_chatroom(my_date):

    global current_date
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_vodafone)
    
    # INFO 2010-04-08 00:00:00 - [          workThread] (       CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    # logger.info("[memberFull],iMonetId:"+iMonetId+";iRoomId:" + iRoomId);

    oem_name='Vodafone'
    stat_category='chatroom'
   
    stat_sql_enter_room_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'enter_room':r'(\[doEnterChatroom\])'}, \
                        group_by={'daily':get_current_date})

    stat_sql_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'send_msg':r'(\[send_a_msg\])'}, \
                        group_by={'daily':get_current_date})

   
    stat_sql_enter_room_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'enter_room':r'(\[doEnterChatroom\])'}, \
                        group_by={'daily':get_current_date, \
                                  'by_room_id':r'roomId: (\d+);'})

    stat_sql_msg_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'send_msg':r'(\[send_a_msg\])'}, \
                        group_by={'daily':get_current_date, \
                                  'by_room_id':r'iRoomId:(\d+)'})

    stat_sql_room_full_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId:\s*(\d+)'}, \
                        where={'room_full':r'(\[memberFull\])'}, \
                        group_by={'daily':get_current_date, \
                                  'by_room_id':r'iRoomId:\s*(\d+)'})



    stat_plan=Stat_plan()

    stat_plan.add_stat_sql(stat_sql_enter_room_daily)
    stat_plan.add_stat_sql(stat_sql_msg_daily)

    stat_plan.add_stat_sql(stat_sql_enter_room_by_room_id_daily)
    stat_plan.add_stat_sql(stat_sql_msg_by_room_id_daily)
    stat_plan.add_stat_sql(stat_sql_room_full_by_room_id_daily)



    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.52\logs_vodafone_chatroom\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\logs_chatroom_vodafone\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.76\vodafoneChatroomLog\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    

    stat_plan.run()    

    stat_plan.dump_sources()

    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_chatroom(time.time()-3600*24*i)

    
