import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
from user_id_filter import user_id_filter_viva
import time
import config

# note: the chatroom node is for viva but some other data is in stc node, need to aggregate and fileter monet_id
# note: log format of viva is slightly different from other partner's format

def stat_chatroom(my_date):

    # INFO 2010-04-08 00:00:00 - [          workThread] (       CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    # 2010-08-22 00:00:02,266- INFO [workThread] com.mozat.morange.service.chatroom.ClientPacketProcesser.doSendMessage(275) - [send_a_msg], type: text; iMonetId: 21215648; iRoomId:419; message :xxx
    # 2010-08-22 00:00:02,828- INFO [workThread] com.mozat.morange.service.chatroom.ClientPacketProcesser.doEnterChatroom(671) - [doEnterChatroom]; monetId: 13819382; roomId: 415; clientType: mobile; morangeVersion: 

    oem_name='Viva'
    stat_category='chatroom'
    """
    stat_sql_enter_room_hourly=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                                        where={'enter_room':r'(\[doEnterChatroom\])','only_viva':user_id_filter_viva.is_valid_user}, \
                                        group_by={'hourly':r'([\s0-9\-]{13})'})
    """
    stat_sql_enter_room_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'enter_room':r'(\[doEnterChatroom\])','only_viva':user_id_filter_viva.is_valid_user}, \
                        group_by={'daily':r'([\s0-9\-]{10})'})
    """
    stat_sql_msg_hourly=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'send_msg':r'(\[send_a_msg\])','only_viva':user_id_filter_viva.is_valid_user}, \
                        group_by={'hourly':r'([\s0-9\-]{13})'})
    """
    stat_sql_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'send_msg':r'(\[send_a_msg\])','only_viva':user_id_filter_viva.is_valid_user}, \
                        group_by={'daily':r'([\s0-9\-]{10})'})



    stat_plan=Stat_plan()

    #stat_plan.add_stat_sql(stat_sql_enter_room_hourly)
    stat_plan.add_stat_sql(stat_sql_enter_room_daily)
    #stat_plan.add_stat_sql(stat_sql_msg_hourly)
    stat_plan.add_stat_sql(stat_sql_msg_daily)

    stat_plan.add_log_source(r'\\192.168.0.103\ChatroomShabik\logs\service.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.103\chatroom-shabik\logs\service.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.122\chatroom_viva_logs\service.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.run()


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_chatroom(time.time()-3600*24*i)

    