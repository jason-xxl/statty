import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import config

from user_id_filter import user_id_filter_umobile
from user_id_filter import user_id_filter_globe
from user_id_filter import user_id_filter_ais

# note: the chatroom node is for stc but contains VIVA partly, need to fileter monet_id

def stat_chatroom(my_date):

    # INFO 2010-04-08 00:00:00 - [          workThread] (       CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    oem_name='Mozat'
    stat_category='chatroom'

    stat_plan=Stat_plan()

    stat_sql_enter_room_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'enter_room':r'(\[doEnterChatroom\])'}, \
                        group_by={'daily':r'INFO ([\s0-9\-]{10})'})

    stat_sql_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'send_msg':r'(\[send_a_msg\])'}, \
                        group_by={'daily':r'INFO ([\s0-9\-]{10})'})

    stat_sql_enter_room_daily_retain_rate=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_retain_rate_by_date={'monet_id':{'key':r'monetId: (\d+)','date_units':[1,2,3,4,5,6,7,14,21,28],'with_average_life_cycle':True}}, \
                        where={'enter_room':r'(\[doEnterChatroom\])'}, \
                        group_by={'daily':r'INFO ([\s0-9\-]{10})'})

    stat_plan.add_stat_sql(stat_sql_enter_room_daily)
    stat_plan.add_stat_sql(stat_sql_msg_daily)
    stat_plan.add_stat_sql(stat_sql_enter_room_daily_retain_rate)

    stat_plan.add_stat_brunch_filters({'only_globe':user_id_filter_globe.is_valid_user})
    stat_plan.add_stat_brunch_filters({'only_umobile':user_id_filter_umobile.is_valid_user})
    stat_plan.add_stat_brunch_filters({'only_ais':user_id_filter_ais.is_valid_user})

    stat_plan.add_log_source(r'\\192.168.0.174\chatroomFast50\chatroom-fast50\logs\service.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    stat_plan.add_log_source(r'\\192.168.0.135\logs\service.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    stat_plan.run()


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_chatroom(time.time()-3600*24*i)

    