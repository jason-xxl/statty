import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import config
import helper_regex 
import helper_mysql

'''
chain: chat page -> chat room -> send message

note:
1) old and new chat page: logs are on different servers
2) old and new versions:

Exp:
1) only focus on Black Berry 
2) Black Berry Version >= 6.4.0.120405

new chat page: sbk-chatroom.i.morange.com/mobile_chatroom_new.aspx%
old chat page: sub_key_pattern='sbk-chatroom.i.morange.com/mobile_chatroom.aspx%

''' 
    
def get_current_date(line):
    global current_date
    return current_date

def stat_ignore_confirm(my_date):
    global current_date
    
    # INFO 2010-04-08 00:00:00 - [          workThread] (        CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    is_valid_user=lambda line:True

    oem_name='Shabik_360' 
    stat_category='friend_action'
    table_name = 'raw_data_shabik_360'
        
    
    ##################### new chatroom log #########################################
    '''2012-08-14 00:00:47,466 INFO  [(null)] - Ignore userId 45458895 Ignore friendId 46763892'''
    
    
    def get_ignore_userID_friendID(line):
        str_user = helper_regex.extract(line, 'Ignore userId (\d+)')
        str_fri = helper_regex.extract(line, 'Ignore friendId (\d+)')
        return str_user + '_' + str_fri
    
    def get_confirm_userID_friendID(line):
        str_user = helper_regex.extract(line, 'Confirm userId (\d+)')
        str_fri = helper_regex.extract(line, 'add friendId (\d+)')
        return str_user + '_' + str_fri
    
    
    stat_plan=Stat_plan()
    stat_sql_ignore_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'userID_friendID':get_ignore_userID_friendID}, \
                        where={'FR_ignore':r' - (Ignore) '}, \
                        group_by={'daily':lambda line:current_date},
                        db_name=table_name)


    stat_plan.add_stat_sql(stat_sql_ignore_daily)
 
    stat_sql_confirm_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'userID_friendID':get_confirm_userID_friendID}, \
                        where={'FR_confirm':r' - (Confirm) '}, \
                        group_by={'daily':lambda line:current_date},
                        db_name=table_name)


    stat_plan.add_stat_sql(stat_sql_confirm_daily)
    
    ip_list = ['192.168.0.75','192.168.0.107','192.168.0.108','192.168.0.117','192.168.0.118','192.168.0.185','192.168.0.195','192.168.0.196']
    for ip_c in ip_list:
        stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\' + ip_c + r'\mobileweb_shabik_logs\friendship.log%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y%m%d')[0:1])   

    stat_plan.run()

if __name__=='__main__':
    #for i in range(21,0,-1):
    for i in range(config.day_to_update_stat,0,-1):
        stat_ignore_confirm(time.time()-3600*24*i)

    