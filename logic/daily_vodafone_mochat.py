import common_vodafone
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date

"""
def get_msg_id_tuple(line):
    try:
        to=int(helper_regex.extract(line,r'to\D{0,2}(\d+)'))
        msg_id=int(helper_regex.extract(line,r'messageID\D{0,2}(\d+)'))
        #return to+'-'+msg_id
        return (to,msg_id)
    except:
        #print 'get_msg_id_tuple error: ',line
        return (0,0)
"""

def stat_mochat(my_date):

    global current_date
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_vodafone)

    oem_name='Vodafone'
    stat_category='mochat'
    
    stat_plan=Stat_plan()

    ##### All Begin #####

    stat_sql_send_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'from(\d+)'}, \
                                     where={'send_msg_text':r'(processSendMessagePkt0)'}, \
                                     group_by={'daily':get_current_date})
    """
    stat_sql_fail_to_send_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                             select_count_distinct_collection={'monet_id':r'id=(\d+)'}, \
                                             where={'fail_to_send_msg':r'(processOnDeliveryFailurePkt)'}, \
                                             group_by={'daily':get_current_date})
    """
    stat_plan.add_stat_sql(stat_sql_send_msg_daily)
    #stat_plan.add_stat_sql(stat_sql_fail_to_send_msg_daily)

           
    """
    wheres={
    }

    'step1':r'(p)rocessSendMessagePkt[025]\s',
    #'step1-0':r'(p)rocessSendMessagePkt0\s',
    #'step1-2':r'(p)rocessSendMessagePkt2\s',
    #'step1-5':r'(p)rocessSendMessagePkt5\s',

    'step4':r'(p)rocessSendStatusPkt6',
    'step6':r'(p)rocessSendAckPkt6',
    #'step8':r'(p)rocessSendStatusPkt6',
    #'step10':r'(p)rocessSendAckPkt6',

    #'resend':r'(R)esend Message 4',
    #'resend_ack':r'(a)ckResend:2 6 ',

    'no_friend_msg':r'(n)ofriend message',
    'blocked_msg':r'(b)eblocked message',


    for k,v in wheres.iteritems():

        stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                                 select_count_distinct={k+'_id':get_msg_id_tuple}, \
                                                 where={k:v}, \
                                                 group_by={'daily':lambda line:current_date}, \
                                                 db_name='raw_data_debug')

        stat_plan.add_stat_sql(stat_sql)
        print k,v

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                             select_count_distinct_collection={'monet_id':r'\/(\d+).txt'}, \
                                             where={'1_month_abandoned':r'no (w)riteOfflineMessage too old file'}, \
                                             group_by={'daily':lambda line:current_date}, \
                                             db_name='raw_data_debug')

    stat_plan.add_stat_sql(stat_sql)

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                             select_count_distinct_collection={'monet_id':r'\/(\d+).txt'}, \
                                             where={'10_retry_abandoned':r'no (w)riteOfflineMessage sendTimes>10'}, \
                                             group_by={'daily':lambda line:current_date}, \
                                             db_name='raw_data_debug')

    stat_plan.add_stat_sql(stat_sql)
    """

    ##### All End #####

    
    """
    # add filter for recent new user

    filter_2d_new_user=common_vodafone.get_new_user_2d_filter(current_date,pattern=r'(?:from|id=)(\d+)')
    stat_plan.add_stat_brunch_filters({'only_2d_new_user':filter_2d_new_user})
    """
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.52\logs_vodafone_mochat\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.60\logs_mochat_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.67\logs_mochat_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.run()    

    stat_plan.dump_sources()

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_mochat(time.time()-3600*24*i)

