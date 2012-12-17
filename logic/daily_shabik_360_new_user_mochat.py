import common_shabik_360
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_mysql
import config

def stat_mochat(my_date):
    
    oem_name='All'
    stat_category='mochat'
    
    stat_plan=Stat_plan()

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    exec('from user_id_filter.user_id_filter_shabik_360_'+current_date.replace('-','_')+' import is_valid_user') in locals(), globals()

    common_shabik_360.init_user_id_range(current_date)

    user_set=helper_mysql.get_raw_collection_from_key(oem_name=oem_name,category=stat_category, \
                                                        key='only_shabik_360_send_msg_text_daily_monet_id_unique', \
                                                        sub_key='',date=current_date,table_name='raw_data_shabik_360',db_conn=None)

    #user_set=set([user_id for user_id in user_set if is_valid_user('monetid='+user_id)])
    #print len(user_set)

    user_dict=dict((user_id,None) for user_id in user_set)
    user_dict_grouped=common_shabik_360.get_categorized_groups(user_dict)

    for k,v in user_dict_grouped.iteritems():
        key='only_shabik_360_send_msg_text_by_user_group_daily_monet_id_unique'
        print len(v)
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+current_date,len(v),table_name='raw_data_shabik_360')

    """
    ##### Shabik_360 Begin #####
    
    stat_sql_send_msg_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct={'monet_id':r'from(\d+)'}, \
                                     select_retain_rate_by_date={'monet_id':{'key':r'from(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                     where={'send_msg_text':r'(processSendMessagePkt0)','only_shabik_360':is_valid_user}, \
                                     group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                               'by_user_group':lambda line:common_shabik_360.get_user_group_name_by_user_id(helper_regex.extract(line,r'from(\d+)'))}, \
                                     db_name='raw_data_shabik_360')
    
    
    stat_sql_fail_to_send_msg_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                             select_count_distinct={'monet_id':r'id=(\d+)'}, \
                                             where={'fail_to_send_msg':r'(processOnDeliveryFailurePkt)','only_shabik_360':is_valid_user}, \
                                             group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                                       'by_user_group':lambda line:common_shabik_360.get_user_group_name_by_user_id(helper_regex.extract(line,r'id=(\d+)'))}, \
                                             db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_send_msg_daily_shabik_360)
    stat_plan.add_stat_sql(stat_sql_fail_to_send_msg_daily_shabik_360)

    ##### Shabik_360 End #####


    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.103\mochat_logs\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.177\logs_mochat_stc\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.run()    

    """

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_mochat(time.time()-3600*24*i)



