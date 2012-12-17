import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_im(my_date):


    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - xawaadleey2@hotmail.com IM1 login ret 0
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index2 IM1 loginTime 5937
    # 2010-04-03 03:00:20,504 [ INFO]     processLogoutPkt - logout client myasseralhamad@yahoo.com monetid=12083269 type=Yahoo    

    oem_name='All'
    stat_category='im'

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)

    #exec('from user_id_filter.user_id_filter_shabik_360_'+current_date.replace('-','_')+' import is_valid_user') in locals(), globals()
    is_valid_user=lambda line:True

    stat_plan=Stat_plan()


    ##### Shabik_360 Begin #####

    # login by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN

    
    stat_sql_login_by_im_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)','only_shabik_360':is_valid_user}, \
                                   group_by={'daily':lambda line:current_date,'by_im':r'type=(\w+)'}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_login_by_im_daily_shabik_360)
    
    # send msg by im
    # 2010-05-11 19:00:01,310 [ INFO]       processChatPkt - monet12997668 IM1 send message for:mero20023002@hotmail.com
    # 2010-07-06 13:00:09,088 [ INFO]       processChatPkt - monet13027186 IM1 send message for:do1409do@hotmail.com

    
    stat_sql_send_msg_by_im_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)','only_shabik_360':is_valid_user}, \
                                   group_by={'daily':lambda line:current_date,'by_im':r'(IM\d+)'}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_send_msg_by_im_daily_shabik_360)
    
    # send msg
    # 2010-05-11 19:00:01,310 [ INFO]       processChatPkt - monet12997668 IM1 send message for:mero20023002@hotmail.com
    # 2010-07-06 13:00:09,088 [ INFO]       processChatPkt - monet13027186 IM1 send message for:do1409do@hotmail.com

    
    stat_sql_send_msg_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)','only_shabik_360':is_valid_user}, \
                                   group_by={'daily':lambda line:current_date}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_send_msg_daily_shabik_360)

    # login    

    stat_sql_login_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)','only_shabik_360':is_valid_user}, \
                                   group_by={'daily':lambda line:current_date}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_login_daily_shabik_360)

    """
    stat_sql_login_daily_retain_rate_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_retain_rate_by_date={'monet_id':{'key':r'monetid=(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                   where={'user_try_login':r'(login client)','only_shabik_360':is_valid_user}, \
                                   group_by={'daily':lambda line:current_date}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_login_daily_retain_rate_shabik_360)
    """

    """
    # male
        
    stat_sql_send_msg_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)', \
                                          'only_shabik_360':is_valid_user, \
                                          'only_male':user_id_filter_stc_male.is_valid_user}, \
                                   group_by={'daily':lambda line:current_date}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_send_msg_daily_shabik_360)


    # female
        
    stat_sql_send_msg_daily_shabik_360=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)', \
                                          'only_shabik_360':is_valid_user, \
                                          'only_female':user_id_filter_stc_female.is_valid_user}, \
                                   group_by={'daily':lambda line:current_date}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_send_msg_daily_shabik_360)
    
    """

    ##### Shabik_360 End #####



    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.100\moim_logs\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.79\logsMoIM\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.75\D_morange\moim\logs\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.107\F_morange\moim\logs\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.108\F_morange\moim\logs\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.117\F_morange\moim\logs\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.118\F_morange\moim\logs\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.185\logs_moim_shabik_360\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.196\logs_moim_shabik_360\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_im(my_date)

