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

def stat_im(my_date):

    global current_date
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_vodafone)

    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - xawaadleey2@hotmail.com IM1 login ret 0
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index2 IM1 loginTime 5937
    # 2010-04-03 03:00:20,504 [ INFO]     processLogoutPkt - logout client myasseralhamad@yahoo.com monetid=12083269 type=Yahoo    

    oem_name='Vodafone'
    stat_category='im'

    stat_plan=Stat_plan()

    # login by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN

    stat_sql_login_by_im_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)'}, \
                                   group_by={'daily':get_current_date,'by_im':r'type=(\w+)'})

    stat_plan.add_stat_sql(stat_sql_login_by_im_daily)

    # send msg by im
    # 2010-05-11 19:00:01,310 [ INFO]       processChatPkt - monet12997668 IM1 send message for:mero20023002@hotmail.com

    stat_sql_send_msg_by_im_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)'}, \
                                   group_by={'daily':get_current_date,'by_im':r'(IM\d+)'})

    stat_plan.add_stat_sql(stat_sql_send_msg_by_im_daily)

    # login    

    stat_sql_login_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)'}, \
                                   group_by={'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_login_daily)

    # login result by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - xawaadleey2@hotmail.com IM1 login ret 0

    stat_sql_login_result_by_im_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'login_result':r'(.)'}, \
                                   where={'user_login':r'(IM\d+ login ret \d+)'}, \
                                   group_by={'daily':get_current_date,'by_im':r'(IM\d+) login ret','result':r'login ret (\d+)'})

    stat_plan.add_stat_sql(stat_sql_login_result_by_im_daily)

    # login process time by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index2 IM1 loginTime 5937

    stat_sql_login_wait_by_im_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_average={'login_wait':r'loginTime (\d+)'}, \
                                   where={'user_login':r'(loginTime)'}, \
                                   group_by={'daily':get_current_date,'by_im':r'Index\d+ (IM\d+)'})

    #stat_plan.add_stat_sql(stat_sql_login_wait_by_im_daily)


    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.67\logs_vodafone_im\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.52\logs_vodafone_im\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\logs_im_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.73\logs_im_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_im_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_im_vodafone_2\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.75\logs_im_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.78\logs_im_vodafone\morange.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))
    

    stat_plan.run()    

    stat_plan.dump_sources()


if __name__=='__main__':

    
    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_im(my_date)
