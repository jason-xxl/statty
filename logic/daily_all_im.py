import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_stc
from user_id_filter import user_id_filter_viva
from user_id_filter import user_id_filter_viva_bh
import config



def stat_im(my_date):


    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - xawaadleey2@hotmail.com IM1 login ret 0
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index2 IM1 loginTime 5937
    # 2010-04-03 03:00:20,504 [ INFO]     processLogoutPkt - logout client myasseralhamad@yahoo.com monetid=12083269 type=Yahoo    

    oem_name='All'
    stat_category='im'


    stat_plan=Stat_plan()



    ##### All Begin #####
    """
    # login by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN

    
    stat_sql_login_by_im_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'type=(\w+)'})

    stat_plan.add_stat_sql(stat_sql_login_by_im_daily)

    # send msg by im
    # 2010-05-11 19:00:01,310 [ INFO]       processChatPkt - monet12997668 IM1 send message for:mero20023002@hotmail.com
    
    
    stat_sql_send_msg_by_im_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'(IM\d+)'})

    stat_plan.add_stat_sql(stat_sql_send_msg_by_im_daily)

    # login    

    stat_sql_login_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

    stat_plan.add_stat_sql(stat_sql_login_daily)

    # login result by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - xawaadleey2@hotmail.com IM1 login ret 0
    

    stat_sql_login_result_by_im_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'login_result':r'(.)'}, \
                                   where={'user_login':r'(IM\d+ login ret \d+)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'(IM\d+) login ret','result':r'login ret (\d+)'})

    stat_plan.add_stat_sql(stat_sql_login_result_by_im_daily)

    # login process time by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index2 IM1 loginTime 5937
    
    stat_sql_login_wait_by_im_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_average={'login_wait':r'loginTime (\d+)'}, \
                                   where={'user_login':r'(loginTime)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'Index\d+ (IM\d+)'})

    stat_plan.add_stat_sql(stat_sql_login_wait_by_im_daily)

    stat_sql_login_daily_retain_rate=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_retain_rate_by_date={'monet_id':{'key':r'monetid=(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                   where={'user_try_login':r'(login client)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

    stat_plan.add_stat_sql(stat_sql_login_daily_retain_rate)
    """
    ##### All End #####



    ##### STC Begin #####

    # login by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN

    
    stat_sql_login_by_im_daily_stc=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)','only_stc':user_id_filter_stc.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'type=(\w+)'})

    stat_plan.add_stat_sql(stat_sql_login_by_im_daily_stc)
    
    # send msg by im
    # 2010-05-11 19:00:01,310 [ INFO]       processChatPkt - monet12997668 IM1 send message for:mero20023002@hotmail.com
    # 2010-07-06 13:00:09,088 [ INFO]       processChatPkt - monet13027186 IM1 send message for:do1409do@hotmail.com

    
    stat_sql_send_msg_by_im_daily_stc=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)','only_stc':user_id_filter_stc.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'(IM\d+)'})

    stat_plan.add_stat_sql(stat_sql_send_msg_by_im_daily_stc)

    # login    

    stat_sql_login_daily_stc=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)','only_stc':user_id_filter_stc.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

    stat_plan.add_stat_sql(stat_sql_login_daily_stc)


    stat_sql_login_daily_retain_rate_stc=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_retain_rate_by_date={'monet_id':{'key':r'monetid=(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                   where={'user_try_login':r'(login client)','only_stc':user_id_filter_stc.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

    stat_plan.add_stat_sql(stat_sql_login_daily_retain_rate_stc)

    ##### STC End #####


    ##### Viva Begin #####

    # login by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN
    
    stat_sql_login_by_im_daily_viva=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)','only_viva':user_id_filter_viva.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'type=(\w+)'})

    stat_plan.add_stat_sql(stat_sql_login_by_im_daily_viva)
    
    # send msg by im
    # 2010-05-11 19:00:01,310 [ INFO]       processChatPkt - monet12997668 IM1 send message for:mero20023002@hotmail.com
    
    
    stat_sql_send_msg_by_im_daily_viva=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)','only_viva':user_id_filter_viva.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'(IM\d+)'})

    stat_plan.add_stat_sql(stat_sql_send_msg_by_im_daily_viva)

    # login    


    stat_sql_login_daily_viva=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)','only_viva':user_id_filter_viva.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

    stat_plan.add_stat_sql(stat_sql_login_daily_viva)


    stat_sql_login_daily_retain_rate_viva=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_retain_rate_by_date={'monet_id':{'key':r'monetid=(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                   where={'user_try_login':r'(login client)','only_viva':user_id_filter_viva.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

    stat_plan.add_stat_sql(stat_sql_login_daily_retain_rate_viva)

    ##### Viva End #####






    ##### Viva Bh Begin #####

    # login by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN

    stat_sql_login_by_im_daily_viva_bh=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)','only_viva_bh':user_id_filter_viva_bh.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'type=(\w+)'})

    stat_plan.add_stat_sql(stat_sql_login_by_im_daily_viva_bh)
    
    # send msg by im
    # 2010-05-11 19:00:01,310 [ INFO]       processChatPkt - monet12997668 IM1 send message for:mero20023002@hotmail.com
    
    
    stat_sql_send_msg_by_im_daily_viva_bh=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)','only_viva_bh':user_id_filter_viva_bh.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'(IM\d+)'})

    stat_plan.add_stat_sql(stat_sql_send_msg_by_im_daily_viva_bh)

    # login    

    stat_sql_login_daily_viva_bh=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)','only_viva_bh':user_id_filter_viva_bh.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

    stat_plan.add_stat_sql(stat_sql_login_daily_viva_bh)


    stat_sql_login_daily_retain_rate_viva_bh=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_retain_rate_by_date={'monet_id':{'key':r'monetid=(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                   where={'user_try_login':r'(login client)','only_viva_bh':user_id_filter_viva_bh.is_valid_user}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

    stat_plan.add_stat_sql(stat_sql_login_daily_retain_rate_viva_bh)

    ##### Viva Bh End #####

    stat_plan.add_log_source(r'\\192.168.0.100\moim_logs\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')
    stat_plan.add_log_source(r'\\192.168.0.79\logsMoIM\morange.log.' \
                 +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
                 +'*')
    stat_plan.add_log_source(r'\\192.168.0.75\D_morange\moim\logs\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')
    stat_plan.add_log_source(r'\\192.168.0.107\F_morange\moim\logs\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')
    stat_plan.add_log_source(r'\\192.168.0.108\F_morange\moim\logs\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')
    stat_plan.add_log_source(r'\\192.168.0.117\F_morange\moim\logs\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')
    stat_plan.add_log_source(r'\\192.168.0.118\F_morange\moim\logs\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')
    stat_plan.add_log_source(r'\\192.168.0.185\logs_moim_shabik_360\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')
    stat_plan.add_log_source(r'\\192.168.0.196\logs_moim_shabik_360\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_im(my_date)

