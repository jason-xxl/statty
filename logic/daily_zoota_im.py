import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

#from user_id_filter import user_id_filter_globe
#from user_id_filter import user_id_filter_umobile
#from user_id_filter import user_id_filter_ais

def stat_im(my_date):

    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - xawaadleey2@hotmail.com IM1 login ret 0
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index2 IM1 loginTime 5937
    # 2010-04-03 03:00:20,504 [ INFO]     processLogoutPkt - logout client myasseralhamad@yahoo.com monetid=12083269 type=Yahoo    

    oem_name='Zoota'
    stat_category='im'

    stat_plan=Stat_plan()

    ##### ZOOTA Begin #####

    # login by im
    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN

    stat_sql_login_by_im_daily_zoota=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'type=(\w+)'}, \
                                   db_name='raw_data_zoota')

    stat_plan.add_stat_sql(stat_sql_login_by_im_daily_zoota)
    
    # send msg by im
    # 2010-05-11 19:00:01,310 [ INFO]       processChatPkt - monet12997668 IM1 send message for:mero20023002@hotmail.com
    # 2010-07-06 13:00:09,088 [ INFO]       processChatPkt - monet13027186 IM1 send message for:do1409do@hotmail.com

    stat_sql_send_msg_by_im_daily_zoota=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monet(\d+)'}, \
                                   where={'user_send_msg':r'(send message)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'(IM\d+)'}, \
                                   db_name='raw_data_zoota')

    stat_plan.add_stat_sql(stat_sql_send_msg_by_im_daily_zoota)

    # login    
    stat_sql_login_daily_zoota=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'monet_id':r'monetid=(\d+)'}, \
                                   where={'user_try_login':r'(login client)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}, \
                                   db_name='raw_data_zoota')

    stat_plan.add_stat_sql(stat_sql_login_daily_zoota)
    
    ##### ZOOTA End #####


    stat_plan.add_log_source(r'\\192.168.12.101\logs_moim\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')

    stat_plan.run()    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_im(my_date)





