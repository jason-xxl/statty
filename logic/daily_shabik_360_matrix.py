import common_shabik_360
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mysql
import helper_user_filter


helper_mysql.quick_insert=True

def stat_moagent(my_date):

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    get_current_date=lambda line:current_date

    oem_name='Shabik_360'
    stat_category='matrix'
    db_name='raw_data_shabik_360'
    
    stat_plan=Stat_plan(plan_name='daily-moagent-shabik-360')

    # log format
    #2012-05-24 07:03:22,660 [INFO] com.mozat.matrix.AutoFriend: 475 - AutoActive: userId: 46508924,friendId: 45326764,dmoain: shabik.com
    #2012-05-24 07:03:22,660 [INFO] com.mozat.matrix.AutoFriend: 475 - AutoActive: userId: 46508924,friendId: 35620962,dmoain: shabik.com
    #2012-05-24 07:03:22,660 [INFO] com.mozat.matrix.AutoFriend: 475 - AutoActive: userId: 46508924,friendId: 44737238,dmoain: shabik.com
    #2012-05-24 07:03:22,660 [INFO] com.mozat.matrix.AutoFriend: 475 - AutoActive: userId: 46508924,friendId: 44910511,dmoain: shabik.com
    #2012-05-24 07:03:22,660 [INFO] com.mozat.matrix.AutoFriend: 475 - AutoActive: userId: 46508924,friendId: 45785243,dmoain: shabik.com

    stat_sql_unique_auto_add_friend_from_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user_from':r'userId: (\d+)', \
                                                                     'user_to':r'friendId: (\d+)'}, \
                                   where={'auto_add_mutual_friend':r'(A)utoActive: userId:.*?(?:shabik\.com)'}, \
                                   group_by={'daily':lambda line:current_date},
                                   db_name=db_name)

    stat_plan.add_stat_sql(stat_sql_unique_auto_add_friend_from_daily)

    # 47
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.47\logs_matrix_shabik_360\matrix.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    # 162
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.162\matrix-2.0-webapi\matrix.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
    stat_plan.run()


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)


