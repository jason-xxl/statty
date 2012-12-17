
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

def stat_website_login(my_date):

    #2010-10-04 02:10:03,186 [8] INFO  Morange.MoZone.App.UserManager - UserLogin: 26674988

    oem_name='Viva_BH'
    stat_category='website_login'

    stat_plan=Stat_plan()
    
    stat_sql_web_login_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'UserLogin: (\d+)'}, \
                                   where={'website_login':r'(M)orange\.MoZone\.App\.UserManager \- UserLogin:'}, \
                                   group_by={'daily':r'([\d\-]{10})'})
    
    stat_plan.add_stat_sql(stat_sql_web_login_daily)

    stat_plan.add_log_source(r'\\192.168.0.140\logs_website_login\all.log' \
           +datetime.fromtimestamp(my_date).strftime('%Y%m%d'))

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_website_login(my_date)
