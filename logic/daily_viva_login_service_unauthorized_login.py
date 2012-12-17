import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

# to be continue

def stat_login_service(my_date):


    oem_name='Viva'
    stat_category='login_service'

    stat_plan=Stat_plan()

    ##### Viva Begin #####

    # responce
    # 2011-01-05 04:00:03,735 [INFO] MoPeerLoginService - 55651596@viva login result: result=0,peerId=22763589,flag=4

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'- ([^@]+)@morange.com login'}, \
                                   where={'login_response':r'(l)ogin result: result=0', \
                                          'from_free_service':r'(@)morange\.com'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

    ##### Viva End #####
    
    stat_plan.add_log_source(r'\\192.168.0.122\applications\moLoginSvr\logs\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') # viva
    
    stat_plan.run()    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_login_service(my_date)
