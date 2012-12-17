import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

# to be continue

def stat_login_service(my_date):


    oem_name='Telk_Armor'
    stat_category='login_service'

    stat_plan=Stat_plan()

    ##### Telk_Armor Begin #####

    # responce
    # 2011-01-05 09:00:03,783 [INFO] MoPeerLoginService - 785348346@umniah.jor login result: result=0,peerId=50108434,flag=4

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'- ([^@]+)@morange.com login'}, \
                                   where={'login_response':r'(l)ogin result: result=0', \
                                          'from_free_service':r'(@)morange\.com'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

    ##### Telk_Armor End #####
    
    stat_plan.add_log_source(r'\\192.168.1.36\logs_login_svc\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.100.20\morange\moLoginSvr\logs\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    
    stat_plan.run()    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_login_service(my_date)
