import os, sys

ENGINE_ROOT = os.path.join(os.path.dirname(__file__),'../engine')
sys.path.insert(0, os.path.join(ENGINE_ROOT, "."))

from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
from celery.task import task
import time
import config

@task
def stat_im():
    print "Start CRONTTAB!!!!!!!!!!!!!!!!!"
    oem_name='test'
    stat_category='test'

    stat_plan=Stat_plan()

    # Dec 16 15:17:39 jason sshd[26803]: Received disconnect from 114.141.181.99: 11: Bye Bye

    stat_sql_test=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'action_id':r'\[(\d+)\]'}, \
                                   where={'sshd':r'(\bsshd\b)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})','by_im':r'type=(\w+)'})

    stat_plan.add_stat_sql(stat_sql_test)

    stat_plan.add_log_source(r'/var/log/secure.log')
    
    """
    stat_plan.add_log_source(r'\\192.168.0.135\logs_moim\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')
    stat_plan.add_log_source(r'\\192.168.0.137\logs_moim\morange.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*')
    """

    stat_plan.run()    

'''if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_im(my_date)
'''




