import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_mochat(my_date):

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    get_current_date=lambda line:current_date

    oem_name='Vodafone'
    stat_category='matrix'
    
    stat_plan=Stat_plan()

    stat_sql_app_finder_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct={'add_friend_monet_id':r'ADD\s+FINDER\s+(\d+)', \
                                                            'ignore_friend_monet_id':r'IGNORE\s+FINDER\s+(\d+)'}, \
                                     where={'in_app_finder':r'(friendExplStats:\s*\w+\s+FINDER)'}, \
                                     group_by={'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_app_finder_daily)

    stat_sql_app_wizard_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct={'add_friend_monet_id':r'ADD\s+WIZARD\s+(\d+)', \
                                                            'ignore_friend_monet_id':r'IGNORE\s+WIZARD\s+(\d+)'}, \
                                     where={'in_app_wizard':r'(friendExplStats:\s*\w+\s+WIZARD)'}, \
                                     group_by={'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_app_wizard_daily)
    
    stat_plan.add_log_source(r'\\192.168.0.162\matrix-2.0-webapi\matrix.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.0.47\logs_matrix_service_vodafone\matrix.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    
    stat_plan.run()    

    stat_plan.dump_sources()

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_mochat(time.time()-3600*24*i)

