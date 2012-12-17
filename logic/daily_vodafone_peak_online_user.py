import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_file
import config

current_date=''

def get_current_date(line):
    global current_date
    return current_date

def is_correct_date(line):
    time=helper_regex.extract(line,r'(\d+\-\d+\-\d+ \d+:\d+)')
    if not time:
        return False
    match=current_date in helper_regex.time_add(time+':00',config.timezone_offset_vodafone*1.0/24)
    return match

def stat(my_date): # run on 5:00 a.m. , calculate yesterday's data

    global current_date
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    oem_name='Vodafone'
    stat_category='monet'

    # daily stat

    """
    date,online
    2011-05-02 00:01,13694.0
    2011-05-02 00:02,13730.0
    2011-05-02 00:03,13706.0
    """
    
    stat_plan=Stat_plan()

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                    select_max={'peak_concurrent_user':r',(\d+)'},
                                    select_average={'peak_concurrent_user':r',(\d+)'},
                                    where={'filtered_date':is_correct_date}, \
                                    group_by={'daily':get_current_date}))

    stat_plan.add_raw_content_source(helper_file.get_http_content_with_cookie( \
            r'''http://angel.morange.com/rrd/img/149/download?start=%s&end=%s'''
                % (helper_regex.date_add(current_date,-1),helper_regex.date_add(current_date,2)), \
            cookies=[
                {'name':'L', 'value':'en', 'domain':'morange.com'},
                {'name':'csrftoken', 'value':'5f842bf4da706e5edb71937b65ec3bf3', 'domain':'angel.morange.com'},
                {'name':'sessionid', 'value':'b7d24a66b7ab78f81c6cc942081bc733', 'domain':'angel.morange.com'},
            ],referer='http://angel.morange.com/rrd/149/widget/192/graph'))
    
    stat_plan.run()

    #daily online curve
    
    stat_plan=Stat_plan()



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()+3600*config.timezone_offset_vodafone-3600*24*i
        stat(my_date)

