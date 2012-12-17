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
    match=current_date in helper_regex.time_add(time+':00',config.timezone_offset*1.0/24)
    if match:
        print current_date,helper_regex.time_add(time+':00',config.timezone_offset*1.0/24),':',line
    return match

def stat(my_date, target_operator, mo_angel_key, timezone_offset): # run on 5:00 a.m. , calculate yesterday's data

    global current_date
    config.timezone_offset = timezone_offset
    my_date=my_date+3600*config.timezone_offset
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    oem_name=target_operator
    stat_category='monet'
    if target_operator == 'Globe':
    	oem_name = 'Mozat'
    	stat_category = 'monet_only_globe'
    if target_operator == 'AIS':
    	oem_name = 'Mozat'
    	stat_category = 'monet_only_ais'

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
            r'''http://angel.morange.com/rrd/img/%s/download?start=%s&end=%s'''
                % (mo_angel_key,helper_regex.date_add(current_date,-1),helper_regex.date_add(current_date,2)), \
            cookies=[
                {'name':'L', 'value':'en', 'domain':'morange.com'},
                {'name':'csrftoken', 'value':'5f842bf4da706e5edb71937b65ec3bf3', 'domain':'angel.morange.com'},
                {'name':'sessionid', 'value':'b7d24a66b7ab78f81c6cc942081bc733', 'domain':'angel.morange.com'},
            ],referer='http://angel.morange.com/rrd/149/widget/192/graph'))
    
    stat_plan.run()   
    #daily online curve
    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat(my_date, 'AIS', '909', config.timezone_offset_ais)
        stat(my_date, 'STC', '2', config.timezone_offset_stc)
        stat(my_date, 'Viva', '3', config.timezone_offset_viva)
        stat(my_date, 'Viva_BH', '4', config.timezone_offset_viva_bh)
        stat(my_date, 'Umniah', '5', config.timezone_offset_umniah)
        stat(my_date, 'Telk_Armor', '7', config.timezone_offset_telk_armor)
        stat(my_date, 'Mozat', '23', config.timezone_offset_mozat)
        stat(my_date, 'Vodafone', '149', config.timezone_offset_vodafone)
        stat(my_date, 'Globe', '483',  config.timezone_offset_globe)
        #stat(my_date, 'Zoota', '1001 ', -7) 
