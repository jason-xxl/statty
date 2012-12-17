import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_viva_bh_sub_no_login
import config


def stat_sub(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Viva_BH'
    stat_category='sub'
    table_name='raw_data_user_info_periodical'

            
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    
    
    # sub-no-login phone numbers
    
    key='sub_no_login_1_day_phone_number'

    for number,i in user_id_filter_viva_bh_sub_no_login.user_list.iteritems():
        helper_mysql.put_raw_data(oem_name=oem_name,category=stat_category,key=key, \
                                  sub_key=number,value=1,table_name=table_name, \
                                  date=date_today)


if __name__=='__main__':

    for i in range(1+config.day_to_update_stat,1,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
