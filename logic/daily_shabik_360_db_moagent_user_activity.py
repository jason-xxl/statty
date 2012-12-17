import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_math
import config
from datetime import date, datetime, timedelta


config.collection_cache_enabled=True




def stat_login(my_date):
    
    oem_name='Shabik_360'
    stat_category='login_retain'
    db_name='raw_data_shabik_360'
    
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)

    active_user_dict=[]
    temp=[]
    total=[]

    for i in range(0,5):
        
        active_user_dict.append(helper_mysql.get_dict_of_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                        key='app_page_daily_visitor_each_unique',sub_key='', \
                                        date=helper_regex.date_add(current_date,-i),table_name='raw_data_shabik_360',db_conn=None))

        temp.append(helper_math.get_simple_dispersion(active_user_dict[i],10)[0])
        total.append(sum(temp[i].values()))

    for j in range(0,22):
        print str(i*10)+'\t',
        for i in range(0,5):
            print float(temp[i].get(j*10,0))/total[j]*100
        print 

        
 






if __name__=='__main__':


    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat_login(my_date)

