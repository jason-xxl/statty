import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config



def stat(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Vodafone'
    stat_category='sms_broadcast_callback'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    batch_number='1'
    time_begin='2011-11-04 22:00:07.030'
    time_end='2011-11-05 15:11:40.880'



    db='shabik_mt'
    sql=r'''

    select msisdn,null
    from [shabik_mt].[dbo].[buzz_broadcast_round_one_log]
    
    '''

    key='sms_sent_total'
    value=helper_sql_server.fetch_dict_into_collection(conn_config=config.conn_vodafone_87,sql=sql,key_name=0, \
                                oem_name=oem_name,category=stat_category,key='sms_sent_total',sub_key=batch_number, \
                                table_name='raw_data',date='',db_conn=None)
    
    print value


    
    db='shabik_mt'
    sql=r'''

    select count(user_id)
    from DB88.mozone_user.dbo.profile with(nolock)
    where user_name in (

        select replace(msisdn,'+20','')+'@voda_egypt'
        from [shabik_mt].[dbo].[buzz_broadcast_round_one_log]

    ) and lastLogin>='%s'
    ''' % (time_begin,)

    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_87,sql)
    print value

    key='user_logined_after_sms_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,batch_number,value)



    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat(my_date)
