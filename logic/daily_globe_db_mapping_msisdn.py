import helper_sql_server
import helper_mysql
import helper_math
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex, re
import config


def stat_sub(my_date): # run on 0:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='mapping_only_globe'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')
        
    key='uid_to_msisdn'
    
    # get max monet id exising

    sql=r'''
    
    SELECT IFNULL(MAX(sub_key),0) 
    FROM raw_data_id_msisdn 
    WHERE oem_name='%s' 
    AND category='%s' 
    AND `key`='%s'
    
    ''' % (oem_name, stat_category, key)

    print 'MySQL:'+sql
    value=helper_mysql.get_one_value_int(sql)
    print value


    # Globe MSISDN: 639XXXXXXXXX
    # incrementally grab msisdn from profile table

    sql=r''' 

    select user_id,user_name 
    from mozone_user.dbo.profile with(nolock) 
    where version_tag='fast_globe' 
    AND user_id>%s

    ''' % (value,)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_mozat,sql)

    for uid,uname in values.iteritems():
        msisdn=helper_regex.extract(uname,r'(\d+)@fast_globe')
        if msisdn is not None:
            helper_mysql.put_raw_data(oem_name,stat_category,key,uid,msisdn,'raw_data_id_msisdn',date_today)


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
