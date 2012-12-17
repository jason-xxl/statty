import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_login(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='STC'
    stat_category='login'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    
    """
    # login 1 day [non-stc]
    
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')

    db='stc_integral'
    key='non_stc_user_last_login_1_day_unique'
    
    sql=r'''

    select count(*) 
    from mozone_user.dbo.profile with(nolock) 
    where [lastLogin]>=DATEADD(day,-1,Getdate())
    and (version_tag='shabik' or version_tag is null)
    and phone not like '+966%%'
    and user_name not like 'circle%%'

    '''
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_stc,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    """

    # login 1 day

   
    db=''
    key='user_last_login_1_day_unique'

    sql=r'''

    select count(distinct monetid)
    from User.loginoff
    where date>='%s'
    and date<'%s'
    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    value=helper_mysql.get_one_value_int(sql,config.conn_stc_login_mysql)
    
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name="raw_data_shabik_360")


    """

    # login 7 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 05:00:00')

    db='stc_integral'
    key='user_last_login_7_day_unique'

    sql=r'''

    select count(distinct monetid)
    from User.loginoff
    where date>=DATE_ADD(NOW(), interval -7 day) 

    '''

    print 'SQL Server:'+sql
    value=helper_mysql.get_one_value_int(sql,config.conn_stc_login_mysql)
    
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # login 30 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*29).replace(' 00:00:00',' 05:00:00')

    db='stc_integral'
    key='user_last_login_30_day_unique'
    
    sql=r'''

    select count(distinct monetid)
    from User.loginoff
    where date>=DATE_ADD(NOW(), interval -30 day) 

    '''

    print 'SQL Server:'+sql
    value=helper_mysql.get_one_value_int(sql,config.conn_stc_login_mysql)
    
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    """






if __name__=='__main__':

    for i in range(config.day_to_update_stat+3,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_login(my_date)
