import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_user_generated_content(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='user_generated_content_mozat_6'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')


    # daily new user splitted by client type
    # register 1 day (mozat 6)
    
    stat_category='login_mozat_6'

    start_time=helper_regex.time_floor(my_date)

    db=''

    sql=r'''
    
    select 

    b.`value`
    count(a.user_id) as new_user
    --,count(case when lastLogin is not null then lastLogin else null end) as logined_user

    from [DB81].[mozone_user].[dbo].[Profile] with(nolock) a

    left join MYSQL158...raw_data_user_device b
    on a.user_id=cast(b.sub_key as UNSIGNED INTEGER)

    where 

    a.[creationDate]>='%s'
    and a.[creationDate]<'%s'
    and a.[user_name] like '%%@morange.com'
    and dbo.find_regular_expression(a.[user_name],N'^M\d+@morange.com',0)=1

    and b.`oem_name`="Mozat"
    and b.`category`="moagent"
    and b.`key`="app_page_by_user_id_client_type_first_int_value"

    group by b.`value`
    order by b.`value`

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    return

    values=helper_sql_server.fetch_row(config.conn_helper_db,sql)

    print values

    key='user_register_1_day_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['new_user'],table_name='raw_data_country')

    key='user_register_logined_1_day_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['logined_user'],table_name='raw_data_country')




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_user_generated_content(my_date)
