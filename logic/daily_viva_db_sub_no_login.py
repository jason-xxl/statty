import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_sub(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Viva'
    stat_category='sub'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    

    # first sub in last day
    
    key='sub_in_last_day'
    
    db='viva_mt'
    sql="""
    
    select count(*) from (
        SELECT msisdn
          FROM [viva_mt].[dbo].[logs] with(nolock)

        where [CreatedOn]<'%s' and action in (2)
        group by [msisdn]
        having min([CreatedOn])>='%s' and min([CreatedOn])<'%s'
    ) a
    
    """ % (end_time,start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    # first sub in last day but no login
    
    key='sub_in_last_day_no_login'
    
    db='viva_mt'
    sql="""

    select count(*) from [DB85].[mozone_user].[dbo].[Profile] b with(nolock)
    where user_name in (

        SELECT replace([msisdn],'+965','')+'@viva' COLLATE DATABASE_DEFAULT as [user_name]
          FROM [viva_mt].[dbo].[logs] with(nolock)
        where [CreatedOn]<'%s' and action in (2)
        group by [msisdn]
        having min([CreatedOn])>='%s' and min([CreatedOn])<'%s'

    ) and lastLogin is null
    
    """ % (end_time,start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    
    # FINALLY unsub in last day but no login
    
    key='finally_unsub_in_last_day_no_login'
    
    db='viva_mt'
    sql="""
    
    select count(*) from (
        SELECT '0'+replace([msisdn],'+966','')+'@shabik.com' as [user_name]
          from [DB86].[viva_mt].[dbo].[logs] with(nolock)
        group by [msisdn]
        having max([CreatedOn])>='%s' and max([CreatedOn])<'%s'
        and max(id*100+action)-max(id*100) in (2)
    ) a 
    left join [mozone_user].[dbo].[Profile] b with(nolock)
    on a.[user_name] = b.[user_name] collate database_default 
    where b.lastLogin is null

    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


if __name__=='__main__':

    for i in range(1+config.day_to_update_stat,1,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
