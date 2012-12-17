import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config



def stat_oa_invite(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Telk_Armor'
    stat_category='invite'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    start_time_invite=helper_regex.time_floor(my_date-60*24*3600)
    end_time_invite=helper_regex.time_ceil(my_date)

    # invite sub total
    
    start_time=helper_regex.time_floor(my_date)

    db='invitation_telkomsel'
    key='invite_oa_sub_unique'
    sql=r'''
    select count(distinct unsub.[msisdn])
    from
    (
    SELECT [msisdn]
    FROM [telkomsel_mt].[dbo].[logs] with(nolock)
    where [CreatedOn] between '%s' and '%s'
    group by [msisdn]
    having max([id]*100+[action])-max([id]*100) in (1,2,3,10,11,12)
    ) unsub
    where unsub.[msisdn] in 
    (
    select distinct msisdn collate database_default 
    from telkomsel_mt.dbo.award_record with(nolock) where type ='oa'
    --and [CreatedOn] between '%s' and '%s'
    )
    ''' % (start_time,end_time,start_time_invite,end_time_invite)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    

    # invite unsub total
    
    start_time=helper_regex.time_floor(my_date)

    db='invitation_telkomsel'
    key='invite_oa_unsub_unique'
    sql=r'''
    select count(distinct unsub.[msisdn])
    from
    (
    SELECT [msisdn]
    FROM [telkomsel_mt].[dbo].[logs] with(nolock)
    where [CreatedOn] between '%s' and '%s'
    group by [msisdn]
    having max([id]*100+[action])-max([id]*100) in (4,5,6,7,8,9)
    ) unsub
    where unsub.[msisdn] in 
    (
    select distinct msisdn collate database_default 
    from telkomsel_mt.dbo.award_record with(nolock) where type ='oa' 
    --and [CreatedOn] between '%s' and '%s'
    )
    ''' % (start_time,end_time,start_time_invite,end_time_invite)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat_oa_invite(my_date)
