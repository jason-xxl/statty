import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config



def stat_chatroom_king_invite(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Umniah'
    stat_category='invite'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')
    
    start_time_invite=helper_regex.time_floor(my_date-60*24*3600)
    end_time_invite=helper_regex.time_ceil(my_date)

    # invite sub total
    
    start_time=helper_regex.time_floor(my_date)

    db='invitation_umniah'
    key='invite_chatroom_king_sub_unique'
    sql=r'''
    select count(distinct unsub.[msisdn])
    from
    (
    SELECT [msisdn]
    from [umniah_mt].[dbo].[logs] with(nolock)
    where [CreatedOn] between '%s' and '%s'
    group by [msisdn]
    having max([id]*100+[action])-max([id]*100)=10
    ) unsub
    where unsub.[msisdn] in 
    (
    select distinct msisdn collate database_default 
    from [umniah_mt].dbo.award_record with(nolock) where type ='chatroom'
    --and [CreatedOn] between '%s' and '%s'
    )
    ''' % (start_time,end_time,start_time_invite,end_time_invite)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    

    # invite unsub total
    
    start_time=helper_regex.time_floor(my_date)

    db='invitation_umniah'
    key='invite_chatroom_king_unsub_unique'
    sql=r'''
    select count(distinct unsub.[msisdn])
    from
    (
    SELECT [msisdn]
    from [umniah_mt].[dbo].[logs] with(nolock)
    where [CreatedOn] between '%s' and '%s'
    group by [msisdn]
    having max([id]*100+[action])-max([id]*100)=11
    ) unsub
    where unsub.[msisdn] in 
    (
    select distinct msisdn collate database_default 
    from [umniah_mt].dbo.award_record with(nolock) where type ='chatroom' 
    --and [CreatedOn] between '%s' and '%s'
    )
    ''' % (start_time,end_time,start_time_invite,end_time_invite)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)








    """

    # invite sub unique
    
    start_time=helper_regex.time_floor(my_date)

    db='umniah_invitation_status'
    key='invite_chatroom_king_sub_unique'
    sql=r'''
    SELECT 
    sum([IsValid]) as sub_chatroom_invite

    FROM [umniah_invitation_status].[dbo].[invitation_statistics] with(nolock)
    where [LastUpdateTime]>='%s' and [LastUpdateTime]<'%s'
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    

    # invite unsub unique
    
    start_time=helper_regex.time_floor(my_date)

    db='umniah_invitation_status'
    key='invite_chatroom_king_unsub_unique'
    sql=r'''
    SELECT 
    sum(1-[IsValid]) as unsub_chatroom_invite

    FROM [umniah_invitation_status].[dbo].[invitation_statistics] with(nolock)
    where [LastUpdateTime]>='%s' and [LastUpdateTime]<'%s'
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    """


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_chatroom_king_invite(my_date)
