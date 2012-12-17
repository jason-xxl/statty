import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config



def stat_invite(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Vodafone'
    stat_category='invite'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    
    sql_sub='1,2,100'
    sql_in_sub='1,2,5,6,100'
    sql_unsub='10'
    
    # invite total
    
    start_time=helper_regex.time_floor(my_date)

    db='invitation'
    sql=r'''

    select count(*) as total
    ,count(distinct msisdn) as [from_user]
    ,count(distinct to_number) as [to_user] 
    
    from [shabik_mt].[dbo].[sms_invitation_record] with(nolock) 
    
    where [createdOn] >= '%s' and [createdOn]<'%s'

    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_vodafone,sql)
    print values

    key='invite_action_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['total'])
    key='invite_from_user'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['from_user'])
    key='invite_to_user'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['to_user'])
    

    # invite succeeded #expired
    """
    start_time=helper_regex.time_floor(my_date)

    db='award_record'
    key='invite_action_succeded_total'
    sql=r'''
    select count(*) from [shabik_mt].dbo.award_record with(nolock) where createdON between '%s' and '%s'
    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    """



    #special time range to keep consistant to the sub statistics
    
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')

    # invited fresh sub

    db='invitation'
    key='invited_fresh_sub_unique'
    sql=r'''

    select count(distinct [msisdn])
    from
    (
        SELECT [msisdn]
        from [shabik_mt].[dbo].[logs] with(nolock)
        where [CreatedOn] < '%s'
        group by [msisdn]
        having min([CreatedOn]) >= '%s'
        and [msisdn] in (
        
            SELECT distinct [to_number] collate database_default 
            from [shabik_mt].[dbo].[sms_invitation_record] with(nolock)
        )
    ) a

    ''' % (end_time,start_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_invite(my_date)
