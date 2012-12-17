import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_invite(my_date): # run on 0:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='invite_only_globe'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    # sender of invitation
    
    db=''
    key='invite_sender_unique'
    sql=r'''
    select count(distinct user_id) from [globe_ph_mt].dbo.sms_invitation_record with(nolock)
    where createdOn between '%s' and '%s'
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # receipient
    
    db=''
    key='invite_receipient_unique'
    sql=r'''
    select count(distinct to_number) from [globe_ph_mt].dbo.sms_invitation_record with(nolock)
    where createdOn between '%s' and '%s'
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # sent invitation
    
    db=''
    key='invite_sent_invitation'
    sql=r'''
    select count(*) as total from [globe_ph_mt].dbo.sms_invitation_record with(nolock)
    where createdOn between '%s' and '%s'
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    




    # successful invitee
    
    db=''
    key='invite_successful_invitation'
    sql=r'''
    SELECT count(distinct [msisdn])
    FROM [globe_ph_mt].[dbo].[award_record]
    where [CreatedOn]>='%s' and [CreatedOn]<'%s'
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_invite(my_date)
