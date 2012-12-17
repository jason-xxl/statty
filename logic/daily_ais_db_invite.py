import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
from datetime import date
import time
import helper_regex
import config
import weekly_shabik_360_db_billing


def stat_mt(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='invite_only_ais'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 00:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 00:00:00')
    date_today=start_time.replace(' 00:00:00','')
    
    # invitation
    
    db='sms_invitation_record'
    sql=r'''

    SELECT 
    count([id]) as daily_invite_sent_total
    ,count(distinct [user_id]) as daily_invite_sender_unique
    ,count(distinct [to_number]) as daily_invite_receipient_unique 

    FROM [ais_th_mt].[dbo].[sms_invitation_record] with(nolock)

    where createdOn>='%s' and createdOn<'%s'
    and type='normal'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_mt,sql)
    print values

    key='daily_invite_sent_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_ais')
    key='daily_invite_sender_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_ais')
    key='daily_invite_receipient_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_ais')


    # accepted invitation

    db='sms_invitation_record'
    sql=r'''

    SELECT 

    count(distinct a.[to_number]) as [daily_invte_accepted_unique]

    FROM [ais_th_mt].[dbo].[sms_invitation_record] a with(nolock)
    right join [ais_th_mt].[dbo].accounts b with(nolock)
    on a.to_number=b.msisdn

    where a.createdOn>='%s' and a.createdOn<'%s'
    and type='normal'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_mt,sql)
    print values

    key='daily_invte_accepted_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_ais')
      

    

if __name__=='__main__':

    for i in range(config.day_to_update_stat+4,0,-1):
        
        my_date=time.time()-3600*24*i
        stat_mt(my_date)
        