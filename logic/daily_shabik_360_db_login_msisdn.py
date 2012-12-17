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
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')
    
    # login 1 day
    
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')

    db='stc_integral'
    key='user_last_login_1_day_msisdn_unique'
    sql=r'''
    
    select count(distinct msisdn) 
    from (
        (
            select a.msisdn as msisdn 
            from stc_integral.dbo.accounts as a with(nolock)
            inner join db85.mozone_user.dbo.profile as b with(nolock)
            on '0'+substring(a.msisdn,5,9)+'@shabik.com'=b.user_name 
            where b.[lastLogin]>=DATEADD(day,-1,Getdate())  
            and b.user_name like '%@shabik.com'
        ) union (
            select c.msisdn collate Chinese_PRC_CI_AS as msisdn 
            from stc_integral.dbo.msisdn_to_sdp_id as c with(nolock)
            inner join (
                select  sp_user_id 
                from db85.sbk_users.dbo.sdp_user_id as a with(nolock) inner 
                join db85.mozone_user.dbo.profile as b with(nolock)
                on a.user_id=b.user_id 
                where b.[lastLogin]>=DATEADD(day,-1,Getdate())  
                and b.user_name like '%@shabik.com'
            ) as d 
            on c.sp_user_id=d.sp_user_id
        )
    ) r
    
    '''

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # login 7 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 05:00:00')

    db='stc_integral'
    key='user_last_login_7_day_msisdn_unique'
    sql=r'''
    
    select count(distinct msisdn) 
    from (
        (
            select a.msisdn as msisdn 
            from stc_integral.dbo.accounts as a with(nolock)
            inner join db85.mozone_user.dbo.profile as b with(nolock)
            on '0'+substring(a.msisdn,5,9)+'@shabik.com'=b.user_name 
            where b.[lastLogin]>=DATEADD(day,-7,Getdate())  
            and b.user_name like '%@shabik.com'
        ) union (
            select c.msisdn collate Chinese_PRC_CI_AS as msisdn 
            from shabik_mt.dbo.msisdn_to_sdp_id as c with(nolock)
            inner join (
                select  sp_user_id 
                from db85.sbk_users.dbo.sdp_user_id as a with(nolock) inner 
                join db85.mozone_user.dbo.profile as b with(nolock)
                on a.user_id=b.user_id 
                where b.[lastLogin]>=DATEADD(day,-7,Getdate())  
                and b.user_name like '%@shabik.com'
            ) as d 
            on c.sp_user_id=d.sp_user_id
        )
    ) r
    
    '''

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # login 30 day
    
    start_time=helper_regex.time_floor(my_date-3600*24*29).replace(' 00:00:00',' 05:00:00')

    db='stc_integral'
    key='user_last_login_30_day_msisdn_unique'
    sql=r'''
    
    select count(distinct msisdn) 
    from (
        (
            select a.msisdn as msisdn 
            from stc_integral.dbo.accounts as a with(nolock)
            inner join db85.mozone_user.dbo.profile as b with(nolock)
            on '0'+substring(a.msisdn,5,9)+'@shabik.com'=b.user_name 
            where b.[lastLogin]>=DATEADD(day,-30,Getdate())  
            and b.user_name like '%@shabik.com'
        ) union (
            select c.msisdn collate Chinese_PRC_CI_AS as msisdn 
            from shabik_mt.dbo.msisdn_to_sdp_id as c with(nolock)
            inner join (
                select  sp_user_id 
                from db85.sbk_users.dbo.sdp_user_id as a with(nolock)inner 
                join db85.mozone_user.dbo.profile as b with(nolock)
                on a.user_id=b.user_id 
                where b.[lastLogin]>=DATEADD(day,-30,Getdate())  
                and b.user_name like '%@shabik.com'
            ) as d 
            on c.sp_user_id=d.sp_user_id
        )
    ) r
    
    '''

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_login(my_date)



    """
    sql=r'''
    
    declare @old_user_unique_login int
    declare @new_user_unique_login int

    select @old_user_unique_login=count(*) from mozone_user.dbo.profile with(nolock) where [lastLogin]>=DATEADD(day,-7,Getdate())  and (version_tag='shabik' or version_tag is null) and user_name like '05%%@shabik.com'
    select @new_user_unique_login=count(distinct sp_user_id) from sbk_users.dbo.sdp_user_id with(nolock) where user_id
    in
    (select user_id from mozone_user.dbo.profile with(nolock) where [lastLogin]>=DATEADD(day,-7,Getdate())  and (version_tag='shabik' or version_tag is null))

    select @old_user_unique_login+@new_user_unique_login
    
    ''' 
    """
