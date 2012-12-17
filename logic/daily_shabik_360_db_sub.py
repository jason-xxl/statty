import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_sub(my_date, sub_user_total_use_real_time=False): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='STC'
    stat_category='sub'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    

    # sub action total
    
    key='sub_action_total'
    
    db='stc_integral'
    sql="select count(*) from [stc_integral].dbo.logs with(nolock) where action=10 and createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # unsub action total
    
    key='unsub_action_total'
    
    db='stc_integral'
    sql="select count(*) from [stc_integral].dbo.logs with(nolock) where action=11 and createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    


    # sub users total
    
    key='sub_user_total'
    
    db='stc_integral'

    if sub_user_total_use_real_time:
        sql="select count(*) from [stc_integral].dbo.accounts with(nolock) where is_deleted=0" 
    else:
        sql=r'''
        
        select top 1 NumOfSubscribers
        from [shabik_mt].[dbo].[num_of_subscribers]
        where [OfDate] > DATEADD(day,1,'%s')
        order by [OfDate] asc
        
        ''' % (date_today,)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_stc_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    

    
    # unique sub user
    
    db='stc_integral'
    sql="""
    
    select sum(sub) as sum_sub,sum(isNewSub) as new_sub,sum(isReqeatedSub) as re_sub
    from(
        select 
        [msisdn],
        case when min([CreatedOn])<'%s' then 1 else 0 end as isReqeatedSub,
        case when min([CreatedOn])>='%s' then 1 else 0 end as isNewSub,
        1 as sub
        from [shabik_mt].[dbo].[logs] with(nolock)
        where [msisdn] in(
            SELECT [msisdn]
              from [shabik_mt].[dbo].[logs] with(nolock)
            where [CreatedOn]>='%s' and [CreatedOn]<'%s'
            group by [msisdn]
            having max([id]*100+[action])-max([id])*100  = 10
        ) and [CreatedOn]<'%s'
        group by [msisdn]
    ) s

    """ % (start_time,start_time,start_time,end_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_mt,sql)
    print value

        
    key='sub_user_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['sum_sub'])
        
    key='sub_user_new_sub_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['new_sub'])
        
    key='sub_user_re_sub_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['re_sub'])
        


    # unique unsub user
    
    db='stc_integral'
    sql="""
    
    select sum(unsub) as sum_unsub,sum(subIn1) as unsub1,sum(sub7to1) as unsub7to1,sum(sub14to7) as unsub14to7,
    sum(sub30to14) as unsub30to14,sum(subBefore30) as unsubBefore30
    from(
        select 
        [msisdn],
        case when min([CreatedOn])>dateadd(dd,-1,'%s') then 1 else 0 end as subIn1,
        case when min([CreatedOn])>dateadd(dd,-7,'%s') and min([CreatedOn])<=dateadd(dd,-1,'%s') then 1 else 0 end as sub7to1,
        case when min([CreatedOn])>dateadd(dd,-14,'%s') and min([CreatedOn])<=dateadd(dd,-7,'%s') then 1 else 0 end as sub14to7,
        case when min([CreatedOn])>dateadd(dd,-30,'%s') and min([CreatedOn])<=dateadd(dd,-14,'%s') then 1 else 0 end as sub30to14,
        case when min([CreatedOn])<dateadd(dd,-30,'%s') then 1 else 0 end as subBefore30,
        1 as unsub
        from [shabik_mt].[dbo].[logs] with(nolock)
        where [msisdn] in(
            SELECT [msisdn]
              from [shabik_mt].[dbo].[logs] with(nolock)
            where [CreatedOn]>='%s' and [CreatedOn]<'%s'
            group by [msisdn]
            having max([id]*100+[action])-max([id])*100  = 11
        ) and [CreatedOn]<'%s'
        group by [msisdn]
    ) a

    """ % (date_today,date_today,date_today,date_today,date_today,date_today,date_today,date_today,start_time,end_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_mt,sql)
    print value

        
    key='unsub_user_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['sum_unsub'])
        
    key='unsub_user_unique_first_sub_1_day'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['unsub1'])
        
    key='unsub_user_unique_first_sub_7_2_day'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['unsub7to1'])
        
    key='unsub_user_unique_first_sub_14_8_day'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['unsub14to7'])
        
    key='unsub_user_unique_first_sub_30_15_day'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['unsub30to14'])
        
    key='unsub_user_unique_first_sub_more_30_day'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['unsubBefore30'])
        


   


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
