import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_viva_bh
import config


def stat_sub(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Viva_BH'
    stat_category='sub'
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    

    # sub action total
    
    key='sub_action_total'
    
    db='bahrain_mt'
    sql="select count(*) from [bahrain_mt].dbo.logs with(nolock) where action=10 and createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    # unsub action total
    
    key='unsub_action_total'
    
    db='bahrain_mt'
    sql="select count(*) from [bahrain_mt].dbo.logs with(nolock) where action=11 and createdon between '%s' and '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    

    # sub users total
    
    key='sub_user_total'
    
    db='bahrain_mt'
    sql="select count(*) from [bahrain_mt].dbo.accounts with(nolock) where is_deleted=0"
    '''
    sql="""
    select count(distinct [msisdn])
    from
    (
    SELECT [msisdn]
    from [bahrain_mt].[dbo].[logs] with(nolock)
    where [CreatedOn]<'%s'
    and [msisdn] not in (
        SELECT distinct [msisdn] 
          from [bahrain_mt].[dbo].[system_action_logs] with(nolock)
        where action=3
        and [CreatedOn]<'%s'
    )    
    
    group by [msisdn]
    having max([id]*100+[action])-max([id])*100  = 10
    ) a
    """ % (end_time,end_time)
    '''    
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # unique sub user
    
    
    db='bahrain_mt'
    sql="""
    
    select sum(sub) as sum_sub,sum(isNewSub) as new_sub,sum(isReqeatedSub) as re_sub
    from(
        select 
        [msisdn],
        case when min([CreatedOn])<'%s' then 1 else 0 end as isReqeatedSub,
        case when min([CreatedOn])>='%s' then 1 else 0 end as isNewSub,
        1 as sub
        from [bahrain_mt].[dbo].[logs] with(nolock)
        where [msisdn] in(
            SELECT [msisdn]
              from [bahrain_mt].[dbo].[logs] with(nolock)
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
    
    
    db='bahrain_mt'
    sql="""
    
    select sum(unsub) as sum_unsub,sum(subIn1) as unsub1,sum(sub7to1) as unsub7to1,sum(sub14to7) as unsub14to7,
    sum(sub30to14) as unsub30to14,sum(subBefore30) as unsubBefore30
    from(
        select 
        [msisdn],
        case when min([CreatedOn])>dateadd(dd,-1,getdate()) then 1 else 0 end as subIn1,
        case when min([CreatedOn])>dateadd(dd,-7,getdate()) and min([CreatedOn])<=dateadd(dd,-1,getdate()) then 1 else 0 end as sub7to1,
        case when min([CreatedOn])>dateadd(dd,-14,getdate()) and min([CreatedOn])<=dateadd(dd,-7,getdate()) then 1 else 0 end as sub14to7,
        case when min([CreatedOn])>dateadd(dd,-30,getdate()) and min([CreatedOn])<=dateadd(dd,-14,getdate()) then 1 else 0 end as sub30to14,
        case when min([CreatedOn])<dateadd(dd,-30,getdate()) then 1 else 0 end as subBefore30,
        1 as unsub
        from [bahrain_mt].[dbo].[logs] with(nolock)
        where [msisdn] in(
            SELECT [msisdn]
              from [bahrain_mt].[dbo].[logs] with(nolock)
            where [CreatedOn]>='%s' and [CreatedOn]<'%s'
            group by [msisdn]
            having max([id]*100+[action])-max([id])*100  = 11
        ) and [CreatedOn]<'%s'
        group by [msisdn]
    ) a

    """ % (start_time,end_time,end_time)
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
        
    """


    # sub duration distribution of all sub user
    # 10240 level means more than 5120
    
    db=''
    key='sub_user_sub_duration_distribution'
    sql=r'''
    

    select num_of_week
    ,count(*) as num_of_user

    from(
        SELECT `user_id`

        ,pow(2,
        case 
        when log2(`subscribe-max_sub_duration`/3600/24/7)<0 then 0
        when log2(`subscribe-max_sub_duration`/3600/24/7)>10 then 11
        else ceil(log2(`subscribe-max_sub_duration`/3600/24/7))
        end) as num_of_week

        FROM `mozat_clustering`.`user_figure_base` 

        where `oem_id`=3 and `subscribe-in_subscription`=1
        and `subscribe-max_sub_duration` is not null
    ) a

    group by num_of_week
    order by num_of_week asc
    
    
    '''
        
    print 'SQL Server:'+sql
    values=helper_mysql.fetch_dict(sql)
    print values

    s=0
    for k,v in values.iteritems():    
        helper_mysql.put_raw_data(oem_name,stat_category,key,date_today+'_'+k,v)
        s=s+int(v)

    key='sub_user_sub_duration_distribution_base'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,s)


    # sub duration distribution of all unsub user
    # 10240 level means more than 5120
    
    db=''
    key='unsub_user_sub_duration_distribution'
    sql=r'''
    

    select num_of_week
    ,count(*) as num_of_user

    from(
        SELECT `user_id`

        ,pow(2,
        case 
        when log2(`subscribe-max_sub_duration`/3600/24/7)<0 then 0
        when log2(`subscribe-max_sub_duration`/3600/24/7)>10 then 11
        else ceil(log2(`subscribe-max_sub_duration`/3600/24/7))
        end) as num_of_week

        FROM `mozat_clustering`.`user_figure_base` 

        where `oem_id`=3 and `subscribe-in_subscription`=0
        and `subscribe-max_sub_duration` is not null
    ) a

    group by num_of_week
    order by num_of_week asc
    
    
    '''
        
    print 'SQL Server:'+sql
    values=helper_mysql.fetch_dict(sql)
    print values

    s=0
    for k,v in values.iteritems():    
        helper_mysql.put_raw_data(oem_name,stat_category,key,date_today+'_'+k,v)
        s=s+int(v)

    key='unsub_user_sub_duration_distribution_base'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,s)

    """

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
