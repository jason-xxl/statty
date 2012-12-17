import helper_sql_server
import helper_mysql
import helper_math
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_sub(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Vodafone'
    stat_category='sub'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    date_today=start_time.replace(' 06:00:00','')

    """
    Public Enum Actions
        None = 0
        Subscribe1 = 1
        Subscribe30 = 2
        DowngradeTo1 = 5
        UpgradeTo30 = 6     # no use any more
        Unsubscribe = 10    # changed to 6, by Panke
        SubscribeFree = 100
    End Enum
    
    Enum SubscriptionType 
        Free = 0 
        Day = 1 
        Month = 30 
    End Enum
    """

    sql_sub='1,2,100'
    sql_in_sub='1,2,5,100'
    sql_unsub='6'

    # sub action total
    
    key='sub_action_total'
    
    db='shabik_mt'
    sql="select count(*) from [shabik_mt].dbo.logs with(nolock) where action in (%s) and createdon between '%s' and '%s'" \
         % (sql_sub,start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # unsub action total
    
    key='unsub_action_total'
    
    db='shabik_mt'
    sql="select count(*) from [shabik_mt].dbo.logs with(nolock) where action in (%s) and createdon between '%s' and '%s'" \
         % (sql_unsub,start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    

        
    # sub users total
    
    key='sub_user_total'
    
    db='shabik_mt'
    #sql="select count(*) from [shabik_mt].dbo.accounts with(nolock) where is_deleted=0"
    
    sql=r'''
    
    select top 1 NumOfSubscribers
    from [shabik_mt].[dbo].[num_of_subscribers]
    where [OfDate] > DATEADD(day,1,'%s')
    order by [OfDate] asc
    
    ''' % (date_today,)
    """
    
    sql='''
    select count(distinct [msisdn])
    from
    (
    SELECT [msisdn]
    from [shabik_mt].[dbo].[logs] with(nolock)
    where [CreatedOn]<'%s'
    --and [msisdn] not in (
    --    SELECT distinct [msisdn] 
    --      from [shabik_mt].[dbo].[system_action_logs] with(nolock)
    --    where action=3
    --    and [CreatedOn]<'%s'
    --)    
    
    group by [msisdn]
    having max([id]*100+[action])-max([id])*100 in (%s)
    ) a
    ''' % (end_time,end_time,sql_in_sub)
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar(config.conn_vodafone_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # unique sub user
    
    
    db='shabik_mt'
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
            having max([id]*100+[action])-max([id])*100 in (%s)
        ) and [CreatedOn]<'%s'
        group by [msisdn]
    ) s

    """ % (start_time,start_time,start_time,end_time,sql_sub,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_vodafone_mt,sql)
    print value

        
    key='sub_user_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['sum_sub'])
        
    key='sub_user_new_sub_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['new_sub'])
        
    key='sub_user_re_sub_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['re_sub'])
        



    # unique unsub user
    
    
    db='shabik_mt'
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
            having max([id]*100+[action])-max([id])*100 in (%s)
        ) and [CreatedOn]<'%s'
        group by [msisdn]
    ) a

    """ % (date_today,date_today,date_today,date_today,date_today,date_today,date_today,date_today,start_time,end_time,sql_unsub,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_vodafone_mt,sql)
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

        where `oem_id`=4 and `subscribe-in_subscription`=1
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

        where `oem_id`=4 and `subscribe-in_subscription`=0
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



    sql=r'''
        select top 1 msisdn_collection
        from [shabik_mt].[dbo].[account_snapshot]
        where [date]='%s'
        and [type]='in_sub_subscriber'
        order by [created_on]
    ''' % (date_today,)

    total_in_sub_msisdn_element_str=helper_sql_server.fetch_scalar(config.conn_vodafone_mt,sql)

    helper_mysql.put_collection(collection=total_in_sub_msisdn_element_str, \
                                oem_name=oem_name,category=stat_category, \
                                key='in_sub_subscriber',sub_key='',date=date_today)
    
    base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size \
    =helper_math.calculate_date_range_retain_rate(1,oem_name,stat_category,'in_sub_subscriber','',date_today)

    helper_mysql.put_raw_data(oem_name,stat_category,'daily_new_subscriber',date_today,fresh_base_size)
    helper_mysql.put_raw_data(oem_name,stat_category,'daily_retained_subscriber',date_today,retained_base_size)
    helper_mysql.put_raw_data(oem_name,stat_category,'daily_unsubbed_subscriber',date_today,lost_base_size)

    """


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
