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


def stat_sub(my_date): # run on 0:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='sub_only_globe'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 00:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 00:00:00')
    date_today=start_time.replace(' 00:00:00','')

    """
    Public Enum Actions
      None = 0,
      Subscribe1 = 1,
      Subscribe3 = 3,
      Subscribe5 = 5,
      Subscribe30 = 30,
      SubscribeFree = 100,
      SubFirstTime = 150,
      Unsubscribe = 200,
      Kickout = 250,
      SelectNextPackageSub1 = 301,
      SelectNextPackageSub3 = 303,
      SelectNextPackageSub5 = 305,
      SelectNextPackageSub30 = 330
    End Enum
    
    """

    sql_sub='1,3,5,30,100,150'
    sql_in_sub='1,3,5,30,100,150,301,303,305,330'
    sql_unsub='200,250'

    sql_in_sub_package_1='1,301'
    sql_in_sub_package_3='3,303'
    sql_in_sub_package_5='5,305'
    sql_in_sub_package_30='30,330'
    sql_in_sub_package_free='100,150'

    # sub action total
    
    key='sub_action_total'
    
    db='globe_ph_mt'
    sql=r'''
    select count(*) 
    from [globe_ph_mt].dbo.logs with(nolock) 
    where action in (%s) 
    and createdon >= '%s' and createdon < '%s'
    and msisdn not like '%%00000%%'
    ''' % (sql_sub,start_time,end_time)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # unsub action total
    
    key='unsub_action_total'
    
    db='globe_ph_mt'
    sql=r'''
    select count(*) 
    from [globe_ph_mt].dbo.logs with(nolock) 
    where action in (%s) 
    and createdon >= '%s' and createdon < '%s'
    and msisdn not like '%%00000%%'
    ''' % (sql_unsub,start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    

        
    # sub users total
    
    key='sub_user_total'
    
    db='globe_ph_mt'
    """
    sql="select count(*) from [globe_ph_mt].dbo.accounts with(nolock) where is_deleted=0"


    sql=r'''

    select cast(max(NumOfSubscribers) as int) NumOfSubscribers from (
    select top 1 NumOfSubscribers
    from [globe_ph_mt].[dbo].[num_of_subscribers]
    where [OfDate] > DATEADD(day,1,'%s')
    order by [OfDate] asc
    union select 0 as NumOfSubscribers) a
    
    ''' % (date_today,)

    """

    sql=r'''
    
    select count(*) as sub_user_total
    ,sum(pack_1) as sub_user_package_1_total
    ,sum(pack_3) as sub_user_package_3_total
    ,sum(pack_5) as sub_user_package_5_total
    ,sum(pack_30) as sub_user_package_30_total
    ,sum(pack_free) as sub_user_package_free_total

    from (
        select msisdn 
        ,case when max(id*1000+[action])-max(id*1000) in (%s) then 1 else 0 end as pack_1
        ,case when max(id*1000+[action])-max(id*1000) in (%s) then 1 else 0 end as pack_3
        ,case when max(id*1000+[action])-max(id*1000) in (%s) then 1 else 0 end as pack_5
        ,case when max(id*1000+[action])-max(id*1000) in (%s) then 1 else 0 end as pack_30
        ,case when max(id*1000+[action])-max(id*1000) in (%s) then 1 else 0 end as pack_free
        from [globe_ph_mt].dbo.logs with(nolock) 
        where createdon < '%s'
        group by msisdn
        having max(id*1000+[action])-max(id*1000) in (%s)
        and msisdn not like '%%00000%%'
    ) a

    ''' % (sql_in_sub_package_1,sql_in_sub_package_3,sql_in_sub_package_5,sql_in_sub_package_30, \
           sql_in_sub_package_free,end_time,sql_in_sub)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_mt,sql)
    print values
    helper_mysql.put_raw_data(oem_name,stat_category,'sub_user_total',date_today,values['sub_user_total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'sub_user_package_1_total',date_today,values['sub_user_package_1_total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'sub_user_package_3_total',date_today,values['sub_user_package_3_total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'sub_user_package_5_total',date_today,values['sub_user_package_5_total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'sub_user_package_30_total',date_today,values['sub_user_package_30_total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'sub_user_package_free_total',date_today,values['sub_user_package_free_total'])
    

    # unique sub user
    
    db='globe_ph_mt'
    sql="""
    
    select sum(sub) as sum_sub,sum(isNewSub) as new_sub,sum(isReqeatedSub) as re_sub
    from(
        select 
        [msisdn],
        case when min([CreatedOn])<'%s' then 1 else 0 end as isReqeatedSub,
        case when min([CreatedOn])>='%s' then 1 else 0 end as isNewSub,
        1 as sub
        from [globe_ph_mt].[dbo].[logs] with(nolock)
        where [msisdn] in(
            SELECT [msisdn]
              from [globe_ph_mt].[dbo].[logs] with(nolock)
            where [CreatedOn]>='%s' and [CreatedOn]<'%s'
            group by [msisdn]
            having max([id]*1000+[action])-max([id])*1000 in (%s)
            and msisdn not like '%%00000%%'
        ) and [CreatedOn]<'%s'
        group by [msisdn]
    ) s

    """ % (start_time,start_time,start_time,end_time,sql_sub,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_mt,sql)
    print value

        
    key='sub_user_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['sum_sub'])
        
    key='sub_user_new_sub_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['new_sub'])
        
    key='sub_user_re_sub_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['re_sub'])
        

    

    # unique sub action user
    
    db='globe_ph_mt'
    sql="""
    
    SELECT count(distinct [msisdn]) as [sub_action_user]
    from [globe_ph_mt].[dbo].[logs] with(nolock)
    where [CreatedOn]>='%s' and [CreatedOn]<'%s'
    and [action] in (%s)
    and msisdn not like '%%00000%%'

    """ % (start_time,end_time,sql_sub)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_mt,sql)
    print value

        
    key='sub_action_user_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value['sub_action_user'])
        

    # unique unsub user
    
    
    db='globe_ph_mt'
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
        from [globe_ph_mt].[dbo].[logs] with(nolock)
        where [msisdn] in(
            SELECT [msisdn]
              from [globe_ph_mt].[dbo].[logs] with(nolock)
            where [CreatedOn]>='%s' and [CreatedOn]<'%s'
            group by [msisdn]
            having max([id]*1000+[action])-max([id])*1000 in (%s)
            and msisdn not like '%%00000%%'
        ) and [CreatedOn]<'%s'
        group by [msisdn]
    ) a

    """ % (date_today,date_today,date_today,date_today,date_today,date_today,date_today,date_today,start_time,end_time,sql_unsub,end_time)
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
    sql=r'''
        select top 1 msisdn_collection
        from [globe_ph_mt].[dbo].[account_snapshot]
        where [date]='%s'
        and [type]='in_sub_subscriber'
        order by [created_on]
    ''' % (date_today,)

    total_in_sub_msisdn_element_str=helper_sql_server.fetch_scalar(config.conn_mt,sql)

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
