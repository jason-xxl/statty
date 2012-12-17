import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_viva
import config



def stat_friend_relation(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Viva'
    stat_category='friend_relation'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    # newly created following relaltion
    
    db='mozone_friend'
    key='newly_created_friend_unidirectional_relation_unique_base'
    sql=r'''
    
    SELECT count([id])
    FROM [mozone_friend].[dbo].[friendship] with(nolock)
    where [CreatedOn] between '%s' and '%s' 
    and [following]=1 and [followed]=0
    and user_id in(
        SELECT [user_id]
        FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
        where version_tag ='viva'
    )

    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    db='mozone_friend'
    key='newly_created_friend_unidirectional_relation_unique'
    sql=r'''
    
    SELECT count(distinct [user_id])
    FROM [mozone_friend].[dbo].[friendship] with(nolock)
    where [CreatedOn] between '%s' and '%s' 
    and [following]=1 and [followed]=0
    and user_id in(
        SELECT [user_id]
        FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
        where version_tag ='viva'
    )
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # newly created mutual relaltion
    
    db='mozone_friend'
    key='newly_created_friend_mutual_relation_unique_base'
    sql=r'''
    
    SELECT count([id])
    FROM [mozone_friend].[dbo].[friendship] with(nolock)
    where [ModifiedOn] between '%s' and '%s' 
    and [following]=1 and [followed]=1
    and user_id in(
        SELECT [user_id]
        FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
        where version_tag ='viva'
    )
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    db='mozone_friend'
    key='newly_created_friend_mutual_relation_unique'
    sql=r'''
    
    SELECT count(distinct [user_id])
    FROM [mozone_friend].[dbo].[friendship] with(nolock)
    where [ModifiedOn] between '%s' and '%s' 
    and [following]=1 and [followed]=1
    and user_id in(
        SELECT [user_id]
        FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
        where version_tag ='viva'
    )
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
        
    # new created blocking relaltion
    
    db='mozone_friend'
    key='newly_blocked_friend_relation_unique_base'
    sql=r'''
    
    SELECT count([id])
    FROM [mozone_friend].[dbo].[friendship] with(nolock)
    where [CreatedOn] between '%s' and '%s' 
    and [blocking]=1
    and user_id in(
        SELECT [user_id]
        FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
        where version_tag ='viva'
    )
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    db='mozone_friend'
    key='newly_blocked_friend_relation_unique'
    sql=r'''
    
    SELECT count(distinct [user_id])
    FROM [mozone_friend].[dbo].[friendship] with(nolock)
    where [CreatedOn] between '%s' and '%s' 
    and [blocking]=1
    and user_id in(
        SELECT [user_id]
        FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
        where version_tag ='viva'
    )
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    """

    # friend relation distribution of all sub user
    # 10240 level means more than 5120
    
    db=''
    key='sub_user_added_friend_distribution'
    sql=r'''
    
    select num_of_friend_added
    ,count(*) as num_of_user

    from(
        SELECT `sns-add_other_as_friend`

        ,pow(2,
        case 
        when log2(`sns-add_other_as_friend`*1.0/5)<0 then 0
        when log2(`sns-add_other_as_friend`*1.0/5)>10 then 11
        else ceil(log2(`sns-add_other_as_friend`*1.0/5))
        end) *5 as num_of_friend_added

        FROM `mozat_clustering`.`user_figure_base` 

        where `oem_id`=2 and `subscribe-in_subscription`=1
        and `sns-add_other_as_friend` is not null
    ) a

    group by num_of_friend_added
    order by num_of_friend_added asc
    
    
    '''
        
    print 'SQL Server:'+sql
    values=helper_mysql.fetch_dict(sql)
    print values

    s=0
    for k,v in values.iteritems():    
        helper_mysql.put_raw_data(oem_name,stat_category,key,date_today+'_'+k,v)
        s=s+int(v)

    key='sub_user_added_friend_distribution_base'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,s)


    # friend relation distribution of all unsub user
    # 10240 level means more than 5120
    
    db=''
    key='unsub_user_added_friend_distribution'
    sql=r'''
    
    select num_of_friend_added
    ,count(*) as num_of_user

    from(
        SELECT `sns-add_other_as_friend`

        ,pow(2,
        case 
        when log2(`sns-add_other_as_friend`*1.0/5)<0 then 0
        when log2(`sns-add_other_as_friend`*1.0/5)>10 then 11
        else ceil(log2(`sns-add_other_as_friend`*1.0/5))
        end) *5 as num_of_friend_added

        FROM `mozat_clustering`.`user_figure_base` 

        where `oem_id`=2 and `subscribe-in_subscription`=0
        and `sns-add_other_as_friend` is not null
    ) a

    group by num_of_friend_added
    order by num_of_friend_added asc
    
    '''
        
    print 'SQL Server:'+sql
    values=helper_mysql.fetch_dict(sql)
    print values

    s=0
    for k,v in values.iteritems():    
        helper_mysql.put_raw_data(oem_name,stat_category,key,date_today+'_'+k,v)
        s=s+int(v)

    key='unsub_user_added_friend_distribution_base'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,s)
    
    """

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat_friend_relation(my_date)
        #time.sleep(10)
