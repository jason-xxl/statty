import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
from user_id_filter import user_id_filter_ais


def stat_user_generated_content(my_date): # run on 0:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='user_generated_content_only_ais'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    
    # update 1 day
    
    key='status_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    sql='''

    select `author`,count(id) as `updates`
    from Status.status
    where 
    created_on>='%s' 
    and created_on<'%s' 
    group by `author`

    ''' % (start_time,end_time)

    print 'MySQL:'+sql

    values=helper_mysql.fetch_dict(sql,db_conn=config.conn_mozat_modb)
    values=user_id_filter_ais.get_filtered_dict(values)

    print len(values)
    print sum([int(v) for v in values.values()])
    helper_mysql.put_raw_data(oem_name,stat_category,'status_1_day_unique_base',date_today,sum([int(v) for v in values.values()]))
    
    
    # update comment 1 day
    
    key='status_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    sql='''

    select `author`,count(a.`id`) as `total`
    from `Comment`.`comment_authertgtidtype2comment_index` a
    inner join `Comment`.`comment`  
    on `Comment`.a.`id`=`Comment`.`comment`.id 
    where
    a.`target_type` = 'status' 
    and `Comment`.`comment`.`created_on`>='%s' 
    and `Comment`.`comment`.`created_on`<'%s' 
    group by `author` 

    ''' % (start_time,end_time)

    print 'MySQL:'+sql

    values=helper_mysql.fetch_dict(sql,db_conn=config.conn_mozat_modb)
    values=user_id_filter_ais.get_filtered_dict(values)

    print len(values)
    print sum([int(v) for v in values.values()])
    helper_mysql.put_raw_data(oem_name,stat_category,'status_comment_1_day_unique_base',date_today,sum([int(v) for v in values.values()]))


    # photo 1 day
    
    key='photo_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql="""select count(id) from mozone.dbo.photos with(nolock) where createdon>='%s' and createdon<'%s' 
        and user_id in (SELECT user_id FROM [mozone_user].[dbo].[ProfileSub] with(nolock) where version_tag ='fast_ais') """ \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    
    # poll 1 day
    
    key='poll_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql="""select count(id) from mozone_poll.dbo.polls with(nolock) where createdon>='%s' and createdon<'%s' 
        and user_id in (SELECT user_id FROM [mozone_user].[dbo].[ProfileSub] with(nolock) where version_tag ='fast_ais') """ \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # photo comment 1 day
    
    key='photo_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql="""select count(id) from mozone.dbo.comments with(nolock) where createdon>='%s' and createdon<'%s' and owner_type=11 
        and user_id in (SELECT user_id FROM [mozone_user].[dbo].[ProfileSub] with(nolock) where version_tag ='fast_ais') """ \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # poll comment 1 day
    
    key='poll_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql="""select count(id) from mozone.dbo.comments with(nolock) where createdon>='%s' and createdon<'%s' and owner_type=12
        and user_id in (SELECT user_id FROM [mozone_user].[dbo].[ProfileSub] with(nolock) where version_tag ='fast_ais') """ \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # rss comment 1 day
    
    key='rss_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='FEED_DB'
    sql="""select count(id) from [FEED_DB].[dbo].[ITEM_COMMENT] with(nolock) where [DATE]>='%s' and [DATE]<'%s'
        and [COMMENT_USER_ID] in (SELECT user_id FROM [mozone_user].[dbo].[ProfileSub] with(nolock) where version_tag ='fast_ais') """ \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    # vote 1 day
    
    key='vote_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''
    select total_rate, total_rate_unique, total_rate*1.0/NULLIF(total_rate_unique,0) total_rate_avg from (
        SELECT 
        count([id]) as total_rate
        ,count(distinct [user_id]) as total_rate_unique
        FROM [mozone].[dbo].[rating]
        where [CreatedOn]>='%s' and [CreatedOn]<'%s'
        and user_id in (SELECT user_id FROM [mozone_user].[dbo].[ProfileSub] with(nolock) where version_tag ='fast_ais') 
    ) a
    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'rate_1_day_unique_base',date_today,value['total_rate'])
    helper_mysql.put_raw_data(oem_name,stat_category,'rate_1_day_unique',date_today,value['total_rate_unique'])
    helper_mysql.put_raw_data(oem_name,stat_category,'rate_1_day_unique_average',date_today,value['total_rate_avg'])


    
    # message 1 day
    
    key='message_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''
    select msg, uv, msg/NULLIF(uv,0) avg from (
        select count(id) msg,count(distinct user_id) uv FROM [mozone].[dbo].[msgs] with(nolock)
        where createdon>='%s' and createdon<'%s'
        and user_id in (SELECT user_id FROM [mozone_user].[dbo].[ProfileSub] with(nolock) where version_tag ='fast_ais') 
    ) a
    ''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_base',date_today,value['msg'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique',date_today,value['uv'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_average',date_today,value['avg'])



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_user_generated_content(my_date)
