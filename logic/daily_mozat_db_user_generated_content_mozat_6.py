import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_user_generated_content(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='user_generated_content_mozat_6'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    
    # update 1 day
    
    key='status_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
         select count(id) as total
         from DB81.MoStatus.dbo.Status with(nolock)
         where 
         dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')>='%s' 
         and dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')<'%s' ''' \
         % (start_time,end_time)

    print 'SQL Server:'+sql

    value=helper_sql_server.fetch_row(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'status_1_day_unique_base',date_today,str(value['total']))


    # update comment 1 day
    
    key='status_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
         select count(id) as total
         from DB81.MoStatus.dbo.Status_Comment with(nolock)
         where 
         dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')>='%s' 
         and dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')<'%s' ''' \
         % (start_time,end_time)

    print 'SQL Server:'+sql

    value=helper_sql_server.fetch_row(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'status_comment_1_day_unique_base',date_today,value['total'])
    
    
    # like status 1 day
    
    key=''

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
         select 
         count(id) as total
         ,count(distinct id_from) as user_count
         ,case when count(distinct id_from)=0 then 0 else count(id)*1.0/count(distinct id_from) end as average
         from DB81.MoStatus.dbo.Object_LikedByUser with(nolock)
         where 
         dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')>='%s' 
         and dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')<'%s' ''' \
         % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'rate_1_day_unique_base',date_today,value['total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'rate_1_day_unique',date_today,value['user_count'])
    helper_mysql.put_raw_data(oem_name,stat_category,'rate_1_day_unique_average',date_today,value['average'])



    
    #filter sql tpl

    filter_sql_tpl=r'''

    select COUNT(id) as total, count(distinct user_id) as user_count
    ,case when count(distinct user_id)=0 then 0 else count(id)*1.0/count(distinct user_id) end as average

    from(
        %%s 
    ) as a
    left join 
    (
        select m_user_id
        from openquery(MYSQL158,'
        SELECT 
        distinct cast(sub_key as UNSIGNED INTEGER) as m_user_id 
        from mozat_stat.raw_data_user_device 
        where `oem_name`="Mozat" and `category`="moagent" and `key`="app_page_by_user_id_client_version_first_int_value" 
        /*and `date`="%s"*/ and `value`=6
        ')
    ) as b
    on a.user_id=b.m_user_id
    where b.m_user_id is not null

    ''' % (date_today,)





    # photo 1 day
    
    key='photo_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''select id,user_id 
            from DB81.mozone.dbo.photos with(nolock) 
            where createdon>='%s' and createdon<'%s' 
            ''' \
         % (start_time,end_time)
    
    sql=filter_sql_tpl % (sql,)
    print 'SQL Server:'+sql

    value=helper_sql_server.fetch_scalar_int(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # saying 1 day
    
    key='saying_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''select id, user_id
         from DB81.mozone.dbo.moblog_msgs with(nolock)
         where createdon>='%s' and createdon<'%s' ''' \
         % (start_time,end_time)

    sql=filter_sql_tpl % (sql,)
    print 'SQL Server:'+sql

    value=helper_sql_server.fetch_row(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'saying_1_day_unique_base',date_today,value['total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'saying_1_day_unique',date_today,value['user_count'])
    helper_mysql.put_raw_data(oem_name,stat_category,'saying_1_day_unique_average',date_today,value['average'])

    


    # poll 1 day
    
    key='poll_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''select id,user_id
        from DB81.mozone_poll.dbo.polls with(nolock) 
        where createdon>='%s' and createdon<'%s' ''' % (start_time,end_time)

    
    sql=filter_sql_tpl % (sql,)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # photo comment 1 day
    
    key='photo_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''select id,user_id 
         from DB81.mozone.dbo.comments with(nolock) 
         where createdon>='%s' and createdon<'%s' and owner_type=11 ''' \
         % (start_time,end_time)

    sql=filter_sql_tpl % (sql,)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    
    # poll comment 1 day
    
    key='poll_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''select id,user_id
        from DB81.mozone.dbo.comments with(nolock) 
        where createdon>='%s' and createdon<'%s' and owner_type=12''' \
         % (start_time,end_time)

    sql=filter_sql_tpl % (sql,)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    
    
    # rss comment 1 day
    
    key='rss_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='FEED_DB'
    sql=r'''select id,COMMENT_USER_ID as user_id
        from DB81.[FEED_DB].[dbo].[ITEM_COMMENT] with(nolock) 
        where [DATE]>='%s' and [DATE]<'%s' ''' \
         % (start_time,end_time)

    sql=filter_sql_tpl % (sql,)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    
    # message 1 day
    
    key='message_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''
    select id,user_id
    FROM DB81.[mozone].[dbo].[msgs] with(nolock)
    where createdon>='%s' and createdon<'%s'
    ''' % (start_time,end_time)

    sql=filter_sql_tpl % (sql,)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_base',date_today,value['total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique',date_today,value['user_count'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_average',date_today,value['average'])

    



    """
    # vote 1 day
    
    key='vote_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''

    SELECT 
    id,user_id
    FROM DB81.[mozone].[dbo].[rating] with(nolock)
    where [CreatedOn]>='%s' and [CreatedOn]<'%s'

    ''' % (start_time,end_time)
    
    sql=filter_sql_tpl % (sql,)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'rate_1_day_unique_base',date_today,value['total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'rate_1_day_unique',date_today,value['user_count'])
    helper_mysql.put_raw_data(oem_name,stat_category,'rate_1_day_unique_average',date_today,value['average'])
    """






if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_user_generated_content(my_date)
