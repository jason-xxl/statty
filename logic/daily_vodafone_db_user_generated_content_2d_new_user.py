import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_user_generated_content(my_date): # run on 6:00 a.m. , calculate yesterday's data

    oem_name='Vodafone'
    stat_category='user_generated_content_only_2d_new_user'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    date_today=start_time.replace(' 06:00:00','')

    start_time_2d=helper_regex.time_floor(my_date-3600*24).replace(' 00:00:00',' 06:00:00')


    """

    # update 1 day
    
    key='status_1_day'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone_tag'
    sql='''
         select count(id) as total
         from MoStatus.dbo.User_Status with(nolock)
         where 
         id_from>30000
         and dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')>='%s' 
         and dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')<'%s' ''' \
         % (start_time,end_time)

    print 'SQL Server:'+sql

    value=helper_sql_server.fetch_row(config.conn_vodafone_87,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'status_1_day_unique_base',date_today,value['total'])

    # update comment 1 day
    
    key='status_comment_1_day'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone_tag'
    sql='''
         select count(id) as total
         from MoStatus.dbo.Comment with(nolock)
         where 
         dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')>='%s' 
         and dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')<'%s' ''' \
         % (start_time,end_time)

    print 'SQL Server:'+sql

    value=helper_sql_server.fetch_row(config.conn_vodafone_87,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'status_comment_1_day_unique_base',date_today,value['total'])

    # photo 1 day
    
    key='photo_1_day'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone'
    sql="select count(id) from mozone.dbo.photos with(nolock) where CreatedOn>='%s' and CreatedOn<'%s' and user_id>30000" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    # photo 1 week
    
    key='photo_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone'
    sql="select count(id) from mozone.dbo.photos with(nolock) where CreatedOn>='%s' and CreatedOn<'%s' and user_id>30000" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # saying 1 day
    key='saying_1_day'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone'
    sql='''select 
        count(id) as msg
        ,count(distinct user_id) as uv,count(id)/case when count(distinct user_id)>0 then count(distinct user_id) else 1 end as avg 
        from mozone.dbo.moblog_msgs with(nolock)
        where createdon>='%s' and createdon<'%s' ''' \
        % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'saying_1_day_unique_base',date_today,value['msg'])
    helper_mysql.put_raw_data(oem_name,stat_category,'saying_1_day_unique',date_today,value['uv'])
    helper_mysql.put_raw_data(oem_name,stat_category,'saying_1_day_unique_average',date_today,value['avg'])    

    
    # saying 1 week
    
    key='saying_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[moblog_msgs] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # poll 1 day
    
    key='poll_1_day'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='polls'
    sql="select count(id) from [mozone_poll].[dbo].[polls] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # poll 1 week
    
    key='poll_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='polls'
    sql="select count(id) from [mozone_poll].[dbo].[polls] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # photo comment 1 day
    
    key='photo_comment_1_day'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[comments] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s' and owner_type=11" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # photo comment 1 week
    
    key='photo_comment_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[comments] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s' and owner_type=11" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # poll comment 1 day
    
    key='poll_comment_1_day'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[comments] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s' and owner_type=12" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # poll comment 1 week
    
    key='poll_comment_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[comments] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s' and owner_type=12" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    """
    
    filter_sql_2d_new_user=r'''

    SELECT [user_id]
    FROM [mozone_user].[dbo].[Profile] with(nolock)
    where creationDate>='%s' and creationDate<'%s'

    ''' % (start_time_2d,end_time)


    # message 1 day
    
    key='message_1_day'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone_tag'
    sql=r'''

    select 
    count(id) msg
    ,count(distinct user_id) uv,count(id)/case when count(distinct user_id)>0 then count(distinct user_id) else 1 end as avg 
    FROM [mozone].[dbo].[msgs] with(nolock)
    where createdon>='%s' and createdon<'%s'
    and [user_id] in (
    %s    
    )''' % (start_time,end_time,filter_sql_2d_new_user)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_base',date_today,value['msg'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique',date_today,value['uv'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_average',date_today,value['avg'])

    """
    
    # message 7 day
    
    key='message_7_day'

    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='mozone_tag'
    sql=r'''

    select count(*) FROM [mozone].[dbo].[msgs] with(nolock)
    where createdon>='%s' and createdon<'%s'
    --and [user_id] in (
    --    SELECT [user_id]
    --      FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
    --    where version_tag ='umniah.jor'
    --)''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    """

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_user_generated_content(my_date)
