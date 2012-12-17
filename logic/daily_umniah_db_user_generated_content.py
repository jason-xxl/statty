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

    oem_name='Umniah'
    stat_category='user_generated_content'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')
    
    # photo 1 day
    
    key='photo_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone'
    sql="select count(id) from mozone.dbo.photos with(nolock) where CreatedOn>='%s' and CreatedOn<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # photo 1 week
    
    key='photo_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone'
    sql="select count(id) from mozone.dbo.photos with(nolock) where CreatedOn>='%s' and CreatedOn<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # saying 1 day
    key='saying_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone'
    sql='''select count(id) as msg,count(distinct user_id) as uv,count(id)/count(distinct user_id) as avg from mozone.dbo.moblog_msgs with(nolock)
         where createdon>='%s' and createdon<'%s' ''' \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'saying_1_day_unique_base',date_today,value['msg'])
    helper_mysql.put_raw_data(oem_name,stat_category,'saying_1_day_unique',date_today,value['uv'])
    helper_mysql.put_raw_data(oem_name,stat_category,'saying_1_day_unique_average',date_today,value['avg'])    

    
    # saying 1 week
    
    key='saying_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[moblog_msgs] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # poll 1 day
    
    key='poll_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='polls'
    sql="select count(id) from [mozone_poll].[dbo].[polls] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # poll 1 week
    
    key='poll_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6)
    end_time=helper_regex.time_ceil(my_date)
    
    db='polls'
    sql="select count(id) from [mozone_poll].[dbo].[polls] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # photo comment 1 day
    
    key='photo_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[comments] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s' and owner_type=11" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # photo comment 1 week
    
    key='photo_comment_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[comments] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s' and owner_type=11" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # poll comment 1 day
    
    key='poll_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[comments] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s' and owner_type=12" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # poll comment 1 week
    
    key='poll_comment_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone'
    sql="select count(id) from [mozone].[dbo].[comments] with(nolock) where [CreatedOn]>='%s' and [CreatedOn]<'%s' and owner_type=12" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    
    
    # rss comment 1 day
    
    key='rss_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='FEED_DB'
    sql="select count(id) from [FEED_DB].[dbo].[ITEM_COMMENT_SHABIK] with(nolock) where [DATE]>='%s' and [DATE]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # rss comment 1 week
    
    key='rss_comment_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6)
    end_time=helper_regex.time_ceil(my_date)
    
    db='FEED_DB'
    sql="select count(id) from [FEED_DB].[dbo].[ITEM_COMMENT_SHABIK] with(nolock) where [DATE]>='%s' and [DATE]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # message 1 day
    
    key='message_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''

    select count(id) msg,count(distinct user_id) uv,count(id)/count(distinct user_id) avg FROM [mozone].[dbo].[msgs] with(nolock)
    where createdon>='%s' and createdon<'%s'
    and [user_id] in (
        SELECT [user_id]
          FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
        where version_tag ='umniah.jor'
    )''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_base',date_today,value['msg'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique',date_today,value['uv'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_average',date_today,value['avg'])


    
    # message 7 day
    
    key='message_7_day'

    start_time=helper_regex.time_floor(my_date-3600*24*6)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''

    select count(*) FROM [mozone].[dbo].[msgs] with(nolock)
    where createdon>='%s' and createdon<'%s'
    and [user_id] in (
        SELECT [user_id]
          FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
        where version_tag ='umniah.jor'
    )''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_umniah,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

  



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_user_generated_content(my_date)
