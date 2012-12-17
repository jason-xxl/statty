import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
from tool_qlx import base 
import tool_qlx
import helper_mail
from qlx_src.helper import html

def stat_user_generated_content(my_date): # run on 6:00 a.m. , calculate yesterday's data

    oem_name='Vodafone'
    stat_category='user_generated_content'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    date_today=start_time.replace(' 06:00:00','')

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
    
    
    # rss comment 1 day
    
    key='rss_comment_1_day'

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='FEED_DB'
    sql="select count(id) from [FEED_DB].[dbo].[ITEM_COMMENT_SHABIK] with(nolock) where [DATE]>='%s' and [DATE]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # rss comment 1 week
    
    key='rss_comment_1_week'

    start_time=helper_regex.time_floor(my_date-3600*24*6).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    
    db='FEED_DB'
    sql="select count(id) from [FEED_DB].[dbo].[ITEM_COMMENT_SHABIK] with(nolock) where [DATE]>='%s' and [DATE]<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    """
    
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
    --and [user_id] in (
    --    SELECT [user_id]
    --      FROM [mozone_user].[dbo].[ProfileSub] with(nolock)
    --    where version_tag ='umniah.jor'
    --)''' % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_row(config.conn_vodafone_88,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_base',date_today,value['msg'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique',date_today,value['uv'])
    helper_mysql.put_raw_data(oem_name,stat_category,'message_1_day_unique_average',date_today,value['avg'])


    
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


def send_email(time_):
    assert(tool_qlx.get_utc_datetime(time_, tool_qlx.UTC2).date() \
           == tool_qlx.get_utc_datetime(time_, tool_qlx.UTC8).date())
    current_date = tool_qlx.get_utc_datetime(time_, tool_qlx.UTC2).date().strftime('%Y-%m-%d')
    print(current_date)
    
    vodafone = base(dbcon=None, table='raw_data', \
               operator='Vodafone',service='user_generated_content',current_date=current_date)
    vodafone.view_name = 'Vodafone - User Generated Content Daily' 
    vodafone.view_url = 'http://statportal.morange.com/xstat/view.php?id=696'

    rows = [dict(key='photo_1_day',sub_key='',view_name='Photo'), \
            dict(key='status_1_day_unique_base',sub_key='',view_name='Status'),\
            dict(key='poll_1_day',sub_key='',view_name='Poll'), \
            dict(key='message_1_day_unique_base',sub_key='',view_name='Message'),\
            dict(key='photo_comment_1_day',sub_key='',view_name='Photo Comment'), \
            dict(key='status_comment_1_day_unique_base',sub_key='',view_name='Status Comment'),\
            dict(key='poll_comment_1_day',sub_key='',view_name='Poll Comment')
            ]
    vodafone.load_raw_data_from_db(rows)
    print(rows)
    
    t = html.Table()
    t.set_caption(vodafone.operator+' '+current_date)
    t.set_headers(['Service','Daily'])
    for row in rows:
        t.append_row([row['view_name'],row['value']])
    t.set_footer(html.URL(vodafone.view_name,vodafone.view_url))
    print(t)
    helper_mail.send_mail(title=vodafone.view_name,\
                      content_html=str(t),target_mails=config.mail_targets_vodafone_stat,\
                      source_mail=config.mail_from)  

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_user_generated_content(my_date)
        send_email(my_date)