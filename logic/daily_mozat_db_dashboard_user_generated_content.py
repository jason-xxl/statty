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

    #country name translator

    translator_country_name=helper_mysql.fetch_dict(r'''
        SELECT 

        distinct cast(`value` as UNSIGNED INTEGER) as country_key
        ,value_text_dict.text as country

        from raw_data_user_info 
        left join value_text_dict

        on raw_data_user_info.`value`=value_text_dict.`id`

        where 
        `oem_name`="Mozat" 
        and `category`="login_service" 
        and `key`="login_response_provided_ip_by_user_id_last_login_country_first_text_value"
    ''')

    print translator_country_name

    #filter sql tpl

    filter_sql_tpl=r'''

    select 

    cast(c.country as int) as country
    ,COUNT(id) as total
    ,count(distinct user_id) as user_count
    ,COUNT(id)*1.0/count(distinct user_id) as average

    from(
        %%s 
    ) as a

    left join /*filter mozat 6*/
    (
        select m_user_id
        from openquery(MYSQL158,'
        SELECT 
        distinct cast(sub_key as UNSIGNED INTEGER) as m_user_id 
        from mozat_stat.raw_data_user_device 
        where `oem_name`="Mozat" and `category`="moagent" and `key`="app_page_by_date_by_user_id_client_version_first_int_value" 
        and `date`="%s" and `value`=6
        ')
    ) as b
    on a.user_id=b.m_user_id

    left join  /*sort by country*/
    (
        select 
        m_user_id,country

        from openquery(MYSQL158,'
            SELECT 

            distinct cast(sub_key as UNSIGNED INTEGER) as m_user_id 
            ,cast(`value` as UNSIGNED INTEGER) as country

            from raw_data_user_info 

            where 
            `oem_name`="Mozat" 
            and `category`="login_service" 
            and `key`="login_response_provided_ip_by_user_id_last_login_country_first_text_value"
        ')
    ) as c
    on a.user_id=c.m_user_id

    where b.m_user_id is not null
    and c.m_user_id is not null

    group by c.country
    order by c.country

    ''' % (date_today,)


    # update 1 day
    
    key='status_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
         select id_to as id, id_from as user_id
         from DB81.MoStatus.dbo.User_Status with(nolock)
         where 
         dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')>='%s' 
         and dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')<'%s' ''' \
         % (start_time,end_time)

    sql=filter_sql_tpl % (sql,)
    print 'SQL Server:'+sql

    values=helper_sql_server.fetch_rows_dict(config.conn_helper_db,sql)
    print values
    
    key='status_1_day'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['total'],table_name='raw_data_country')
    
    key='status_1_day_user_unique'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['user_count'],table_name='raw_data_country')
    

    """
    # update comment 1 day # need a mechanism to extract creator id from modb
    
    key='status_comment_1_day'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
         select count(id) as total
         from DB81.MoStatus.dbo.Comment with(nolock)
         where 
         dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')>='%s' 
         and dateadd(ss,([time]/10000000-62135596800),'01/01/1970 00:00:00 AM')<'%s' ''' \
         % (start_time,end_time)

    print 'SQL Server:'+sql

    value=helper_sql_server.fetch_row(config.conn_helper_db,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,'status_comment_1_day_unique_base',date_today,value['total'])
    """

    # photo 1 day

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

    values=helper_sql_server.fetch_rows_dict(config.conn_helper_db,sql)
    print values
    
    key='photo_1_day'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['total'],table_name='raw_data_country')
    
    key='photo_1_day_user_unique'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['user_count'],table_name='raw_data_country')


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

    values=helper_sql_server.fetch_rows_dict(config.conn_helper_db,sql)
    print values
    
    key='saying_1_day'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['total'],table_name='raw_data_country')
    
    key='saying_1_day_user_unique'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['user_count'],table_name='raw_data_country')


    # poll 1 day
    
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''select id,user_id
        from DB81.mozone_poll.dbo.polls with(nolock) 
        where createdon>='%s' and createdon<'%s' ''' % (start_time,end_time)

    
    sql=filter_sql_tpl % (sql,)

    print 'SQL Server:'+sql

    values=helper_sql_server.fetch_rows_dict(config.conn_helper_db,sql)
    print values
    
    key='poll_1_day'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['total'],table_name='raw_data_country')
    
    key='poll_1_day_user_unique'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['user_count'],table_name='raw_data_country')
    

    # photo comment 1 day

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''select id,user_id 
         from DB81.mozone.dbo.comments with(nolock) 
         where createdon>='%s' and createdon<'%s' and owner_type=11 ''' \
         % (start_time,end_time)

    sql=filter_sql_tpl % (sql,)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_rows_dict(config.conn_helper_db,sql)
    print values
    
    key='photo_comment_1_day'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['total'],table_name='raw_data_country')
    
    key='photo_comment_1_day_user_unique'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['user_count'],table_name='raw_data_country')
    

    # poll comment 1 day
    
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''select id,user_id
        from DB81.mozone.dbo.comments with(nolock) 
        where createdon>='%s' and createdon<'%s' and owner_type=12''' \
         % (start_time,end_time)

    sql=filter_sql_tpl % (sql,)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_rows_dict(config.conn_helper_db,sql)
    print values
    
    key='poll_comment_1_day'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['total'],table_name='raw_data_country')
    
    key='poll_comment_1_day_user_unique'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['user_count'],table_name='raw_data_country')


    # rss comment 1 day

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='FEED_DB'
    sql=r'''select id,COMMENT_USER_ID as user_id
        from DB81.[FEED_DB].[dbo].[ITEM_COMMENT] with(nolock) 
        where [DATE]>='%s' and [DATE]<'%s' ''' \
         % (start_time,end_time)

    sql=filter_sql_tpl % (sql,)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_rows_dict(config.conn_helper_db,sql)
    print values
    
    key='rss_comment_1_day'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['total'],table_name='raw_data_country')
    
    key='rss_comment_1_day_user_unique'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['user_count'],table_name='raw_data_country')


    
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
    values=helper_sql_server.fetch_rows_dict(config.conn_helper_db,sql)
    print values
    
    key='message_1_day'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['total'],table_name='raw_data_country')
    
    key='message_1_day_user_unique'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['user_count'],table_name='raw_data_country')

    

    
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
    values=helper_sql_server.fetch_rows_dict(config.conn_helper_db,sql)
    print values
    
    key='vote_1_day'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['total'],table_name='raw_data_country')
    
    key='vote_1_day_user_unique'
    for i in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,translator_country_name[i]+'_'+date_today,values[i]['user_count'],table_name='raw_data_country')





if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_user_generated_content(my_date)
