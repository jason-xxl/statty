import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import common_shabik_360

def stat_user_generated_content(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Shabik_360'
    stat_category='user_generated_content'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')

    
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    exec('from user_id_filter.user_id_filter_shabik_360_'+current_date.replace('-','_')+' import get_filtered_dict')

    common_shabik_360.init_user_id_range(current_date)

    # photo 1 day
    
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql=r'''
    
    select user_id,count(id) 
    from mozone_tag.dbo.photos_tags with(nolock) 
    where createdon>='%s' and createdon<'%s'
    group by user_id

    ''' % (start_time,end_time)

        
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_stc,sql)
    values=get_filtered_dict(values)

    grouped_values=common_shabik_360.get_categorized_groups(values)
    for k,v in grouped_values.iteritems():
        key='photo_1_day_by_user_group'
        print sum(v.values())
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,sum(v.values()),table_name='raw_data_shabik_360')

    
    # poll 1 day
    
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
    
    select user_id,count(id) 
    
    from mozone_tag.dbo.polls_tags with(nolock)
    where createdon>='%s' and createdon<'%s'
    group by user_id

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_stc,sql)
    values=get_filtered_dict(values)

    grouped_values=common_shabik_360.get_categorized_groups(values)
    for k,v in grouped_values.iteritems():
        key='poll_1_day_by_user_group'
        print sum(v.values())
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,sum(v.values()),table_name='raw_data_shabik_360')

    


    
    # photo comment 1 day

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
    
    select owner_id,count(id) 
    
    from mozone_tag.dbo.comments_tags with(nolock)
    where createdon>='%s' and createdon<'%s'
    and owner_type=11
    
    group by owner_id

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_stc,sql)
    values=get_filtered_dict(values)

    grouped_values=common_shabik_360.get_categorized_groups(values)
    for k,v in grouped_values.iteritems():
        key='photo_comment_1_day_by_user_group'
        print sum(v.values())
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,sum(v.values()),table_name='raw_data_shabik_360')


    
    
    # poll comment 1 day
 
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
    
    select owner_id,count(id) 
    
    from mozone_tag.dbo.comments_tags with(nolock)
    where createdon>='%s' and createdon<'%s'
    and owner_type=12
    
    group by owner_id

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_stc,sql)
    values=get_filtered_dict(values)

    grouped_values=common_shabik_360.get_categorized_groups(values)
    for k,v in grouped_values.iteritems():
        key='poll_comment_1_day_by_user_group'
        print sum(v.values())
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,sum(v.values()),table_name='raw_data_shabik_360')


    
    
    # rss comment 1 day
    
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
    
    select COMMENT_USER_ID,count(id) 
    
    from [FEED_DB].[dbo].[ITEM_COMMENT_SHABIK] with(nolock) 
    where [DATE]>='%s' and [DATE]<'%s'
    
    group by COMMENT_USER_ID

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_stc,sql)
    values=get_filtered_dict(values)

    grouped_values=common_shabik_360.get_categorized_groups(values)
    for k,v in grouped_values.iteritems():
        key='rss_comment_1_day_by_user_group'
        print sum(v.values())
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,sum(v.values()),table_name='raw_data_shabik_360')

    


    # message 1 day
    
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    
    db='mozone_tag'
    sql='''
    
    select user_id,count(id) 
    
    from [mozone].[dbo].[msgs] with(nolock)
    where createdon>='%s' and createdon<'%s'
    
    group by user_id

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_stc,sql)
    values=get_filtered_dict(values)

    grouped_values=common_shabik_360.get_categorized_groups(values)
    for k,v in grouped_values.iteritems():
        key='message_1_day_unique_by_user_group'
        print len(v)
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,len(v),table_name='raw_data_shabik_360')

        key='message_1_day_unique_base_by_user_group'
        print sum(v.values())
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,sum(v.values()),table_name='raw_data_shabik_360')

    
    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_user_generated_content(my_date)
