import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config



def stat_friend_relation(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
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
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    db='mozone_friend'
    key='newly_created_friend_unidirectional_relation_unique'
    sql=r'''
    
    SELECT count(distinct [user_id])
    FROM [mozone_friend].[dbo].[friendship] with(nolock)
    where [CreatedOn] between '%s' and '%s' 
    and [following]=1 and [followed]=0
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
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
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    db='mozone_friend'
    key='newly_created_friend_mutual_relation_unique'
    sql=r'''
    
    SELECT count(distinct [user_id])
    FROM [mozone_friend].[dbo].[friendship] with(nolock)
    where [ModifiedOn] between '%s' and '%s' 
    and [following]=1 and [followed]=1
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
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
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    db='mozone_friend'
    key='newly_blocked_friend_relation_unique'
    sql=r'''
    
    SELECT count(distinct [user_id])
    FROM [mozone_friend].[dbo].[friendship] with(nolock)
    where [CreatedOn] between '%s' and '%s' 
    and [blocking]=1
    
    ''' % (start_time,end_time)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat_friend_relation(my_date)
        #time.sleep(10)
