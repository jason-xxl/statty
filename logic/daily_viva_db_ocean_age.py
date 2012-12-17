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


def stat_ocean_age(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Viva'
    stat_category='ocean_age'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    
    # oa level 1
    
    key='level_1_user_unique'
    
    db='OceanAgeVivaKu'
    sql="select count(*) from OceanAgeVivaKu.dbo.Fisher with(nolock) where shipyardlevel=1 "
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


    # oa no money user
    
    key='level_1.1_user_unique'
    
    db='OceanAgeVivaKu'
    sql="select count(*) from OceanAgeVivaKu.dbo.Fisher with(nolock) where shipyardlevel=1 and money>0 "
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # oa level 2
    
    key='level_2_user_unique'
    
    db='OceanAgeVivaKu'
    sql="select count(*) from OceanAgeVivaKu.dbo.Fisher with(nolock) where shipyardlevel=2 "
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    # oa level 3
    
    key='level_3_user_unique'
    
    db='OceanAgeVivaKu'
    sql="select count(*) from OceanAgeVivaKu.dbo.Fisher with(nolock) where shipyardlevel=3 "
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # oa level 4
    
    key='level_4_user_unique'
    
    db='OceanAgeVivaKu'
    sql="select count(*) from OceanAgeVivaKu.dbo.Fisher with(nolock) where shipyardlevel=4 "
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # oa level 5
    
    key='level_5_user_unique'
    
    db='OceanAgeVivaKu'
    sql="select count(*) from OceanAgeVivaKu.dbo.Fisher with(nolock) where shipyardlevel=5 "
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # oa level 6
    
    key='level_6_user_unique'
    
    db='OceanAgeVivaKu'
    sql="select count(*) from OceanAgeVivaKu.dbo.Fisher with(nolock) where shipyardlevel=6 "
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # oa level 7
    
    key='level_7_user_unique'
    
    db='OceanAgeVivaKu'
    sql="select count(*) from OceanAgeVivaKu.dbo.Fisher with(nolock) where shipyardlevel=7 "
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    # oa no money user
    
    key='no_money_user_unique'
    
    db='OceanAgeVivaKu'
    sql="select count(*) from OceanAgeVivaKu.dbo.Fisher with(nolock) where money=0 "
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    
    
    # oa monthly player
    
    key='monthly_user_unique'

    start_time=helper_regex.time_floor(my_date-3600*24*30)
    
    db='OceanAge'
    sql="select count(distinct monetId) from OceanAgeVivaKu.dbo.Fisher with(nolock) where [lastLogin]>='%s'" \
        % (start_time,)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_ocean_age(my_date)
