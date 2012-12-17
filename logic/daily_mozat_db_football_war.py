import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
from datetime import date
import time
import helper_regex
import config


def stat_football_war(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='football_war'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')

    
    # Max CCU Daily for Virtual League Shooter
    
    key='daily_ccu_for_virtual_league_shooter_max'
    
    db='footballwar'

    sql=r"""
    
    SELECT max([quantity]) as [max_ccu]
    FROM [footballwar].[dbo].[shooter_statistics] with(nolock)
    where [datetime]>='%s' and [datetime]<'%s'

    """ % (start_time,end_time)

    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mozat,sql)
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    
    # Daily Comments Total 
    
    key=''
    
    db='footballwar'

    sql=r"""
    
    SELECT
    
    count(*) as [daily_comment_total]
    ,count(case when target_type = 1 then 1 else null end) as [daily_team_comment_total]
    ,count(case when target_type = 2 then 1 else null end) as [daily_member_comment_total]

    FROM [footballwar].[dbo].[comments] with(nolock)
    where [create_time]>='%s' and [create_time]<'%s'

    """ % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_mozat,sql)
    print values
    
    helper_mysql.put_raw_data(oem_name,stat_category,'daily_comment_total',date_today,values['daily_comment_total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'daily_team_comment_total',date_today,values['daily_team_comment_total'])
    helper_mysql.put_raw_data(oem_name,stat_category,'daily_member_comment_total',date_today,values['daily_member_comment_total'])



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        
        my_date=time.time()-3600*24*i
        stat_football_war(my_date)
        
