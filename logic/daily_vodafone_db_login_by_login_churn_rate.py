import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config



def stat_login(my_date):

    oem_name='Vodafone'
    stat_category='login'
        
    for i in range(0,110):

        start_time=helper_regex.time_floor(my_date-3600*24*i).replace(' 00:00:00',' 06:00:00')
        end_time=helper_regex.time_ceil(my_date-3600*24*i).replace(' 00:00:00',' 06:00:00')
        date_today=start_time.replace(' 06:00:00','')

        db='mozone_user'
        key='user_sub_login_by_login_retain_trend_unique_user'
        db_name='raw_data_trend'
        sql=r'''

        SELECT 

        count([user_id]) [total]
        ,sum(case when [lastLogin] is not null then 1 else 0 end) [loginned]
        ,sum(case when [lastLogin]>=dateadd(dd,1,'%(start_time)s') then 1 else 0 end) as [1day]
        ,sum(case when [lastLogin]>=dateadd(dd,2,'%(start_time)s') then 1 else 0 end) as [2day]
        ,sum(case when [lastLogin]>=dateadd(dd,3,'%(start_time)s') then 1 else 0 end) as [3day]
        ,sum(case when [lastLogin]>=dateadd(dd,4,'%(start_time)s') then 1 else 0 end) as [4day]
        ,sum(case when [lastLogin]>=dateadd(dd,5,'%(start_time)s') then 1 else 0 end) as [5day]
        ,sum(case when [lastLogin]>=dateadd(dd,6,'%(start_time)s') then 1 else 0 end) as [6day]
        ,sum(case when [lastLogin]>=dateadd(dd,7,'%(start_time)s') then 1 else 0 end) as [7day]
        ,sum(case when [lastLogin]>=dateadd(dd,14,'%(start_time)s') then 1 else 0 end) as [14day]
        ,sum(case when [lastLogin]>=dateadd(dd,21,'%(start_time)s') then 1 else 0 end) as [21day]
        ,sum(case when [lastLogin]>=dateadd(dd,28,'%(start_time)s') then 1 else 0 end) as [28day]
        ,sum(case when [lastLogin]>=dateadd(dd,35,'%(start_time)s') then 1 else 0 end) as [35day]
        ,sum(case when [lastLogin]>=dateadd(dd,42,'%(start_time)s') then 1 else 0 end) as [42day]
        ,sum(case when [lastLogin]>=dateadd(dd,49,'%(start_time)s') then 1 else 0 end) as [49day]
        ,sum(case when [lastLogin]>=dateadd(dd,56,'%(start_time)s') then 1 else 0 end) as [56day]

        FROM [mozone_user].[dbo].[Profile]
        where
        [lastLogin]>='%(start_time)s'
        and [lastLogin]<'%(end_time)s'
        and [user_name] like '%%@voda%%'

        ''' % {'start_time':start_time,'end_time':end_time}
        
        print 'SQL Server:'+sql
        values=helper_sql_server.fetch_row(config.conn_vodafone_88,sql)

        print values

        helper_mysql.put_raw_data(oem_name,stat_category,key,'total',values['total'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'loginned',values['loginned'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'1day',values['1day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'2day',values['2day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'3day',values['3day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'4day',values['4day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'5day',values['5day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'6day',values['6day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'7day',values['7day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'14day',values['14day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'21day',values['21day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'28day',values['28day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'35day',values['35day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'42day',values['42day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'49day',values['49day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'56day',values['56day'],db_name,date_today)

        #return



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat_login(my_date)
