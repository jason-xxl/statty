import helper_sql_server
import helper_mysql
import helper_collection
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_sub(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Vodafone'
    stat_category='sub'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    date_today=start_time.replace(' 06:00:00','')

    """
    public enum ChargeType { Sub = 1, Recurring = 2, OverdueByUser = 3, OverdueByCron = 4 };
    """



    # historical charged subscriber
    
    db=''
    sql='''

    SELECT 

    count(distinct MSISDN) as 'historical_charged_uv'

    FROM [shabik_mt].[dbo].[charge_logs] with(nolock)
    where [CreatedOn]<'%(end_time)s'
    and status=1

    ''' % {'end_time':end_time}

    print 'SQL Server:'+sql

    values=helper_sql_server.fetch_row(config.conn_vodafone_mt,sql)
    print values

    key='historical_subscription_charged_msisdn_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,'',values['historical_charged_uv'],date=date_today)


    # historical touched subscriber
    
    db=''
    sql='''

    SELECT 

    count(distinct MSISDN) as 'historical_uv'

    FROM [shabik_mt].[dbo].[charge_logs] with(nolock)
    where [CreatedOn]<'%(end_time)s'

    ''' % {'end_time':end_time}

    print 'SQL Server:'+sql

    values=helper_sql_server.fetch_row(config.conn_vodafone_mt,sql)
    print values

    key='historical_subscription_msisdn_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,'',values['historical_uv'],date=date_today)




    # revenue splitted
    
    day_length=helper_regex.get_matched_date(date_today)

    for length_name,length in day_length.iteritems():
        
        db=''
        sql='''

        SELECT 

        [amount]
        ,sum([amount]) as 'total'
        ,count(distinct [msisdn]) as 'unique'

        FROM [shabik_mt].[dbo].[charge_logs] with(nolock)

        where 
        [CreatedOn]>=dateadd(day,%(day)s,'%(end_time)s')
        and [CreatedOn]<'%(end_time)s'
        and [status]=1
        
        group by [amount]

        ''' % {'end_time':end_time,'day':-length}

        print 'SQL Server:'+sql
        values=helper_sql_server.fetch_rows_dict(config.conn_vodafone_mt,sql)
        print values

        for k,v in values.iteritems():
            key='subscription_by_type_%s_day_revenue_total'
            helper_mysql.put_raw_data(oem_name,stat_category,key % (length_name,),k,v['total'],date=date_today)
            key='subscription_by_type_%s_day_revenue_unique'
            helper_mysql.put_raw_data(oem_name,stat_category,key % (length_name,),k,v['unique'],date=date_today)



    # subscription active user
    
    key='subscription_active_user_unique'
    
    db=''
    sql='''
    


    declare @leng as int
    declare @count_1 as int
    declare @count_7 as int
    declare @count_14 as int
    declare @count_30 as int

    select @leng=30

    SELECT [MSISDN]
        ,max([id]*1000.0+[amount])-max([id]*1000.0) as last_charged_amount
        ,max([CreatedOn]) as last_charged_time
    into #t	
    FROM [shabik_mt].[dbo].[charge_logs] with(nolock)
    where [CreatedOn]>=dateadd(day,-30-@leng,'%(end_time)s')
    and [CreatedOn]<'%(end_time)s'
    and [status]=1
    group by [MSISDN]


    select @leng=30

    select @count_30=count([MSISDN])
    from #t
    where 
    dateadd(day,case 
        when last_charged_amount=5.00 then 30
        when last_charged_amount=2.50 then 14
        when last_charged_amount=1.25 then 7
        when last_charged_amount=1.40 then 7
        when last_charged_amount=1.20 then 6
        when last_charged_amount=1.00 then 5
        when last_charged_amount=0.80 then 4
        when last_charged_amount=0.60 then 3
        when last_charged_amount=0.40 then 2
        when last_charged_amount=0.20 then 1
        else 0
    end+1,last_charged_time)>=dateadd(day,-@leng,'%(end_time)s')


    select @leng=14

    select @count_14=count([MSISDN])
    from #t
    where 
    dateadd(day,case 
        when last_charged_amount=5.00 then 30
        when last_charged_amount=2.50 then 14
        when last_charged_amount=1.25 then 7
        when last_charged_amount=1.40 then 7
        when last_charged_amount=1.20 then 6
        when last_charged_amount=1.00 then 5
        when last_charged_amount=0.80 then 4
        when last_charged_amount=0.60 then 3
        when last_charged_amount=0.40 then 2
        when last_charged_amount=0.20 then 1
        else 0
    end+1,last_charged_time)>=dateadd(day,-@leng,'%(end_time)s')

    select @leng=7

    select @count_7=count([MSISDN])
    from #t
    where 
    dateadd(day,case 
        when last_charged_amount=5.00 then 30
        when last_charged_amount=2.50 then 14
        when last_charged_amount=1.25 then 7
        when last_charged_amount=1.40 then 7
        when last_charged_amount=1.20 then 6
        when last_charged_amount=1.00 then 5
        when last_charged_amount=0.80 then 4
        when last_charged_amount=0.60 then 3
        when last_charged_amount=0.40 then 2
        when last_charged_amount=0.20 then 1
        else 0
    end+1,last_charged_time)>=dateadd(day,-@leng,'%(end_time)s')


    select @leng=1

    select @count_1=count([MSISDN])
    from #t
    where 
    dateadd(day,case 
        when last_charged_amount=5.00 then 30
        when last_charged_amount=2.50 then 14
        when last_charged_amount=1.25 then 7
        when last_charged_amount=1.40 then 7
        when last_charged_amount=1.20 then 6
        when last_charged_amount=1.00 then 5
        when last_charged_amount=0.80 then 4
        when last_charged_amount=0.60 then 3
        when last_charged_amount=0.40 then 2
        when last_charged_amount=0.20 then 1
        else 0
    end+1,last_charged_time)>=dateadd(day,-@leng,'%(end_time)s')


    drop table #t

    select 
    @count_30 as count_30
    ,@count_14 as count_14
    ,@count_7 as count_7
    ,@count_1 as count_1



    ''' % {'end_time':end_time}

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_vodafone_mt,sql)
    print values
    helper_mysql.put_raw_data(oem_name,stat_category,key,'1',values['count_1'],date=date_today)
    helper_mysql.put_raw_data(oem_name,stat_category,key,'7',values['count_7'],date=date_today)
    helper_mysql.put_raw_data(oem_name,stat_category,key,'14',values['count_14'],date=date_today)
    helper_mysql.put_raw_data(oem_name,stat_category,key,'30',values['count_30'],date=date_today)






    # revenue
    
    db=''
    sql='''

    SELECT 

    sum(case when [CreatedOn]>=dateadd(day,-1,'%(end_time)s') then amount else 0 end) as 'total_1'
    ,sum(case when [CreatedOn]>=dateadd(day,-7,'%(end_time)s') then amount else 0 end) as 'total_7'
    ,sum(case when [CreatedOn]>=dateadd(day,-14,'%(end_time)s') then amount else 0 end) as 'total_14'
    ,sum(case when [CreatedOn]>=dateadd(day,-30,'%(end_time)s') then amount else 0 end) as 'total_30'

    ,count(distinct case when [CreatedOn]>=dateadd(day,-1,'%(end_time)s') then MSISDN else null end) as 'uv_1'
    ,count(distinct case when [CreatedOn]>=dateadd(day,-7,'%(end_time)s') then MSISDN else null end) as 'uv_7'
    ,count(distinct case when [CreatedOn]>=dateadd(day,-14,'%(end_time)s') then MSISDN else null end) as 'uv_14'
    ,count(distinct case when [CreatedOn]>=dateadd(day,-30,'%(end_time)s') then MSISDN else null end) as 'uv_30'

    FROM [shabik_mt].[dbo].[charge_logs] with(nolock)
    where [CreatedOn]>=dateadd(day,-35,'%(end_time)s')
    and [CreatedOn]<'%(end_time)s'
    and [status]=1

    ''' % {'end_time':end_time}

    print 'SQL Server:'+sql

    values=helper_sql_server.fetch_row(config.conn_vodafone_mt,sql)
    print values

    key='subscription_revenue_total'

    helper_mysql.put_raw_data(oem_name,stat_category,key,'1',values['total_1'],date=date_today)
    helper_mysql.put_raw_data(oem_name,stat_category,key,'7',values['total_7'],date=date_today)
    helper_mysql.put_raw_data(oem_name,stat_category,key,'14',values['total_14'],date=date_today)
    helper_mysql.put_raw_data(oem_name,stat_category,key,'30',values['total_30'],date=date_today)

    key='subscription_charged_msisdn_unique'

    helper_mysql.put_raw_data(oem_name,stat_category,key,'1',values['uv_1'],date=date_today)
    helper_mysql.put_raw_data(oem_name,stat_category,key,'7',values['uv_7'],date=date_today)
    helper_mysql.put_raw_data(oem_name,stat_category,key,'14',values['uv_14'],date=date_today)
    helper_mysql.put_raw_data(oem_name,stat_category,key,'30',values['uv_30'],date=date_today)




if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
