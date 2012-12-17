import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_viva_bh
import config


def stat_sub(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Viva_BH'
    stat_category='sub'
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    

    # sub users total
    
    key='net_sub_user'
    
    db='bahrain_mt'

    sql="""

    SELECT replace(rtrim([msisdn]),'+',''),1
    from [DB86].[bahrain_mt].[dbo].[logs] with(nolock)
    where [CreatedOn]>='%s' and [CreatedOn]<'%s'

    and [msisdn] not in (
        SELECT [msisdn]
        from [DB86].[bahrain_mt].[dbo].[logs] with(nolock)
        where [CreatedOn]<'%s'
        group by [msisdn]
        having max([id]*100+[action])-max([id])*100 = 10
    )
    
    group by [msisdn]
    having max([id]*100+[action])-max([id])*100 = 10
    
    """ % (start_time,end_time,start_time)

    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print values

    for msisdn in values.keys():
        helper_mysql.put_raw_data(oem_name,stat_category,key,msisdn+'_'+date_today,1,table_name='raw_data_msisdn')
        pass



    # unique sub user
    
    key='net_unsub_user'

    db='bahrain_mt'
    
    sql="""

    SELECT replace(rtrim([msisdn]),'+',''),1
    from [DB86].[bahrain_mt].[dbo].[logs] with(nolock)
    where [CreatedOn]>='%s' and [CreatedOn]<'%s'

    and [msisdn] not in (
        SELECT [msisdn]
        from [DB86].[bahrain_mt].[dbo].[logs] with(nolock)
        where [CreatedOn]<'%s'
        group by [msisdn]
        having max([id]*100+[action])-max([id])*100 = 11
    )
    
    group by [msisdn]
    having max([id]*100+[action])-max([id])*100 = 11
    
    """ % (start_time,end_time,start_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print values

    for msisdn in values.keys():
        helper_mysql.put_raw_data(oem_name,stat_category,key,msisdn+'_'+date_today,1,table_name='raw_data_msisdn')
        pass
     


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
