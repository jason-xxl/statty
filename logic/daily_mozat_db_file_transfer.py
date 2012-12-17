import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_file_transfer(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Mozat'
    stat_category='file_transfer'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    # transfered file
    
    db=''
    sql=r"""
    
    select 

    count(distinct fuid) as file_total
    ,sum(downloadedSize) as download_size_total
    ,sum(filesize) as file_size_total
    ,sum(0+started) as started_total
    ,sum(0+finished) as finished_total

    from FileTransfer.fileEntry

    where 
    `time`>='%s' and `time`<'%s'
    
    """ % (start_time,end_time)

    print 'Mysql Server:'+sql
    values=helper_mysql.fetch_row(sql,config.conn_mozat_file_transfer)
    print values

    key='download_size_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key])

    key='file_size_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key])

    key='started_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key])

    key='finished_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key])


    # transfered file type

    """
    db=''
    key='transfered_file_by_type_total'
    sql=r'''
    
    select 

    substr([fuid], locate('.', fuid)) as [file_type]
    ,count(distinct [fuid]) as [file_by_type_total]

    from [MYSQL136]...[fileEntry]

    where [time]>='%s' and [time]<'%s'

    group by [file_type]
    
    ''' % (start_time,end_time)

    print 'Mysql Server:'+sql
    values=helper_mysql.fetch_dict(sql,config.conn_helper_db)
    print values

    for file_type in values:
        helper_mysql.put_raw_data(oem_name,stat_category,key,file_type+'_'+date_today,values[file_type])

    """

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): # this stat can only run for today, not any day before
        my_date=time.time()-3600*24*i
        stat_file_transfer(my_date)
