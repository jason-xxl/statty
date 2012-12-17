import helper_sql_server
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_file
import helper_regex
import helper_mysql
import config

helper_mysql.quick_insert=True


def stat_website(my_date):

    oem_name='Vodafone'
    stat_category='project_sms_broadcast'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 06:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 06:00:00')
    date_today=start_time.replace(' 06:00:00','')

    #first batch

    sub_key='1'


    
    key='total_sent_msisdn'
    
    db='shabik_mt'
    sql="select count(*) from [shabik_mt].dbo.buzz_broadcast_round_one_log with(nolock)" 
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_87,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key,value)

    
    
    key='total_sent_logined_msisdn'
    
    db='shabik_mt'
    sql=r'''
    select count(*)
    from DB88.[mozone_user].[dbo].[Profile] with(nolock)
    where user_name in (
        select rtrim(replace(msisdn,'+20',''))+'@voda_egypt' from [shabik_mt].dbo.buzz_broadcast_round_one_log with(nolock)
    )
    and lastLogin >='2011-11-17 00:00:00'
    
    ''' 
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_vodafone_87,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key,value)



    #2011-08-13 16:02:12 GET /360/stc.jar - 84.235.75.80 HTTP/1.1 Nokia7230/5.0+(06.90)+Profile/MIDP-2.1+Configuration/CLDC-1.1 - 200 517172 34003

    #helper_mysql.clear_raw_data_space(oem_name=oem_name,category=stat_category,key='filtered_%',sub_key=None,date=current_date,table_name=db_name)

    #collect device-client matching rules
    #2011-04-18 10:00:10 GET /download/ve.jar - 41.206.130.5 HTTP/1.1 Nokia7610/2.0+(7.0642.0)+SymbianOS/7.0s+Series60/2.1+Profile/MIDP-2.0+Configuration/CLDC-1.0 - - 304 213 296
    
    #request by downloader
  	#2011-09-07 07:49:18 GET /download-gateway.aspx - 115.66.172.127 HTTP/1.1 s605th/shabik360/353391046226409 - - 200 328 218
  	#2011-09-07 07:49:18 GET /download-gateway.aspx - 115.66.172.127 HTTP/1.1 s603rd/shabik360/353391046226409 - - 200 328 218


    stat_plan=Stat_plan()
    
    stat_sql_download_user_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_count_distinct_collection={'msisdn':r'Buzz.com.eg\/\?no=([0-9a-zA-Z\-_]+)[ignorecase]'},
                                        where={'filtered':lambda line:True}, \
                                        group_by={'by_batch':lambda line:'1'})
    
    stat_plan.add_stat_sql(stat_sql_download_user_daily)

    for i in range(0,100):

        stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                            my_date+3600*24*i,r'\\192.168.1.52\W3SVC1602359321\ex%(date)s%(hour)s.log', \
                                            timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))
        
        stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                            my_date+3600*24*i,r'\\192.168.1.52\log_download\ex%(date)s%(hour)s.log', \
                                            timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))
        
        stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                            my_date+3600*24*i,r'\\192.168.1.53\log_synchronizer\log_download_vodafone\ex%(date)s%(hour)s.log', \
                                            timezone_offset_to_sg=0,date_format='%y%m%d')) #special server of GMT+2

    stat_plan.run()    



if __name__=='__main__':

    time_tuple = (2011, 11, 17, 0, 0, 0, 0, 0, 0)
    my_date = time.mktime(time_tuple)
    stat_website(my_date)
