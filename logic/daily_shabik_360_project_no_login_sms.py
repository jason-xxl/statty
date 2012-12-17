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
import common_shabik_360

helper_mysql.quick_insert=True


def stat_no_login_sms(my_date):
    
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    get_current_date=lambda line:current_date

    #2011-08-13 16:02:12 GET /360/stc.jar - 84.235.75.80 HTTP/1.1 Nokia7230/5.0+(06.90)+Profile/MIDP-2.1+Configuration/CLDC-1.1 - 200 517172 34003

    oem_name='Shabik_360'
    stat_category='project_no_login_sms'
    
    db_name='raw_data_shabik_360'


    # generated sms

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')

    
    db='stc_integral'
    sql=r'''

    select count(*) as [total],count(distinct msisdn) as [unique]
    from shabik_mt.dbo.sub_no_login_sms_alert_log with(nolock)
    where last_sms_alert>='%s' and last_sms_alert<'%s'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_row(config.conn_stc_billing,sql)
    
    key='daily_generated_sms_msisdn_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,current_date,values['total'],table_name=db_name)
    
    key='daily_generated_sms_msisdn_unique_average'
    helper_mysql.put_raw_data(oem_name,stat_category,key,current_date,1.0*values['total']/values['unique'] if values['total'] else 0,table_name=db_name)


    # opened download link

    stat_plan=Stat_plan()
    
    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_count_distinct_collection={'msisdn':r'no=(\d+)'},\
                                        where={'from_no_login_sms':'(&p=m)'}, \
                                        group_by={'daily':get_current_date},
                                        db_name=db_name)

    stat_plan.add_stat_sql(stat_sql)

    stat_plan.add_log_source(r'\\192.168.0.175\w3svc581074064\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'.log')

    stat_plan.run()    

if __name__=='__main__':

    #print time.time()-3600*24*1
    #exit()
    
    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_no_login_sms(my_date)
