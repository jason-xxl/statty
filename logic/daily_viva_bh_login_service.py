import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

# to be continue

def get_msisdn_for_login_log(line):
    #33786385@viva.bh
    return "973"+helper_regex.extract(line,r"(\d+)@viva.bh")

def stat_login_service(my_date):



    oem_name='Viva_BH'
    stat_category='login_service'

    stat_plan=Stat_plan()

    #Daily

    ##### All Begin #####

    # request
    # 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'peername=([^,]+),'}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'peername=([^,]+),'}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'device_type':r'deviceType=(\d+),'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'peername=([^,]+),'}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'protocol_version':r'version=(\d+),'}))


    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'result=([^,]+),'}))
        

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_flag':r'flag=(\d+)'}))
        


    # result
    # 2010-06-04 20:00:59,875 [INFO] MoPeerLoginService - gooffy_go@morange.com login failure : wrong username


    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_result':r'(l)ogin failure :'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'login failure : (.*)'}))


    ##### All End #####

    
    # Hourly


    ##### All Begin #####

    # request
    # 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'peername=([^,]+),'}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'hourly':r'(\d{4}\-\d{2}\-\d{2} \d{2})'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'peername=([^,]+),'}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'hourly':r'(\d{4}\-\d{2}\-\d{2} \d{2})', \
                                             'device_type':r'deviceType=(\d+),'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r'peername=([^,]+),'}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'hourly':r'(\d{4}\-\d{2}\-\d{2} \d{2})', \
                                             'protocol_version':r'version=(\d+),'}))


    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:'}, \
                                   group_by={'hourly':r'(\d{4}\-\d{2}\-\d{2} \d{2})', \
                                             'result_type':r'result=([^,]+),'}))
        

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:'}, \
                                   group_by={'hourly':r'(\d{4}\-\d{2}\-\d{2} \d{2})', \
                                             'result_flag':r'flag=(\d+)'}))
        


    # result
    # 2010-06-04 20:00:59,875 [INFO] MoPeerLoginService - gooffy_go@morange.com login failure : wrong username


    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_result':r'(l)ogin failure :'}, \
                                   group_by={'hourly':r'(\d{4}\-\d{2}\-\d{2} \d{2})', \
                                             'result_type':r'login failure : (.*)'}))


    ##### All End #####
    



    """
    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_int_value={'login_time':r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"}, \
                                   where={'logined_log':r'(@)viva\.bh login result: result=0'}, \
                                   group_by={'by_msisdn':get_msisdn_for_login_log}, \
                                   db_name='raw_data_login_log_mobile'))

    """


    stat_plan.add_log_source(r'\\192.168.0.140\logs_login\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') # viva bh
    
    stat_plan.run()    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_login_service(my_date)
