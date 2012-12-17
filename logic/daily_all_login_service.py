import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_stc
from user_id_filter import user_id_filter_viva
from user_id_filter import user_id_filter_viva_bh
from user_id_filter import user_id_filter_umniah
import config

def get_peer_name(line):
    try:
        name=helper_regex.extract(line,r'peername=([^,]+),')
        return helper_regex.regex_replace('[^\w_\.@]','',name)
    except:
        print 'error log (peername): ',line
        return ''


def stat_login_service(my_date):


    oem_name='All'
    stat_category='login_service'

    stat_plan=Stat_plan()

    #Daily

    ##### All Begin #####

    # request
    # 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'device_type':r'deviceType=(\d+),'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
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
    

    ##### STC Begin #####

    # request
    # 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_stc':r'(@)shabik.com'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_stc':r'(@)shabik.com'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'device_type':r'deviceType=(\d+),'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_stc':r'(@)shabik.com'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'protocol_version':r'version=(\d+),'}))



    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:', \
                                          'only_stc':r'(@)shabik.com'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'result=([^,]+),'}))
        

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:', \
                                          'only_stc':r'(@)shabik.com'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_flag':r'flag=(\d+)'}))
        


    # result
    # 2010-06-04 20:00:59,875 [INFO] MoPeerLoginService - gooffy_go@morange.com login failure : wrong username


    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_result':r'(l)ogin failure :', \
                                          'only_stc':r'(@)shabik.com'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'login failure : (.*)'}))


    ##### STC End #####



    

    ##### Viva Begin #####

    # request
    # 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_viva':r'(@)viva(?!\.bh)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_viva':r'(@)viva(?!\.bh)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'device_type':r'deviceType=(\d+),'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_viva':r'(@)viva(?!\.bh)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'protocol_version':r'version=(\d+),'}))



    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:', \
                                          'only_viva':r'(@)viva(?!\.bh)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'result=([^,]+),'}))
        

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:', \
                                          'only_viva':r'(@)viva(?!\.bh)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_flag':r'flag=(\d+)'}))
        


    # result
    # 2010-06-04 20:00:59,875 [INFO] MoPeerLoginService - gooffy_go@morange.com login failure : wrong username


    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_result':r'(l)ogin failure :', \
                                          'only_viva':r'(@)viva(?!\.bh)'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'login failure : (.*)'}))


    ##### Viva End #####



    """

    ##### Viva_BH Begin #####

    # request
    # 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_viva_bh':r'(@)viva\.bh'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_viva_bh':r'(@)viva\.bh'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'device_type':r'deviceType=(\d+),'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_viva_bh':r'(@)viva\.bh'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'protocol_version':r'version=(\d+),'}))



    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:', \
                                          'only_viva_bh':r'(@)viva\.bh'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'result=([^,]+),'}))
        

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:', \
                                          'only_viva_bh':r'(@)viva\.bh'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_flag':r'flag=(\d+)'}))
        


    # result
    # 2010-06-04 20:00:59,875 [INFO] MoPeerLoginService - gooffy_go@morange.com login failure : wrong username


    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_result':r'(l)ogin failure :', \
                                          'only_viva_bh':r'(@)viva\.bh'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'login failure : (.*)'}))


    ##### Viva_BH End #####

    """


    
    """
    ##### Umniah Begin #####

    # request
    # 2010-06-04 20:00:30,296 [INFO] MoPeerLoginService - login request: peernnamePre=lx_meti_xl@yahoo.com,peername=lx_meti_xl@yahoo.com,pwd=ce4aba70dcd089ced8f3cbb360206955,version=1,deviceType=0,ip=/92.42.49.71

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_umniah':r'(@)umniah.jor'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_umniah':r'(@)umniah.jor'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'device_type':r'deviceType=(\d+),'}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':get_peer_name}, \
                                   where={'login_request':r' (-) login request:', \
                                          'only_umniah':r'(@)umniah.jor'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'protocol_version':r'version=(\d+),'}))



    # response
    # 2010-06-04 20:00:30,796 [INFO] MoPeerLoginService - fedo.91@morange.cc login result: result=0,peerId=12592462,flag=8
    

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:', \
                                          'only_umniah':r'(@)umniah.jor'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'result=([^,]+),'}))
        

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_response':r'(l)ogin result:', \
                                          'only_umniah':r'(@)umniah.jor'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_flag':r'flag=(\d+)'}))
        


    # result
    # 2010-06-04 20:00:59,875 [INFO] MoPeerLoginService - gooffy_go@morange.com login failure : wrong username


    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'peer_name':r' - (.*?) login'}, \
                                   where={'login_result':r'(l)ogin failure :', \
                                          'only_umniah':r'(@)umniah.jor'}, \
                                   group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                             'result_type':r'login failure : (.*)'}))


    ##### Umniah End #####



    """
    
    #stat_plan.add_log_source(r'\\192.168.0.103\logsLogin\morange.log.' \
    #                         +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') #stc
    stat_plan.add_log_source(r'\\192.168.0.79\logs_login\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') #stc
    stat_plan.add_log_source(r'\\192.168.0.100\logs_login_stc\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') #stc
    stat_plan.add_log_source(r'\\192.168.1.40\umniah_login_logs\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') # umniah
    stat_plan.add_log_source(r'\\192.168.1.41\umniah_login_logs\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') # umniah
    stat_plan.add_log_source(r'\\192.168.1.36\logs_login_svc_umniah\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') # umniah
    stat_plan.add_log_source(r'\\192.168.0.122\applications\moLoginSvr\logs\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') # viva
    stat_plan.add_log_source(r'\\192.168.0.107\logs_login\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') # viva
    stat_plan.add_log_source(r'\\192.168.0.185\logs_mologin_shabik_360\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.196\logs_mologin_shabik_360\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    """
    stat_plan.add_log_source(r'\\192.168.0.122\applications\moLoginSvr\logs\morange.log.2011-10-05-23')
    stat_plan.add_log_source(r'\\192.168.0.140\logs_login\morange.log.' \
                             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') # viva bh
    """
    

    
    stat_plan.run()    


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_login_service(my_date)
