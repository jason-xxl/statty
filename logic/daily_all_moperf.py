import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_stc
import config



def stat_moperf(my_date):


    # 2010-04-03 03:00:20,441 [ INFO]      processLoginPkt - Index3 IM1 login client aalove992009@hotmail.com monetid=10654750 type=MSN

    oem_name='All'
    stat_category='moperf'

    stat_plan=Stat_plan()


    ##### All Begin #####

    # INFO 17 May 07:44:32,078 - [eBase runThread] (monService.java:  63) - BReq    8350027    12740126354387    1    http://mobile.morange.com/mobile_chatroom.aspx    1274012635483    
    # INFO 17 May 07:44:32,078 - [eBase runThread] (monService.java:  75) - BRep    8350027    12740126354387    1274012649942    

    # 2010-05-18 01:02:03,360    BRep    12508665    127411545866627    1274115491954    202.134.8.13
    # 2010-05-18 01:02:03,375    BReq    11060617    127411571548012    1    http://mobileshabik.morange.com/mobile_editprofile.aspx?action=nick&nick=%e2%99%a5%d8%ba%d9%86%d9%80Hamad%d9%80%d9%88%d8%ac%db%92%e2%99%a5    1274115715591    212.118.143.146

    # 2010-05-18 16:00:00,094 [ INFO] MoPerfmonService.ProcessBrowserResponse -  BRep    4285126    1274169551661232    1274169558734    203.78.122.240
    # 2010-05-18 16:00:00,219 [ INFO] MoPerfmonService.ProcessBrowserRequest -  BReq    13860825    116777997253210    1    http://mobile.morange.com/mobile_sone.aspx?action=sone_homepage&userid=13879390&contactId=0    1167779972533    209.8.246.12

    '''
    stat_sql_client_page_request_time_hourly=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_span_average={'page_response_time':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$', \
                                                                              'sec_group_key_on_value':helper_regex.log10_moperf_span_level}}, \
                                   where={'client_side':r'(\-\s*BRe(?:q|p)\t)'}, \
                                   group_by={'hourly':'^\s*(?:INFO\s*)?(\d+\s+[A-Za-z]+\s+\d{2}|[0-9\-]+\s+\d{2})'})

    stat_plan.add_stat_sql(stat_sql_client_page_request_time_hourly)
    '''
    
    stat_sql_client_page_request_time_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_span_average={'page_response_time':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$', \
                                                                              'sec_group_key_on_value':helper_regex.log10_moperf_span_level}}, \
                                   where={'client_side':r'(\-\s*BRe(?:q|p)\t)'}, \
                                   group_by={'daily':'^\s*(?:INFO\s*)?(\d+\s+[A-Za-z]+|[0-9\-]+)'})
    
    stat_plan.add_stat_sql(stat_sql_client_page_request_time_daily)

    '''
    stat_sql_client_page_request_time_by_url_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_span_average={'page_response_time_by_url':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$', \
                                                                              'sec_group_key_on_value':helper_regex.available_moperf_span_level, \
                                                                              'sec_group_key_on_log':helper_regex.get_simplified_url_unique_key}}, \
                                   where={'client_side':r'(\-\s*BRe(?:q|p)\t)'}, \
                                   group_by={'daily':'^\s*(?:INFO\s*)?(\d+\s+[A-Za-z]+|[0-9\-]+)'})

    stat_plan.add_stat_sql(stat_sql_client_page_request_time_by_url_daily)
    '''

    ##### All End #####


    ##### STC Begin #####

    # INFO 17 May 07:44:32,078 - [eBase runThread] (monService.java:  63) - BReq    8350027    12740126354387    1    http://mobile.morange.com/mobile_chatroom.aspx    1274012635483    
    # INFO 17 May 07:44:32,078 - [eBase runThread] (monService.java:  75) - BRep    8350027    12740126354387    1274012649942    

    # 2010-05-18 01:02:03,360    BRep    12508665    127411545866627    1274115491954    202.134.8.13
    # 2010-05-18 01:02:03,375    BReq    11060617    127411571548012    1    http://mobileshabik.morange.com/mobile_editprofile.aspx?action=nick&nick=%e2%99%a5%d8%ba%d9%86%d9%80Hamad%d9%80%d9%88%d8%ac%db%92%e2%99%a5    1274115715591    212.118.143.146

    '''
    stat_sql_client_page_request_time_hourly_stc=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_span_average={'page_response_time':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$', \
                                                                              'sec_group_key_on_value':helper_regex.log10_moperf_span_level}}, \
                                   where={'client_side':r'(\-\s*BRe(?:q|p)\t)','only_stc':user_id_filter_stc.is_valid_user}, \
                                   group_by={'hourly':'^\s*(?:INFO\s*)?(\d+\s+[A-Za-z]+\s+\d{2}|[0-9\-]+\s+\d{2})'})
    stat_plan.add_stat_sql(stat_sql_client_page_request_time_hourly_stc)
    '''
    
    stat_sql_client_page_request_time_daily_stc=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_span_average={'page_response_time':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$', \
                                                                              'sec_group_key_on_value':helper_regex.log10_moperf_span_level}}, \
                                   where={'client_side':r'(\-\s*BRe(?:q|p)\t)','only_stc':user_id_filter_stc.is_valid_user}, \
                                   group_by={'daily':'^\s*(?:INFO\s*)?(\d+\s+[A-Za-z]+|[0-9\-]+)'})


    stat_plan.add_stat_sql(stat_sql_client_page_request_time_daily_stc)

    '''
    stat_sql_client_page_request_time_by_url_daily_stc=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_span_average={'page_response_time_by_url':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$', \
                                                                              'sec_group_key_on_value':helper_regex.available_moperf_span_level, \
                                                                              'sec_group_key_on_log':helper_regex.get_simplified_url_unique_key}}, \
                                   where={'client_side':r'(\-\s*BRe(?:q|p)\t)','only_stc':user_id_filter_stc.is_valid_user}, \
                                   group_by={'daily':'^\s*(?:INFO\s*)?(\d+\s+[A-Za-z]+|[0-9\-]+)'})

    stat_plan.add_stat_sql(stat_sql_client_page_request_time_by_url_daily_stc)
    '''

    ##### STC End #####




    ##### Viva Begin #####

    # INFO 17 May 07:44:32,078 - [eBase runThread] (monService.java:  63) - BReq    8350027    12740126354387    1    http://mobile.morange.com/mobile_chatroom.aspx    1274012635483    
    # INFO 17 May 07:44:32,078 - [eBase runThread] (monService.java:  75) - BRep    8350027    12740126354387    1274012649942    

    # 2010-05-18 01:02:03,360    BRep    12508665    127411545866627    1274115491954    202.134.8.13
    # 2010-05-18 01:02:03,375    BReq    11060617    127411571548012    1    http://mobileshabik.morange.com/mobile_editprofile.aspx?action=nick&nick=%e2%99%a5%d8%ba%d9%86%d9%80Hamad%d9%80%d9%88%d8%ac%db%92%e2%99%a5    1274115715591    212.118.143.146

    '''
    stat_sql_client_page_request_time_hourly_viva=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_span_average={'page_response_time':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$', \
                                                                              'sec_group_key_on_value':helper_regex.log10_moperf_span_level}}, \
                                   where={'client_side':r'(\-\s*BRe(?:q|p)\t)','only_viva':user_id_filter_stc.is_valid_user}, \
                                   group_by={'hourly':'^\s*(?:INFO\s*)?(\d+\s+[A-Za-z]+\s+\d{2}|[0-9\-]+\s+\d{2})'})
    
    stat_plan.add_stat_sql(stat_sql_client_page_request_time_hourly_viva)
    '''
    
    stat_sql_client_page_request_time_daily_viva=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_span_average={'page_response_time':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$', \
                                                                              'sec_group_key_on_value':helper_regex.log10_moperf_span_level}}, \
                                   where={'client_side':r'(\-\s*BRe(?:q|p)\t)','only_viva':user_id_filter_stc.is_valid_user}, \
                                   group_by={'daily':'^\s*(?:INFO\s*)?(\d+\s+[A-Za-z]+|[0-9\-]+)'})

    stat_plan.add_stat_sql(stat_sql_client_page_request_time_daily_viva)

    '''
    stat_sql_client_page_request_time_by_url_daily_viva=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_span_average={'page_response_time_by_url':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$', \
                                                                              'sec_group_key_on_value':helper_regex.available_moperf_span_level, \
                                                                              'sec_group_key_on_log':helper_regex.get_simplified_url_unique_key}}, \
                                   where={'client_side':r'(\-\s*BRe(?:q|p)\t)','only_viva':user_id_filter_stc.is_valid_user}, \
                                   group_by={'daily':'^\s*(?:INFO\s*)?(\d+\s+[A-Za-z]+|[0-9\-]+)'})

    stat_plan.add_stat_sql(stat_sql_client_page_request_time_by_url_daily_viva)
    '''

    ##### Viva End #####



    log_files_1=r'\\192.168.0.79\moperf_logs\moperfmon.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'*'
    #log_files_1=r'\\192.168.0.79\moperf_logs\moperfmon.log.2010-05-22-11'
    
    stat_plan.add_log_source(log_files_1)

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_moperf(my_date)


