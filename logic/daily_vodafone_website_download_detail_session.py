import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_file
import helper_mysql
import helper_regex
import config

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date

file_size={}
download_url_prefix='http://buzz.com.eg/download/'

def get_file_name(line):
    return helper_regex.extract(line,r'/download/([^\.]+\.\w+)').lower()

def is_download_finished(line):
    global current_date
    response_size=0
    try:
        response_size=int(helper_regex.extract(line,r'\d+ (\d+) \d+\s*$'))
    except:
        #print 'problemetic log (response size): '+line
        pass

    response_status=''
    try:
        response_status=int(helper_regex.extract(line,r'(\d+) \d+ \d+\s*$'))
    except:
        #print 'problemetic log (response status): '+line
        pass

    file_name=helper_regex.extract(line,r'/download/([^\.]+\.\w+)').lower()

    if not response_size or not file_name or not response_status:
        if not file_name:
            #print 'problemetic log (file name): '+line
            pass
        if not response_status:
            #print 'problemetic log (response status): '+line
            pass
        return ''

    if response_status==304 or response_status==206:
        return file_name

    if not file_size.has_key(file_name):
        size=helper_mysql.get_raw_data(oem_name='Vodafone',category='website',key='client_file_size',sub_key=file_name, \
                                            default_value=-1,table_name='raw_data_url_pattern',date=current_date)
        if size>0:
            file_size[file_name]=size
            #print 'file size from db of '+download_url_prefix+file_name+' is: '+str(file_size[file_name])
        else:
            file_size[file_name]=helper_file.get_web_file_size(download_url_prefix+file_name)
            #print 'file current size of '+download_url_prefix+file_name+' is: '+str(file_size[file_name])
            
            helper_mysql.put_raw_data(oem_name='Vodafone',category='website',key='client_file_size',sub_key=file_name, \
                                      value=file_size[file_name],table_name='raw_data_url_pattern',date=current_date)
    if file_size[file_name]==-1:
        #print '-1 file size: '+file_size[file_name]
        raise Exception()

    is_finished=response_size>file_size[file_name]

    if not is_finished:
        #print 'broken response: '+file_name+'('+str(response_size)+' / '+str(file_size[file_name])+'): '+line
        pass
    return file_name if is_finished else ''


error_request=[]

def is_download_done(line):
    response_code=helper_regex.extract(line,r'(\d{3}) \d+ \d+\s*\n')
    if not response_code:
        print 'no response_code:'+line
        pass
    response_code_type=response_code[0:1]
    if response_code_type=='2' or response_code_type=='3':
        return True
    #error_request.append('['+response_code+']'+line)
    return False

log_by_session={}

def process_log(line='',exist='',group_key=''):
    session_id=helper_regex.extract(line,get_downloader_unique_id)
    status_code=helper_regex.extract(line,r'(\d{3}) \d+ \d+\s*\n')
    if not log_by_session.has_key(session_id):
        log_by_session[session_id]=[]
    log_by_session[session_id].append('['+status_code+']'+line)


def get_downloader_unique_id(line):
    """
    if line.find('192.168.1.42')>-1:
        return ''
    return helper_regex.extract(line,r' - (\d+\.\d+\.\d+\.\d+ \w+\/\d\.\d [^\s]+) ')
    """
    return helper_regex.extract(line,r'sess=([\w\-]+)')

def stat_website(my_date):

    global current_date
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_vodafone)

    ##Fields: date time cs-method cs-uri-stem cs-uri-query c-ip cs-version cs(User-Agent) cs(Referer) sc-status sc-bytes time-taken 
    #2011-04-27 04:33:40 GET /download/ve-3.sisx - 41.206.155.115 HTTP/1.1 Mozilla/5.0+(SymbianOS/9.2;+U;+Series60/3.1+Nokia6120ci/7.02;+Profile/MIDP-2.0+Configuration/CLDC-1.1+)+AppleWebKit/413+(KHTML,+like+Gecko)+Safari/413 ASP.NET_SessionId=tx2hau45ifltgx2uqhvuzh45 http://buzz.com.eg/ 304 212 265

    #(1.53)2011-08-08 06:37:29 GET /download/ve.jar - 41.69.255.137 HTTP/1.1 Nokia2690/2.0+(10.10)+Profile/MIDP-2.1+Configuration/CLDC-1.1 - - 304 212 0
    #(1.52)2011-08-08 07:00:01 GET /survey.aspx - 192.168.1.42 HTTP/1.0 NokiaX3-00/5.0+(11.00)+Profile/MIDP-2.1+Configuration/CLDC-1.1+Mozilla/5.0+AppleWebKit/420++(KHTML,+like+Gecko)+Safari/420+ - http://buzz.com.eg/ 200 1972 0

    #2011-08-18 09:53:00 GET /download/ve.jad - 217.212.230.86 HTTP/1.1 Opera/9.80+(J2ME/MIDP;+Opera+Mini/4.1.13907/25.729;+U;+ar)+Presto/2.5.25+Version/10.54 sess=6e53d0e6-70e2-42b3-a637-b7f719b60a90 - 200 1198 93


    oem_name='Vodafone'
    stat_category='website'

    stat_plan=Stat_plan(encode_exception_treatment='replace')

    stat_sql_client_download_by_file_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'downloader_ip':r' (\d+\.\d+\.\d+\.\d+) ', \
                                                          'downloader_session':get_downloader_unique_id}, \
                                   select_count_exist={'succeeded':is_download_done,
                                                       'action':r'(.)', \
                                                       'has/_session':get_downloader_unique_id}, \
                                   select_average={'spent_time':r' (\d+)\s*$'}, \
                                   where={'client_download':r'(GET|POST) /download/', \
                                          'has_session':get_downloader_unique_id}, \
                                   group_by={'daily':get_current_date, \
                                             'by_file_name':get_file_name}, \
                                   db_name='raw_data_url_pattern')

    stat_plan.add_stat_sql(stat_sql_client_download_by_file_daily)

    stat_sql_client_download_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'downloader_ip':r' (\d+\.\d+\.\d+\.\d+) ', \
                                                          'downloader_session':get_downloader_unique_id}, \
                                   select_count_exist={'succeeded':is_download_done,
                                                       'action':r'(.)'}, \
                                   select_average={'spent_time':r' (\d+)\s*$'}, \
                                   where={'client_download':r'(GET|POST) /download/', \
                                          'has_session':get_downloader_unique_id}, \
                                   group_by={'daily':get_current_date}, \
                                   db_name='raw_data_url_pattern')
    
    stat_plan.add_stat_sql(stat_sql_client_download_daily)

    stat_sql_client_download_by_file_succ_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'downloader_ip':r' (\d+\.\d+\.\d+\.\d+) ', \
                                                          'downloader_session':get_downloader_unique_id}, \
                                   select_average={'spent_time':r' (\d+)\s*$'}, \
                                   where={'client_download':r'(GET|POST) /download/',\
                                          'only_succeeded':is_download_done, \
                                          'has_session':get_downloader_unique_id}, \
                                   group_by={'daily':get_current_date, \
                                             'by_file_name':get_file_name}, \
                                   db_name='raw_data_url_pattern')
    
    stat_plan.add_stat_sql(stat_sql_client_download_by_file_succ_daily)



    stat_sql_client_download_succ_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'downloader_ip':r' (\d+\.\d+\.\d+\.\d+) ', \
                                                          'downloader_session':get_downloader_unique_id}, \
                                   select_count_exist={'succeeded':is_download_done,
                                                       'action':r'(.)'}, \
                                   select_average={'spent_time':r' (\d+)\s*$'}, \
                                   where={'client_download':r'(GET|POST) /download/',\
                                          'only_succeeded':is_download_done, \
                                          'has_session':get_downloader_unique_id}, \
                                   group_by={'daily':get_current_date}, \
                                   db_name='raw_data_url_pattern')
    
    stat_plan.add_stat_sql(stat_sql_client_download_succ_daily)
    

    #collect device-client matching rules
    #2011-04-18 10:00:10 GET /download/ve.jar - 41.206.130.5 HTTP/1.1 Nokia7610/2.0+(7.0642.0)+SymbianOS/7.0s+Series60/2.1+Profile/MIDP-2.0+Configuration/CLDC-1.0 - - 304 213 296
    """
    db_name='raw_data_user_info_periodical'

    stat_sql_client_native_info_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_text_value={'provided_client_link':r' GET ([^\s]*?) '}, \
                                   select_count_exist={'provided_client_count':r' GET ([^\s]*?) '}, \
                                   select_average={'spent_time':r' (\d+)\s*$'}, \
                                   where={'client_download':r'(GET|POST) /download/.*?(?:\.jar|\.jad|\sisx|\.sis|\.cod|\.apk|\.ipa)'}, \
                                   group_by={'daily':get_current_date, \
                                             'by_native_useragent':r'HTTP[^\s]+ ([^\s]+)'},
                                   db_name=db_name)
    
    stat_plan.add_stat_sql(stat_sql_client_native_info_daily)

    

    stat_sql_process_log_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   process_exist={'process_log':{'pattern':r'(.)','process':process_log}},
                                   where={'client_download':r'(GET|POST) /download/',\
                                          'has_session':get_downloader_unique_id})
    
    stat_plan.add_stat_sql(stat_sql_process_log_daily)
    """

#    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
#                                        my_date,r'\\192.168.1.52\W3SVC1602359321\ex%(date)s%(hour)s.log', \
#                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))
#    
#    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
#                                        my_date,r'\\192.168.1.52\log_download\ex%(date)s%(hour)s.log', \
#                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))
#    
#    
#    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
#                                        my_date,r'\\192.168.1.53\log_synchronizer\log_download_vodafone\ex%(date)s%(hour)s.log', \
#    
#                                        timezone_offset_to_sg=0,date_format='%y%m%d')) #special server of GMT+2
    
    # OLD
#    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
#                                        my_date,r'\\192.168.1.71\logs_download_buzz\ex%(date)s%(hour)s.log', \
#                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))
    # new
#    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
#                                        my_date,r'\\192.168.1.71\E_iis_log\download.buzz.com.eg\W3SVC1602359321\ex%(date)s%(hour)s.log', \
#                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'z:\download.buzz.com.eg\W3SVC1602359321\ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'http://i.buzz.com.eg/log_vodafone_download_internal_server/ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=0,date_format='%y%m%d')) #special server of GMT+2
    
    stat_plan.run()    

#    print '===Error Request==='
#    for l in error_request:
#        print l.replace('\n','')
#
#   
#    print helper_regex.translate_iis_website_hourly_log_path(
#                                        my_date,r'\\192.168.1.53\log_synchronizer\log_download_vodafone\ex%(date)s%(hour)s.log', \
#                                        timezone_offset_to_sg=0,date_format='%y%m%d')


if __name__=='__main__':
    #print get_file_name(r'2011-08-08 06:37:29 GET /download/ve.jar - 41.69.255.137 HTTP/1.1 Nokia2690/2.0+(10.10)+Profile/MIDP-2.1+Configuration/CLDC-1.1 - - 304 212 0')
    #exit()

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_website(my_date)
