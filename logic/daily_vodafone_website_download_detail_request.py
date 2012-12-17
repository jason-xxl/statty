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
        
        print '-1 file size: '+file_size[file_name]
        return file_name
        #raise Exception()

    is_finished=response_size>file_size[file_name]

    if not is_finished:
        #print 'broken response: '+file_name+'('+str(response_size)+' / '+str(file_size[file_name])+'): '+line
        pass
    return file_name if is_finished else ''



def stat_website(my_date):

    global current_date
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=0)

    ##Fields: date time cs-method cs-uri-stem cs-uri-query c-ip cs-version cs(User-Agent) cs(Referer) sc-status sc-bytes time-taken 
    #2011-04-14 16:00:04 GET /download/ve.ipa - 41.206.130.9 HTTP/1.1 iTunes-iPhone/4.2.1+(3;+16GB) - 200 2416786 93250

    oem_name='Vodafone'
    stat_category='website'

    stat_plan=Stat_plan()


    stat_sql_client_download_by_file_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'downloader_ip':r' (\d+\.\d+\.\d+\.\d+) '}, \
                                   select_count_exist={'succeeded':is_download_finished,
                                                       'action':r'(.)'}, \
                                   select_average={'spent_time':r' (\d+)\s*$'}, \
                                   where={'client_download':r'(GET|POST) /download/'}, \
                                   group_by={'daily':get_current_date, \
                                             'by_file_name':get_file_name}, \
                                   db_name='raw_data_url_pattern')

    stat_plan.add_stat_sql(stat_sql_client_download_by_file_daily)

    stat_sql_client_download_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'downloader_ip':r' (\d+\.\d+\.\d+\.\d+) '}, \
                                   select_count_exist={'succeeded':is_download_finished,
                                                       'action':r'(.)'}, \
                                   select_average={'spent_time':r' (\d+)\s*$'}, \
                                   where={'client_download':r'(GET|POST) /download/'}, \
                                   group_by={'daily':get_current_date}, \
                                   db_name='raw_data_url_pattern')
    
    stat_plan.add_stat_sql(stat_sql_client_download_daily)

    stat_sql_client_download_by_file_succ_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'downloader_ip':r' (\d+\.\d+\.\d+\.\d+) '}, \
                                   select_average={'spent_time':r' (\d+)\s*$'}, \
                                   where={'client_download':r'(GET|POST) /download/',\
                                          'only_succeeded':is_download_finished}, \
                                   group_by={'daily':get_current_date, \
                                             'by_file_name':get_file_name}, \
                                   db_name='raw_data_url_pattern')
    
    stat_plan.add_stat_sql(stat_sql_client_download_by_file_succ_daily)



    stat_sql_client_download_succ_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'downloader_ip':r' (\d+\.\d+\.\d+\.\d+) '}, \
                                   select_count_exist={'succeeded':is_download_finished,
                                                       'action':r'(.)'}, \
                                   select_average={'spent_time':r' (\d+)\s*$'}, \
                                   where={'client_download':r'(GET|POST) /download/',\
                                          'only_succeeded':is_download_finished}, \
                                   group_by={'daily':get_current_date}, \
                                   db_name='raw_data_url_pattern')
    
    stat_plan.add_stat_sql(stat_sql_client_download_succ_daily)
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.52\W3SVC1602359321\ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.52\log_download\ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\logs_download_buzz\ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%y%m%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.53\log_synchronizer\log_download_vodafone\ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=0,date_format='%y%m%d')) #special server of GMT+2
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\10.254.212.172\log_download_vodafone\ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=0,date_format='%y%m%d')) #special server of GMT+2
    
   
    stat_plan.run()    
    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i+3600*config.timezone_offset_vodafone
        stat_website(my_date)
