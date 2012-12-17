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

def is_target_files(line):
    if line.find('Python-urllib')>-1:
        return ''
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if 'download-finished.html' in file_name or 'download-gateway.aspx' in file_name:
        return file_name
    if 'downloader' in file_name:
        return file_name
    imei=helper_regex.extract(line,r'shabik360\/(\d{10,})')
    if imei:
        return ''
    if '/360/' in file_name or '/comp-test/' in file_name or '360.aspx' in file_name:
        return file_name
    return ''

def is_download_finished (line):
    
    download_url_prefix='http://shabik.net.sa'

    try:
        response_size=int(helper_regex.extract(line,r'\d+ (\d+) \d+\s*$'))
        response_status=int(helper_regex.extract(line,r'(\d+) \d+ \d+\s*$'))
        file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    except:
        print 'problemetic log (response size): '+line
        return 'Wrong-Format'

    real_size=helper_file.get_cached_http_response_size(download_url_prefix+file_name)

    if response_status==206 and not helper_regex.extract(line,r'rand=(\d+)'):
        return '206-Unknown-Status'

    if response_status>=200 and response_status<300:
        if response_size<real_size*0.9:
            return 'Interupted'
        else:
            return 'Completed'
    
    if response_status>=300 and response_status<400:
        return 'Redirected' 
    
    if response_status>=400 and response_status<500:
        return 'Client-Side-Error' 
    
    if response_status>=500:
        return 'Server-Side-Error' 
        
    return 'Strange-Status'

def get_file_name(line):

  	#2011-09-07 07:49:18 GET /download-gateway.aspx - 115.66.172.127 HTTP/1.1 s605th/shabik360/353391046226409 - - 200 328 218
  	#2011-09-07 07:49:18 GET /download-gateway.aspx - 115.66.172.127 HTTP/1.1 s603rd/shabik360/353391046226409 - - 200 328 218

    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    imei=helper_regex.extract(line,r'shabik360\/(\d{10,})')
    symbian_version=helper_regex.extract(line,r's60(\d+)')
    if imei:
        return file_name+' (from downloader '+symbian_version+')'
    return file_name


stat_plan_detailed=None
partial_request_temp={} # definition: (file_path,random_code)=>(accumulated_file_size,last_log)

def process_line_reform_206_request(line='',exist='',group_key=''):

    global stat_plan_detailed,partial_request_temp
    #print '===='

    try:
        response_status=int(helper_regex.extract(line,r'(\d+) \d+ \d+\s*$'))
    except:
        print 'response_status error: ',line
        stat_plan_detailed.process_line(line)

        return 

    if response_status!=206:
        stat_plan_detailed.process_line(line)
        return
    
    try:
        file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
        random_code=int(helper_regex.extract(line,r'rand=(\d+)'))
        response_size=int(helper_regex.extract(line,r'\d+ (\d+) \d+\s*$'))
        response_time=int(helper_regex.extract(line,r'\d+ \d+ (\d+)\s*$'))
        #print file_name,random_code,response_status,response_size,response_time
    except:
        print 'problemetic log: '+line
        stat_plan_detailed.process_line(line)
        return
    
    if not partial_request_temp.has_key((file_name,random_code)):
        partial_request_temp[(file_name,random_code)]=[0,0,'']

    partial_request_temp[(file_name,random_code)][0]+=response_size
    partial_request_temp[(file_name,random_code)][1]+=response_time
    partial_request_temp[(file_name,random_code)][2]=line

    #partial_request_temp[(file_name,random_code)][2].append(line)
    #print (file_name,random_code)
    #print partial_request_temp[(file_name,random_code)]


def get_phone_os_type_from_native_useragent(line):
    user_agent=helper_regex.extract(line,r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?:HTTP[^\s]+ )?([^\s]+)').lower()
    #print user_agent
    os_type=helper_regex.get_phone_os_type_from_raw_native_useragent(user_agent)
    if os_type=='Unidentified':
        print 'unrecognized ua: ',user_agent
    return os_type

def get_stage_of_special_client_file(line):
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if '6.cod' in file_name or '7.cod' in file_name:
        return 'bb-end-file'
    if '.jad' in file_name and 'bb' in file_name:
        return 'bb-start-file'
    if '.jar' in file_name:
        return 'jme-end-file'
    if '.jad' in file_name:
        return 'jme-start-file'
    return 'n/a'

"""
def get_stage_of_client_file(line):
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if '.jad' in file_name or 'download-gateway.aspx' in file_name or '.jar' in file_name or '.sisx' in file_name or '.apk' in file_name:
        return 'client-start-file'
    if '.jar' in file_name or 'download-finished.html' in file_name or '6.cod' in file_name or '7.cod' in file_name or '.sisx' in file_name or '.apk' in file_name:
        return 'client-end-file'
    return 'n/a'
"""

def is_start_stage_of_client_file(line):
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if '.jad' in file_name or 'download-gateway.aspx' in file_name or '.jar' in file_name or '.sisx' in file_name or '.apk' in file_name:
        return 'client-start-file'
    return 'n/a'

def is_end_stage_of_client_file(line):
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if '.jar' in file_name or 'download-finished.html' in file_name or '6.cod' in file_name or '7.cod' in file_name or '.sisx' in file_name or '.apk' in file_name:
        return 'client-end-file'
    return 'n/a'

def is_download_request(line):
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if 'download-finished.html' in file_name or 'download-gateway.aspx' in file_name \
            or '.jad' in file_name or '.jar' in file_name or '.sisx' in file_name \
            or '.apk' in file_name or '.cod' in file_name:
        return 'Y'
    return 'N'
        

def get_client_type_from_file_name(line):
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if file_name.find('.cod')>-1 or file_name.find('bb')>-1:
        return 'BlackBerry'
    if file_name.find('.jad')>-1 or file_name.find('.jar')>-1:
        return 'J2ME'
    if file_name.find('.sisx')>-1 or file_name.find('.sis')>-1:
        if 'downloader' in file_name:
            if helper_regex.extract(file_name,r'[^\d](3)[^\d]'):
                return 'Symbian-Downloader-3'
            else:
                return 'Symbian-Downloader-5'
        else:
            if helper_regex.extract(file_name,r'[^\d](3)[^\d]'):
                return 'Symbian-3'
            else:
                return 'Symbian-5'
    if file_name.find('.apk')>-1:
        return 'Android'
    if file_name.find('.ipa')>-1:
        return 'iOS'
    return 'Non-Client-Files'


def stat_website(my_date):

    global stat_plan_detailed,partial_request_temp

    stat_plan_detailed=None
    partial_request_temp={}

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    get_current_date=lambda line:current_date

    #2011-08-13 16:02:12 GET /360/stc.jar - 84.235.75.80 HTTP/1.1 Nokia7230/5.0+(06.90)+Profile/MIDP-2.1+Configuration/CLDC-1.1 - 200 517172 34003

    oem_name='Shabik_360'
    stat_category='website'
    
    db_name='raw_data_device_shabik_360'
    #db_name='raw_data_test'

    #helper_mysql.clear_raw_data_space(oem_name=oem_name,category=stat_category,key='filtered_%',sub_key=None,date=current_date,table_name=db_name)

    #collect device-client matching rules
    #2011-04-18 10:00:10 GET /download/ve.jar - 41.206.130.5 HTTP/1.1 Nokia7610/2.0+(7.0642.0)+SymbianOS/7.0s+Series60/2.1+Profile/MIDP-2.0+Configuration/CLDC-1.0 - - 304 213 296
    
    #request by downloader
  	#2011-09-07 07:49:18 GET /download-gateway.aspx - 115.66.172.127 HTTP/1.1 s605th/shabik360/353391046226409 - - 200 328 218
  	#2011-09-07 07:49:18 GET /download-gateway.aspx - 115.66.172.127 HTTP/1.1 s603rd/shabik360/353391046226409 - - 200 328 218


    # init final stat plan

    stat_plan_detailed=Stat_plan()
    
    stat_sql_download_user_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_count_distinct_collection={'session':r'NET_SessionId=([0-9a-z]+)', \
                                                               'msisdn':r'msisdn=(\d+)', \
                                                               'imei':r's60\d\w\w\/shabik360\/(\d+)'},\
                                        select_count_exist={'action':r'(.)'},\
                                        select_average={'spent_time':r' (\d+)\s*$'}, \
                                        where={'filtered_sg':lambda line:True}, \
                                        group_by={'daily':get_current_date},
                                        db_name=db_name)
    
    stat_plan_detailed.add_stat_sql(stat_sql_download_user_daily, mixed_group_bys={ \
                            '1.cli-type':get_client_type_from_file_name, \
                            '2.file-name':get_file_name, \
                            '3.os':get_phone_os_type_from_native_useragent, \
                            #'5.ua':r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?:HTTP[^\s]+ )?([^\s]+)', \
                            '6.done':is_download_finished, \
                            })

    stat_plan_detailed.add_stat_sql(stat_sql_download_user_daily, mixed_group_bys={ \
                            '7.is-download':is_download_request, \
                            },keep_original_stat=False)

    stat_plan_detailed.add_stat_sql(stat_sql_download_user_daily, mixed_group_bys={ \
                            '6.done':is_download_finished, \
                            '8.special-file-type':get_stage_of_special_client_file, \
                            },keep_original_stat=False)

    stat_plan_detailed.add_stat_sql(stat_sql_download_user_daily, mixed_group_bys={ \
                            '6.done':is_download_finished, \
                            '9.file-type-start':is_start_stage_of_client_file, \
                            },keep_original_stat=False)

    stat_plan_detailed.add_stat_sql(stat_sql_download_user_daily, mixed_group_bys={ \
                            '6.done':is_download_finished, \
                            '10.file-type-end':is_end_stage_of_client_file, \
                            },keep_original_stat=False)
    
    """
    stat_sql_download_user_agent_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_first_text_value={'user_agent':r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?:HTTP[^\s]+ )?([^\s]+)'}, \
                                        where={'filtered':lambda line:True}, \
                                        group_by={'msisdn':r'msisdn=(\d+)'}, \
                                        db_name=db_name, \
                                        target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan_detailed.add_stat_sql(stat_sql_download_user_agent_daily)
    """

    stat_plan_detailed.reset()

    # init stat plan for reforming 206 request
    
    stat_plan_reform_206_request=Stat_plan()

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        process_exist={'reform_206_request':{'pattern':r'(.)', \
                                                             'process':process_line_reform_206_request}}, \
                        where={'filtered_sg':is_target_files})

    stat_plan_reform_206_request.add_stat_sql(stat_sql)

    stat_plan_reform_206_request.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.100\W3SVC1213381794\ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%y%m%d'))

    stat_plan_reform_206_request.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.175\W3SVC1213381794\ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%y%m%d'))

    stat_plan_reform_206_request.dump_sources()

    stat_plan_reform_206_request.run()    
    

    # include the partial_request_temp

    for k,v in partial_request_temp.iteritems():
        line=v[2]
        response_size=helper_regex.extract(line,r'\d+ (\d+) \d+\s*$')
        response_time=helper_regex.extract(line,r'\d+ \d+ (\d+)\s*$')
        line=helper_regex.regex_replace(' '+response_size,' '+str(v[0]),line)
        line=helper_regex.regex_replace(' '+response_time,' '+str(v[1]),line)

        stat_plan_detailed.process_line(line)

    # calculate final stat plan

    stat_plan_detailed.do_calculation()


    # check if download user turned into subscriber and logined

    target_keys=[
        ['Shabik_360','website','filtered_sg_9.file-type-start_daily_msisdn_unique','client-start-file'],
        ['Shabik_360','website','filtered_sg_1.cli-type_daily_msisdn_unique','Android'],
        ['Shabik_360','website','filtered_sg_8.special-file-type_daily_msisdn_unique','bb-start-file'],
        ['Shabik_360','website','filtered_sg_8.special-file-type_daily_msisdn_unique','jme-end-file'],
        ['Shabik_360','website','filtered_sg_1.cli-type_daily_msisdn_unique','Symbian-3'],
        ['Shabik_360','website','filtered_sg_1.cli-type_daily_msisdn_unique','Symbian-5'],
    ]

    for key in target_keys:
        common_shabik_360.calculate_subscription_login_status(oem_name=key[0],category=key[1],key=key[2],sub_key=key[3],date=current_date,table_name='raw_data_device_shabik_360',db_conn=None)
        pass




if __name__=='__main__':
    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_website(my_date)
