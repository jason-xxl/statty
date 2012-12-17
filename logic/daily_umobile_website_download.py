import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_file
import helper_regex
import helper_mysql
import config


def is_target_files(line):
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if file_name.find('umobile')>-1:
        return file_name
    return ''

download_last_files=[ 
    '/download/umobile-3.sisx',
    '/download/umobile-5.sisx',
    '/download/umobile.apk',
    '/download/umobile.jar',
    '/download/umobilebb46-6.cod', #loose the condition of last file of bb, for bb's last file may vary in different release
    '/download/umobilebb46-7.cod',
    '/download/umobilebb47-6.cod',
    '/download/umobilebb47-7.cod',
    '/download/umobilebb5-6.cod',
    '/download/umobilebb5-7.cod',
    '/download/umobilebb6-6.cod',
    '/download/umobilebb6-7.cod',
]

download_first_files=[ 
    '/download/umobile-3.sisx',
    '/download/umobile-5.sisx',
    '/download/umobile.apk',
    '/download/umobile.jad',
    '/download/umobilebb46.jad',
    '/download/umobilebb47.jad',
    '/download/umobilebb5.jad',
    '/download/umobilebb6.jad',
]

def is_last_file(line):
    global download_last_files
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if file_name in download_last_files:
        return True
    return False

def is_first_file(line):
    global download_first_files
    file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if file_name in download_first_files:
        return True
    return False

def get_response_code(line):
    response_code=helper_regex.extract(line,r'(\d{3}) \d+ \d+')
    if not response_code:
        return ''
    return response_code

file_size={}

def is_download_finished (line):

    """
    global file_size
    download_url_prefix='http://shabik.net.sa'
    """

    response_size=0
    response_status=''
    file_name=''

    try:
        response_size=int(helper_regex.extract(line,r'\d+ (\d+) \d+\s*$'))
        response_status=int(helper_regex.extract(line,r'(\d+) \d+ \d+\s*$'))
        file_name=helper_regex.extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    except:
        #print 'problemetic log (response size): '+line
        return 'U'

    if response_status==304 or response_status==206 or response_status==200:
        return 'Y'

    return 'N'

    """
    if not file_size.has_key(file_name):
        size=helper_mysql.get_raw_data(oem_name='Vodafone',category='website',key='client_file_size',sub_key=file_name, \
                                       default_value=-1,table_name='raw_data_url_pattern_shabik_360',date=current_date)
        if size>0:
            file_size[file_name]=size
            #print 'file size from db of '+download_url_prefix+file_name+' is: '+str(file_size[file_name])
        else:
            file_size[file_name]=helper_file.get_web_file_size(download_url_prefix+file_name)
            #print 'file current size of '+download_url_prefix+file_name+' is: '+str(file_size[file_name])
            
            helper_mysql.put_raw_data(oem_name='Vodafone',category='website',key='client_file_size',sub_key=file_name, \
                                      value=file_size[file_name],table_name='raw_data_url_pattern_shabik_360',date=current_date)

    if file_size[file_name]==-1:
        #print '-1 file size: '+file_size[file_name]
        raise Exception()

    is_finished=response_size>file_size[file_name]

    if not is_finished:
        #print 'broken response: '+file_name+'('+str(response_size)+' / '+str(file_size[file_name])+'): '+line
        pass

    return file_name if is_finished else ''
    """

def get_unique_id(line):
    #return helper_regex.extract(line,r'sess=([\w\-]+)')
    #line=r'2012-01-10 16:00:51 192.168.6.130 GET /umobile.aspx p=umobile - 123.136.98.69 MAUI+WAP+Browser User-Identity-Forward-msisdn=60183834792;User-Identity-Forward-ppp-username=void;User-Identity-Authentication=Bearer;ip-address=10.22.111.220;Bearer-Type=UDP;wtls-security-level=none;network-access-type=WCDMA;nas-ip-address=10.33.138.2;nas-identifier=10.33.138.2;apn=GGHTC;charging-id=3363855777;imsi=502181075125086;Called-station-id=my3g;accounting-session-id=7B886554C8805DA1;accounting-authentication-method=2;sgsn-ip-address=203.82.89.43;negotiated-qos=99-13921F7164D1FE4402FFFF;charging-characteristics=Postpaid;imei=3586880000001578 - 200 2678 31'
    
    msisdn=helper_regex.extract(line,r'User\-Identity\-Forward\-msisdn=(\d{11}|\d{12})')
    if not msisdn:
        return ''
   
    return msisdn
    

def stat_website(my_date,simple=False):

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    get_current_date=lambda line:current_date

    #2010-06-12 16:00:00 GET /rpc/avatar.ashx user_id=12896151&size=48 94.96.36.135 HTTP/1.1 Mozilla/4.0+(compatible;+MSIE+8.0;+Windows+NT+6.1;+Trident/4.0;+SLCC2;+.NET+CLR+2.0.50727;+.NET+CLR+3.5.30729;+.NET+CLR+3.0.30729;+Media+Center+PC+6.0;+InfoPath.2) http://shabik.net.sa/chatroom2/main.aspx?roomId=54 302 466 296
    #2010-06-12 16:00:00 POST /rpc/photos/RatingProxy.ashx - 188.117.114.228 HTTP/1.1 Mozilla/5.0+(Windows;+U;+Windows+NT+5.1;+en-US)+AppleWebKit/533.4+(KHTML,+like+Gecko)+Chrome/5.0.375.70+Safari/533.4 http://shabik.net.sa/photos/albums.aspx?user_id=3319753&photo_id=13017261&album_id=998364 200 178 312
    #2011-08-13 16:02:12 GET /360/stc.jar - 84.235.75.80 HTTP/1.1 Nokia7230/5.0+(06.90)+Profile/MIDP-2.1+Configuration/CLDC-1.1 - 200 517172 34003


    oem_name='Mozat'
    stat_category='website_only_umobile'
    db_name='raw_data_device'

    if not simple:
        helper_mysql.clear_raw_data_space(oem_name=oem_name,category=stat_category,key='filtered_%',sub_key=None,date=current_date,table_name=db_name)
    
    stat_plan=Stat_plan()

    #collect device-client matching rules
    #2011-04-18 10:00:10 GET /download/ve.jar - 41.206.130.5 HTTP/1.1 Nokia7610/2.0+(7.0642.0)+SymbianOS/7.0s+Series60/2.1+Profile/MIDP-2.0+Configuration/CLDC-1.0 - - 304 213 296

    stat_sql_download_user_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_count_distinct_collection={'session':get_unique_id},\
                                        select_count_exist={'action':r'(.)'},\
                                        select_average={'spent_time':r' (\d+)\s*$'}, \
                                        where={'filtered':is_target_files}, \
                                        group_by={'daily':get_current_date},
                                        db_name=db_name)

    if simple:
        mixed_group_bys={ \
                        '1.cli-type':helper_regex.get_client_type_from_file_name, \
                        '2.file-name':r'(?:GET|POST) ([^\s]+)', \
                        '6.done':is_download_finished \
                        }
    else:
        mixed_group_bys={ \
                        '1.cli-type':helper_regex.get_client_type_from_file_name, \
                        '2.file-name':r'(?:GET|POST) ([^\s]+)', \
                        '3.os':helper_regex.get_phone_os_type_from_native_useragent, \
                        '4.resp-code':get_response_code, \
                        '5.ua':r' HTTP\/[\d\.]+ ([^\s]+)', \
                        '6.done':is_download_finished \
                        }
    
    stat_plan.add_stat_sql(stat_sql_download_user_daily, mixed_group_bys=mixed_group_bys)


    stat_sql_download_user_start_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_count_distinct_collection={'session':get_unique_id},\
                                        select_count_exist={'action':r'(.)'},\
                                        where={'filtered':is_target_files, \
                                               'is_first_file':is_first_file}, \
                                        group_by={'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_download_user_start_daily)



    stat_sql_download_user_end_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_count_distinct_collection={'session':get_unique_id},\
                                        select_count_exist={'action':r'(.)'},\
                                        where={'filtered':is_target_files, \
                                               'is_last_file':is_last_file}, \
                                        group_by={'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_download_user_end_daily)


    stat_plan.add_log_source(r'\\192.168.0.130\log_download\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'*')

    stat_plan.run()    



if __name__=='__main__':
    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_website(my_date)
