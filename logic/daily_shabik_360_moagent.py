import common_shabik_360
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mail
import helper_mysql
import helper_user_filter
import helper_sql_server
from user_id_filter import user_id_filter_stc_male
from user_id_filter import user_id_filter_stc_female
from shabik_360 import share,common
from qlx_src.helper import email_,datetime_

helper_mysql.quick_insert=True

def stat_moagent(my_date):
    
    
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    get_current_date=lambda line:current_date

    oem_name='Shabik_360'
    stat_category='moagent'
    table_name='raw_data_shabik_360'
    import helper_mail
    helper_mail.send_mail(title=oem_name+' '+stat_category+' '+str(current_date))
    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-shabik-360')

    # uv

    stat_sql_unique_user_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':{'key':r'monetid=(\d+)','date_units':['weekly','monthly'],'with_value':True}}, \
                                   #select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                   #                'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                   #                'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                   #                'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                   #                'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                   #                'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':lambda line:current_date},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_unique_user_daily)


    # app uv
    
    stat_sql_app_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':{'key':r'monetid=(\d+)','date_units':[],'with_value':True}}, \
                                   #select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                   #                'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                   #                'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                   #                'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                   #                'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                   #                'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':lambda line:current_date,'by_app':helper_regex.recognize_app_from_moagent_log_line},
                                   db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql_app_daily)

    # by url key 

    stat_sql_url_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                                   'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                                   #'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                                   #'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                                   #'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                                   #'page_size':r'(?:[^\t]+\t){6}(\d+)', \
                                                   }, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_url_pattern':helper_regex.get_simplified_url_unique_key,'daily':lambda line:current_date}, \
                                   db_name='data_url_pattern_shabik_360', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_url_daily)

    """
    # phone model
    
    stat_sql_phone_model_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_phone_model':helper_regex.extract_client_phone_model,'daily':r'(^\d+ \w+)'}, \
                                   db_name='raw_data_phone_model')

    stat_plan.add_stat_sql(stat_sql_phone_model_daily)
    
    """
    # morange version
    
    stat_sql_version_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_morange_version':helper_regex.extract_client_morange_version,'daily':get_current_date},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_version_daily)

    # morange version type
    
    stat_sql_version_type_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_morange_version_type':helper_regex.extract_client_morange_version_type_with_s3_s5, \
                                             'daily':lambda line:current_date},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_version_type_daily)

    # symbian for minor version
    
    # 29 May 16:00:00,822 - 9271224    141    125    0    0    0    1234    http://stc-oa.i.mozat.com:8080/OceanAge/foot.jsp?monetid=41502604&moclientwidth=240&userAgent=+NOKIA-6120c%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Far+Caps%2F7519+Morange%2F6.4.1+CS60%2F6.41.987+S60%2F30+PI%2Ff01b702206e428d83b9a6acf58f347d4+Domain%2F%40shabik.com+Source%2F&moclientheight=266&cli_ip=84.235.73.247
    
    stat_sql_only_symbian_user_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':{'key':r'monetid=(\d+)','date_units':[],'with_value':True}}, \
                                   where={'app_page':helper_regex.app_page_pattern,\
                                          'only_symbian': r'CS60(?:%2f|\s*)(6[\.\d]+)[ignorecase]'}, \
                                   group_by={'daily':lambda line:current_date, \
                                             'by_version':  r'CS60(?:%2f|\s*)(6[\.\d]+)[ignorecase]'},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_only_symbian_user_daily)

    # share
    stat_sql_only_share_user_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':{'key':r'monetid=(\d+)','date_units':[],'with_value':True}}, \
                                   where={'share':share.share_filter}, \
                                   group_by={'daily':lambda line:current_date, \
                                             'type':share.share_classify},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_only_share_user_daily)
    
    """
    # morange version
    
    stat_sql_version_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_morange_version':helper_regex.extract_client_morange_version_client_type,'daily':get_current_date},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_version_daily)
    """

    """
    # client lang
    
    stat_sql_client_lang_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_client_lang':r'\+Lang%2F(.*?)\+[ignorecase]', \
                                             'daily':get_current_date},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_client_lang_daily)

    # screen size
    
    stat_sql_screen_size_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_client_screen_size':helper_regex.extract_client_screen_size,'daily':get_current_date},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_screen_size_daily)
    """    

    """
    # app uv
    
    stat_sql_app_daily_retain_rate=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_retain_rate_by_date={'visitor':{'key':r'monetid=(\d+)','date_units':[1,2,3,4,5,6,7,14,21,28]}}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':get_current_date,'by_app':helper_regex.recognize_app_from_moagent_log_line},
                                   db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql_app_daily_retain_rate)
    
    # user phone info
    
    stat_sql_user_agent_str=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_text_value={'user_agent':r'userAgent=(.*?)(?:&|\s*$)'}, \
                                   select_first_int_value={'client_width':r'moclientwidth=(\d+)', \
                                                           'client_height':r'moclientheight=(\d+)', \
                                                           'ip':helper_regex.ip_to_number}, \
                                   where={'from_app_request':r'(u)serAgent=[^&$]'}, \
                                   group_by={'by_monet_id':r'monetid=(\d+)'}, \
                                   db_name='data_int_user_info_shabik_360', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)
    
    stat_plan.add_stat_sql(stat_sql_user_agent_str)
    """


    """
    # add filter for recent new user

    filter_2d_new_user=common_shabik_360.get_new_user_2d_filter(current_date,pattern=r'monetid=(\d+)')
    stat_plan.add_stat_brunch_filters({'only_2d_new_user':filter_2d_new_user})
    """


    stat_plan.add_stat_brunch_filters({'only_from_getjar':lambda line:helper_regex.extract(line,r'(u)serAgent=.*?(?:GetJar)')},stat_sql_unique_user_daily)
    stat_plan.add_stat_brunch_filters({'only_from_inmobi':lambda line:helper_regex.extract(line,r'(u)serAgent=.*?(?:InMobi2)')},stat_sql_unique_user_daily)

    """
    # app uv (male)

    stat_sql_app_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern, \
                                          'only_male':user_id_filter_stc_male.is_valid_user}, \
                                   group_by={'daily':lambda line:current_date,'by_app':helper_regex.recognize_app_from_moagent_log_line},
                                   db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql_app_daily)
  

    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern, \
                                          'only_male':user_id_filter_stc_male.is_valid_user}, \
                                   group_by={'daily':lambda line:current_date},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_daily)

    # app uv (female)

    stat_sql_app_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern, \
                                          'only_female':user_id_filter_stc_female.is_valid_user}, \
                                   group_by={'daily':lambda line:current_date,'by_app':helper_regex.recognize_app_from_moagent_log_line},
                                   db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql_app_daily)

  

    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern, \
                                          'only_female':user_id_filter_stc_female.is_valid_user}, \
                                   group_by={'daily':lambda line:current_date},
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_daily)
    """


    # [CBB log] 28 May 09:00:01,315 - 13381668003438	32	0	-1	-1	0	579	http://matrix-web.i.mozat.com:9600/api?action=get_reqs&domain=%40shabik.com&lan=ar&userAgent=8520_5.2.0.67_5.0.0.681%20JConf%2fCLDC-1.1%20JProf%2fMIDP-2.1%20Encoding%2fISO8859_1%20Locale%2far%20Lang%2far%20Caps%2f7516%20Morange%2f6.4.0.120321%20Domain%2f%40shabik.com%20CBB%2f120321%20CBBP%2f28e8765a%20Source%2fnull%20PI%2f0189626a352443aa4ab02c817317ca35&hash=1&monetid=44662699&cli_ip=84.235.73.209

    # blackberry os version / morange version
    
    stat_sql_version_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern, \
                                          'only_blackberry':r'userAgent=.*?(CBB)'}, \
                                   group_by={'by_morange_version':helper_regex.extract_client_morange_version_client_type, \
                                             'by_bb_os_version':r'userAgent=[^_]+_([^_]+_[0-9\.]+)', \
                                             'daily':get_current_date}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_version_daily)

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.174\logs_moagent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.177\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.117\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.118\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.75\logs_moagent_shabik_360\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.107\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.108\moAgent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.195\logs_moagent_shabik_360\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.185\logs_moagent_shabik_360\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.196\logs_moagent_shabik_360\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.run()

    # collection to user_filter

    print config.conn_stat_portal
    user_id_collection=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent',key='app_page_daily_visitor_unique_collection_id',sub_key='',date=current_date,table_name='raw_data_shabik_360')
    user_id_dict=dict((i,None) for i in user_id_collection)
    helper_user_filter.export_to_file('user_id_filter_shabik_360_'+current_date.replace('-','_'),user_id_dict)

    import py_compile
    py_compile.compile(r'E:\RoutineScripts\user_id_filter\user_id_filter_shabik_360_'+current_date.replace('-','_')+'.py')

    if not len(user_id_collection):
        import helper_mail
        helper_mail.send_mail(title='shabik_360 moagent uv empty',content_html='')


    ############################# inmobi channel
    inmobi_category='moagent_only_from_inmobi'
    inmobi_start_date = '2012-05-25'
    # inmobi users
    accumulative_inmobi_user=helper_mysql.get_raw_collection_from_key_date_range(\
                                 key='app_page_daily_visitor_unique',begin_date=inmobi_start_date,end_date=current_date,\
                                 table_name=table_name,oem_name=oem_name,category=inmobi_category)
    helper_mysql.put_collection(collection=accumulative_inmobi_user, key='accumulative_app_page_daily_visitor_unique',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    # inmobi stc users
    accumulative_inmobi_stc_user = common_shabik_360.retain_stc_user_id_set(accumulative_inmobi_user)
    helper_mysql.put_collection(collection=accumulative_inmobi_stc_user, key='accumulative_app_page_daily_visitor_stc_unique',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    # inmobi subscribed users
    accumulative_inmobi_sub_user = common_shabik_360.get_in_sub_id_set(accumulative_inmobi_user)
    print len(accumulative_inmobi_sub_user)
    helper_mysql.put_collection(collection=accumulative_inmobi_sub_user, key='accumulative_app_page_daily_visitor_sub_unique',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    # inmobi stc subscribed users
    accumulative_inmobi_stc_sub_user = common_shabik_360.get_in_sub_id_set(accumulative_inmobi_stc_user)
    print len(accumulative_inmobi_stc_sub_user)
    helper_mysql.put_collection(collection=accumulative_inmobi_stc_sub_user, key='accumulative_app_page_daily_visitor_stc_sub_unique',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    # stc users: subscription fees
#    accumulative_inmobi_msisdns = [msisdn for msisdn in common.get_id_msisdn_dict(accumulative_inmobi_user).values() if msisdn]
    accumulative_inmobi_msisdns = common.get_id_msisdn_dict(accumulative_inmobi_stc_user).values()
    today = datetime.fromtimestamp(my_date)
    (start_datetime,end_datetime) = datetime_.get_daily_range(today,5) # offset is 5
    fees = common.get_subscription_fees(accumulative_inmobi_msisdns, start_datetime, end_datetime)
    helper_mysql.put_raw_data(value=fees, key='daily_sub_fee',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    # stc users: top-up fees
    today = datetime.fromtimestamp(my_date)
    (start_datetime,end_datetime) = datetime_.get_daily_range(today,5) # offset is 5
    top_up_fees = common.get_top_up_fees(accumulative_inmobi_stc_user,start_datetime, end_datetime)
    helper_mysql.put_raw_data(value=top_up_fees, key='daily_top_up_fee',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    # new users daily or accumulative
    daily = True
    accumulative_new_user = set()
    if daily:
        accumulative_new_user = common_shabik_360.get_user_ids_created_in_time_range(current_date) # daily users
        key_prefix = ''
        print current_date, len(accumulative_new_user)
    else: # accu
        key_prefix = 'accumulative_'
        for iter_date in helper_regex.date_iterator(begin_date=inmobi_start_date, end_date=current_date):
            new_users = common_shabik_360.get_user_ids_created_in_time_range(iter_date) # all the users
            accumulative_new_user |= new_users #(no overlappping)
            print iter_date, len(new_users), len(accumulative_new_user), len(accumulative_inmobi_stc_user)
    
    # inmobi new users
    accumulative_inmobi_new_user = accumulative_inmobi_user & accumulative_new_user
    print len(accumulative_inmobi_new_user)
    helper_mysql.put_collection(collection=accumulative_inmobi_new_user, key=key_prefix+'app_page_daily_visitor_new_unique',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    # inmobi new stc users
    accumulative_inmobi_stc_new_user = accumulative_inmobi_stc_user & accumulative_new_user
    print len(accumulative_inmobi_stc_new_user)
    helper_mysql.put_collection(collection=accumulative_inmobi_stc_new_user, key=key_prefix+'app_page_daily_visitor_new_stc_unique',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)

    # new stc users: subscription fees
    accumulative_inmobi_msisdns = common.get_id_msisdn_dict(accumulative_inmobi_stc_new_user).values()
    today = datetime.fromtimestamp(my_date)
    (start_datetime,end_datetime) = datetime_.get_daily_range(today,5) # offset is 5
    fees = common.get_subscription_fees(accumulative_inmobi_msisdns, start_datetime, end_datetime)
    helper_mysql.put_raw_data(value=fees, key='daily_new_stc_sub_fee',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    # new stc users: top-up fees
    today = datetime.fromtimestamp(my_date)
    (start_datetime,end_datetime) = datetime_.get_daily_range(today,5) # offset is 5
    top_up_fees = common.get_top_up_fees(accumulative_inmobi_stc_new_user,start_datetime, end_datetime)
    helper_mysql.put_raw_data(value=top_up_fees, key='daily_new_stc_top_up_fee',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
        
    # inmobi new subscribed users
    accumulative_inmobi_new_sub_user = common_shabik_360.get_in_sub_id_set(accumulative_inmobi_new_user)
    print len(accumulative_inmobi_new_sub_user)
    helper_mysql.put_collection(collection=accumulative_inmobi_new_sub_user, key=key_prefix+'app_page_daily_visitor_new_sub_unique',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    # inmobi new stc subscribed users
    accumulative_inmobi_stc_new_sub_user = common_shabik_360.get_in_sub_id_set(accumulative_inmobi_stc_new_user)
    print len(accumulative_inmobi_stc_new_sub_user)
    helper_mysql.put_collection(collection=accumulative_inmobi_stc_new_sub_user, key=key_prefix+'app_page_daily_visitor_new_stc_sub_unique',\
                                table_name=table_name,oem_name=oem_name,category=inmobi_category,date=current_date)
    
    print '\n'.join(msisdn for id, msisdn in common_shabik_360.get_id_msisdn_dict(accumulative_inmobi_stc_new_sub_user).iteritems() if msisdn)
    
if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)

