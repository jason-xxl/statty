import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_math
from datetime import date, datetime, timedelta
import common_shabik_360
import helper_view

config.collection_cache_enabled=True
helper_mysql.quick_insert=True

base_user_sets={}

def generate_sql_group_name_of_base_user_sets():
    global base_user_sets
    keys=sorted(base_user_sets.keys())

    ret=[]
    for k in keys:
        k=k.replace('*','')
        key_name=(' '.join(k.split('_')).replace('-',' ')).title()
        ret.append(r"		when `key` like '%%_%s_total%%' then '%s'" % (k,key_name))
        
    return '\n'.join(ret)

def generate_sql_where_of_base_user_sets():
    global base_user_sets
    keys=sorted(base_user_sets.keys())

    idx=0
    ret=[]
    for k in keys:
        k=k.replace('*','')

        key_name=(' '.join(k.split('_')).replace('-',' ')).title()
        idx+=1
        ret.append(r'   or `oem_name`="All" and `category`="login_retain" and case when :key%s="1" then `key` like "%%_%s_total%%" else 0 end' % (idx,k))
        
    return '\n'.join(ret)

def generate_html_discription_of_base_user_sets():
    global base_user_sets
    keys=sorted(base_user_sets.keys())

    idx=0
    ret=[]
    for k in keys:
        key_name=(' '.join(k.split('_')).replace('-',' ')).title()
        #k=k.replace('*','')
        idx+=1
        ret.append(r'<li>%s <input type="checkbox" name="key%s" value="1" /></li>' % (key_name,idx,))
        
    return ''.join(ret)
    

def stat_login():

    global temp_first_access_date,temp_last_access_date,temp_profile_create_date, \
    temp_profile_last_login_date,date_min,date_max,dict_date_to_collection_id,base_user_sets

    oem_name='All'
    stat_category='login_retain'
    db_name='raw_data_login_trend'

    date_max=helper_regex.date_add(helper_regex.get_date_str_now(),-1)
    date_min=helper_regex.date_add(date_max,-30)

   
    for i in range(1,10000):

        current_date=helper_regex.date_add(date_min,i)
        print 'current date:',current_date
        
        if current_date>date_max:
            break
        
        # get daily subscriber 

        start_time=current_date+' 05:00:00'
        end_time=helper_regex.date_add(current_date,1)+' 05:00:00'
        
    
        
        #shabik saudi new user

        shabik_360_saudi_new_user_set=common_shabik_360.get_user_ids_created_in_time_range(current_date)
        
        #shabik saudi new user stc

        shabik_360_saudi_stc_new_user_set=common_shabik_360.get_user_ids_created_in_time_range(current_date,only_stc=True)
        
        #shabik saudi new user non stc

        shabik_360_saudi_non_stc_new_user_set=common_shabik_360.get_user_ids_created_in_time_range(current_date,only_stc=False)

        # inmobi user
        inmobi_user_set = helper_mysql.get_raw_collection_from_key(key='app_page_daily_visitor_unique',date=current_date,\
                                 table_name='raw_data_shabik_360',oem_name='Shabik_360',category='moagent_only_from_inmobi')
    
    
        shabik_360_saudi_active_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_daily_visitor_unique',sub_key='', \
                                    date=current_date,table_name='raw_data_shabik_360',db_conn=None)
        
        shabik_360_saudi_client_symbian_3_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60-3', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)
        shabik_360_saudi_client_symbian_5_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60-5', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)
        shabik_360_saudi_client_symbian_user_set=shabik_360_saudi_client_symbian_3_user_set | shabik_360_saudi_client_symbian_5_user_set | \
                                    helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)
        
        shabik_360_saudi_client_android_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='Android', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)
        shabik_360_saudi_client_ios_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='iOS', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)
        shabik_360_saudi_client_blackberry_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='BlackBerry', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)

        shabik_360_saudi_client_jme_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='JME', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)
        # fix jme set
        shabik_360_saudi_client_jme_user_set=shabik_360_saudi_client_jme_user_set-shabik_360_saudi_client_blackberry_user_set 

        shabik_360_saudi_client_unknown_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='Unknown', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)
        
        shabik_360_saudi_client_6_4_0_120426_jme_user_set=helper_mysql.get_merged_collection_from_raw_data_with_sub_key_pattern( \
                                    oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_daily_user_unique_collection_id', \
                                    sub_key_pattern='(\-6\.4\.0\.120426).*?(?:CJME|MIDP)', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None) & shabik_360_saudi_client_jme_user_set
        
        shabik_360_saudi_client_6_4_6_120427_jme_user_set=helper_mysql.get_merged_collection_from_raw_data_with_sub_key_pattern( \
                                    oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_daily_user_unique_collection_id', \
                                    sub_key_pattern='(\-6\.4\.6\.120427).*?(?:CJME|MIDP)', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None) & shabik_360_saudi_client_jme_user_set
        
        shabik_360_saudi_client_6_4_7_120427_jme_user_set=helper_mysql.get_merged_collection_from_raw_data_with_sub_key_pattern( \
                                    oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_daily_user_unique_collection_id', \
                                    sub_key_pattern='(\-6\.4\.7\.120427).*?(?:CJME|MIDP)', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None) & shabik_360_saudi_client_jme_user_set

        
        shabik_360_saudi_client_6_4_7_120510_jme_user_set=helper_mysql.get_merged_collection_from_raw_data_with_sub_key_pattern( \
                                    oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_daily_user_unique_collection_id', \
                                    sub_key_pattern='(\-6\.4\.7\.120510).*?(?:CJME|MIDP)', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None) & shabik_360_saudi_client_jme_user_set

        shabik_360_saudi_client_6_4_jme_user_set=helper_mysql.get_merged_collection_from_raw_data_with_sub_key_pattern( \
                                    oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_daily_user_unique_collection_id', \
                                    sub_key_pattern='(\-6\.4).*?(?:CJME|MIDP)', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None) & shabik_360_saudi_client_jme_user_set


        shabik_360_saudi_client_6_4_0_120521_jme_user_set=helper_mysql.get_merged_collection_from_raw_data_with_sub_key_pattern( \
                                    oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_daily_user_unique_collection_id', \
                                    sub_key_pattern='(\-6\.4\.0\.120521).*?(?:CJME|MIDP)', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None) & shabik_360_saudi_client_jme_user_set 
        
        shabik_360_saudi_client_6_4_7_120606_jme_user_set=helper_mysql.get_merged_collection_from_raw_data_with_sub_key_pattern( \
                                    oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_daily_user_unique_collection_id', \
                                    sub_key_pattern='(\-6\.4\.7\.120606).*?(?:CJME|MIDP)', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None) & shabik_360_saudi_client_jme_user_set 
                                                                
        # symbian minor version

        shabik_360_saudi_client_6_42_1406_symbian_user_set=helper_mysql.get_raw_collection_from_key( \
                                    oem_name='Shabik_360',category='moagent', \
                                    key='app_page_only_symbian_by_version_daily_visitor_unique', \
                                    sub_key='6.42.1406', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)
        
        shabik_360_saudi_client_6_41_1426_symbian_user_set=helper_mysql.get_raw_collection_from_key( \
                                    oem_name='Shabik_360',category='moagent', \
                                    key='app_page_only_symbian_by_version_daily_visitor_unique', \
                                    sub_key='6.41.1426', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None) 
       
                                    
        """   
        shabik_360_saudi_active_user_8520_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_phone_model_daily_visitor_unique',sub_key='8520', \
                                    date=current_date,table_name='raw_data_test',db_conn=None)
        shabik_360_saudi_active_user_9300_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_phone_model_daily_visitor_unique',sub_key='9300', \
                                    date=current_date,table_name='raw_data_test',db_conn=None)
        shabik_360_saudi_active_user_nokia6120c_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_phone_model_daily_visitor_unique',sub_key='nokia6120c', \
                                    date=current_date,table_name='raw_data_test',db_conn=None)
        shabik_360_saudi_active_user_9780_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_phone_model_daily_visitor_unique',sub_key='9780', \
                                    date=current_date,table_name='raw_data_test',db_conn=None)
        shabik_360_saudi_active_user_nokiac3_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_phone_model_daily_visitor_unique',sub_key='nokiac3', \
                                    date=current_date,table_name='raw_data_test',db_conn=None)
        shabik_360_saudi_active_user_nokian70_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_phone_model_daily_visitor_unique',sub_key='nokian70', \
                                    date=current_date,table_name='raw_data_test',db_conn=None)
        """

        # game heroes new player

        start_time=current_date+' 05:00:00'
        end_time=helper_regex.date_add(current_date,1)+' 05:00:00'

        sql=r'''

        select PlayerID
        from `woh`.`player`
        where `CreatedTime`>='%s' and `CreatedTime`<'%s'

        ''' % (start_time,end_time)

        shabik_360_heroes_new_player_set=helper_mysql.fetch_set(sql,config.conn_stc_heroes)
        shabik_360_heroes_new_player_set=set([str(user_id) for user_id in shabik_360_heroes_new_player_set])

       
        shabik_360_saudi_active_user_auto_add_friends_set = helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='matrix', \
                                    key='auto_add_mutual_friend_daily_user_from_unique',sub_key='', \
                                    date=current_date,table_name='raw_data_shabik_360',db_conn=None)


        #########################################
        #                                       #
        #   Add new user group definition here  #
        #                                       #
        #########################################

        """

        #vodafone

        start_time=current_date+' 06:00:00'
        end_time=helper_regex.date_add(current_date,1)+' 06:00:00'

        sql=r'''

        SELECT [user_id]
        FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [creationDate]>='%s' and [creationDate]<'%s' 
        and [user_name] like '%%@voda_egypt'
        and [user_name] not like '%%000000%%'
        and [user_name] not like '%%motest%%'
        --and lastLogin is not null
        --and len('20'+replace([user_name],'@voda_egypt',''))>=12

        ''' % (start_time,end_time)

        vodafone_new_user_set=helper_sql_server.fetch_set(conn_config=config.conn_vodafone_88,sql=sql)
        vodafone_new_user_set=set([str(user_id) for user_id in vodafone_new_user_set])

        vodafone_active_user_set = helper_mysql.get_raw_collection_from_key(oem_name='Vodafone', \
                        category='moagent',key='app_page_daily_visitor_unique',sub_key='', \
                        date=current_date, \
                        table_name='raw_data',db_conn=None)

        vodafone_client_jme_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='JME', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        
        #vodafone_client_symbian_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
        #                            key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60', \
        #                            date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)

        vodafone_client_symbian_3_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60-3', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        vodafone_client_symbian_5_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60-5', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        vodafone_client_symbian_user_set=shabik_360_saudi_client_symbian_3_user_set | shabik_360_saudi_client_symbian_5_user_set | \
                                    helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        
        vodafone_client_android_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='Android', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        vodafone_client_ios_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='iOS', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        vodafone_client_blackberry_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='BlackBerry', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        vodafone_client_unknown_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='Unknown', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)

        # mozat new user


        start_time=current_date+' 00:00:00'
        end_time=helper_regex.date_add(current_date,1)+' 00:00:00'

        sql=r'''

        SELECT [user_id]
        FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [creationDate]>='%s' and [creationDate]<'%s'
        and user_name not like 'circle_%%'
        and user_name like '%%@morange.com'

        ''' % (start_time,end_time)

        mozat_new_user_set=helper_sql_server.fetch_set(conn_config=config.conn_mozat,sql=sql)
        mozat_new_user_set=set([str(user_id) for user_id in mozat_new_user_set])

        mozat_active_user_set = helper_mysql.get_raw_collection_from_key(oem_name='Mozat', \
                        category='moagent',key='app_page_daily_visitor_unique',sub_key='', \
                        date=current_date, \
                        table_name='raw_data',db_conn=None)

        mozat_client_jme_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='JME', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        
        #mozat_client_symbian_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='moagent', \
        #                            key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60', \
        #                            date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)

        mozat_client_symbian_3_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60-3', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        mozat_client_symbian_5_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60-5', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        mozat_client_symbian_user_set=shabik_360_saudi_client_symbian_3_user_set | shabik_360_saudi_client_symbian_5_user_set | \
                                    helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='S60', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)

        mozat_client_android_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='Android', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        mozat_client_ios_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='iOS', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        mozat_client_blackberry_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='BlackBerry', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        mozat_client_unknown_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='Unknown', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)



        # thai new user

        start_time=current_date+' 00:00:00'
        end_time=helper_regex.date_add(current_date,1)+' 00:00:00'

        sql=r'''

        SELECT [user_id]
        FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [creationDate]>='%s' and [creationDate]<'%s'
        and user_name not like 'circle_%%'
        and version_tag='fast_ais'

        ''' % (start_time,end_time)

        thai_new_user_set=helper_sql_server.fetch_set(conn_config=config.conn_mozat,sql=sql)
        thai_new_user_set=set([str(user_id) for user_id in thai_new_user_set])

        thai_active_user_set = helper_mysql.get_raw_collection_from_key(oem_name='Mozat', \
                        category='moagent_only_ais',key='app_page_daily_visitor_unique',sub_key='', \
                        date=current_date, \
                        table_name='raw_data',db_conn=None)



        # thai-ais new user

        sql=r'''

        SELECT [user_id]
        FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [creationDate]>='%s' and [creationDate]<'%s'
        and user_name not like 'circle_%%'
        and version_tag='fast_ais'
        and user_name like '668%%'

        ''' % (start_time,end_time)

        thai_ais_new_user_set=helper_sql_server.fetch_set(conn_config=config.conn_mozat,sql=sql)
        thai_ais_new_user_set=set([str(user_id) for user_id in thai_ais_new_user_set])


        # thai-non-ais new user

        sql=r'''

        SELECT [user_id]
        FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [creationDate]>='%s' and [creationDate]<'%s'
        and user_name not like 'circle_%%'
        and version_tag='fast_ais'
        and user_name not like '668%%'

        ''' % (start_time,end_time)

        thai_non_ais_new_user_set=helper_sql_server.fetch_set(conn_config=config.conn_mozat,sql=sql)
        thai_non_ais_new_user_set=set([str(user_id) for user_id in thai_non_ais_new_user_set])

        """


        # consistency check

        """
        print 'consistency check: shabik 360, client:',current_date
        print len(shabik_360_saudi_active_user_set)
        print len(shabik_360_saudi_client_jme_user_set | shabik_360_saudi_client_symbian_user_set | shabik_360_saudi_client_blackberry_user_set | shabik_360_saudi_client_ios_user_set | shabik_360_saudi_client_android_user_set | shabik_360_saudi_client_unknown_user_set)
        print len((shabik_360_saudi_client_jme_user_set | shabik_360_saudi_client_symbian_user_set | shabik_360_saudi_client_blackberry_user_set | shabik_360_saudi_client_ios_user_set | shabik_360_saudi_client_android_user_set | shabik_360_saudi_client_unknown_user_set) & shabik_360_saudi_active_user_set)

        print 'consistency check: vodafone, client:',current_date
        print len(vodafone_active_user_set)
        print len(vodafone_client_jme_user_set | vodafone_client_symbian_user_set | vodafone_client_blackberry_user_set | vodafone_client_ios_user_set | vodafone_client_android_user_set | vodafone_client_unknown_user_set)
        print len((vodafone_client_jme_user_set | vodafone_client_symbian_user_set | vodafone_client_blackberry_user_set | vodafone_client_ios_user_set | vodafone_client_android_user_set | vodafone_client_unknown_user_set) & vodafone_active_user_set)
        """

        #continue 

        # calculate
        base_user_sets={

#            'mozat_new-subscriber':mozat_new_user_set,
#                'mozat_new-subscriber_active':mozat_new_user_set & mozat_active_user_set,
#                    'mozat_new-subscriber_active_jme':mozat_new_user_set & mozat_client_jme_user_set & mozat_active_user_set,
#                    'mozat_new-subscriber_active_symbian':mozat_new_user_set & mozat_client_symbian_user_set & mozat_active_user_set,
#                    'mozat_new-subscriber_active_blackberry':mozat_new_user_set & mozat_client_blackberry_user_set & mozat_active_user_set,
#                    'mozat_new-subscriber_active_ios':mozat_new_user_set & mozat_client_ios_user_set & mozat_active_user_set,
#                    'mozat_new-subscriber_active_android':mozat_new_user_set & mozat_client_android_user_set & mozat_active_user_set,
#
#            'thai_new-subscriber':thai_new_user_set,
#                'thai_new-subscriber_active':thai_new_user_set & thai_active_user_set,
#                    'thai-ais_new-subscriber_active':thai_ais_new_user_set & thai_active_user_set,
#                    'thai-non-ais_new-subscriber_active':thai_non_ais_new_user_set & thai_active_user_set,
#
#            'vodafone_new-subscriber':vodafone_new_user_set,
#                'vodafone_new-subscriber_active*':vodafone_new_user_set & vodafone_active_user_set,
#                    'vodafone_new-subscriber_active_jme':vodafone_new_user_set & vodafone_client_jme_user_set & vodafone_active_user_set,
#                    'vodafone_new-subscriber_active_symbian':vodafone_new_user_set & vodafone_client_symbian_user_set & vodafone_active_user_set,
#                    'vodafone_new-subscriber_active_blackberry':vodafone_new_user_set & vodafone_client_blackberry_user_set & vodafone_active_user_set,
#                    'vodafone_new-subscriber_active_ios':vodafone_new_user_set & vodafone_client_ios_user_set & vodafone_active_user_set,
#                    'vodafone_new-subscriber_active_android':vodafone_new_user_set & vodafone_client_android_user_set & vodafone_active_user_set,
#

            'shabik-360_saudi_new-subscriber':shabik_360_saudi_new_user_set,
                'shabik-360_saudi_new-subscriber_active':shabik_360_saudi_new_user_set & shabik_360_saudi_active_user_set,
#                    'shabik-360_saudi_new-subscriber_active_mod2=0':helper_math.filter_by_mod(shabik_360_saudi_new_user_set,2,0) & shabik_360_saudi_active_user_set,
#                    'shabik-360_saudi_new-subscriber_active_mod2=1':helper_math.filter_by_mod(shabik_360_saudi_new_user_set,2,1) & shabik_360_saudi_active_user_set,
                
                'shabik-360_saudi-non-stc_new-subscriber':shabik_360_saudi_non_stc_new_user_set,
                    'shabik-360_saudi-non-stc_new-subscriber_active':shabik_360_saudi_non_stc_new_user_set & shabik_360_saudi_active_user_set,

                'shabik-360_saudi-stc_new-subscriber':shabik_360_saudi_stc_new_user_set,

                    'shabik-360_saudi-stc_new-subscriber_active_mod2=0':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set,2,0) & shabik_360_saudi_active_user_set,
                    'shabik-360_saudi-stc_new-subscriber_active_mod2=1':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set,2,1) & shabik_360_saudi_active_user_set,
                    
                    'shabik-360_saudi-stc_new-subscriber_active':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_set,
                        'shabik-360_saudi-stc_new-subscriber_active_jme':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_jme_user_set & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_jme_mod3=0':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_jme_user_set,3,0) & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_jme_mod3=1':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_jme_user_set,3,1) & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_jme_mod3=2':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_jme_user_set,3,2) & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_jme_mod3=12':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_jme_user_set,3,1) | helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_jme_user_set,3,2) & shabik_360_saudi_active_user_set,
                            
                        'shabik-360_saudi-stc_new-subscriber_active_symbian':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_user_set & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_symbian_mod3=0':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_user_set,3,0) & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_symbian_mod3=1':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_user_set,3,1) & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_symbian_mod3=2':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_user_set,3,2) & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_symbian_mod3=12':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_user_set,3,1) | helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_user_set,3,2) & shabik_360_saudi_active_user_set,
#                            'shabik-360_saudi-stc_new-subscriber_active_symbian_3':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_3_user_set & shabik_360_saudi_active_user_set,
#                                'shabik-360_saudi-stc_new-subscriber_active_symbian_3_mod3=0':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_3_user_set,3,0) & shabik_360_saudi_active_user_set,
#                                'shabik-360_saudi-stc_new-subscriber_active_symbian_3_mod3=1':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_3_user_set,3,1) & shabik_360_saudi_active_user_set,
#                                'shabik-360_saudi-stc_new-subscriber_active_symbian_3_mod3=2':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_3_user_set,3,2) & shabik_360_saudi_active_user_set,
#                                'shabik-360_saudi-stc_new-subscriber_active_symbian_3_mod3=12':(helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_3_user_set,3,1) & shabik_360_saudi_active_user_set)|(helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_3_user_set,3,2) & shabik_360_saudi_active_user_set),
#
#                            'shabik-360_saudi-stc_new-subscriber_active_symbian_5':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_5_user_set & shabik_360_saudi_active_user_set,
#                            
#                                'shabik-360_saudi-stc_new-subscriber_active_symbian_5_mod3=0':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_5_user_set,3,0) & shabik_360_saudi_active_user_set,
#                                'shabik-360_saudi-stc_new-subscriber_active_symbian_5_mod3=1':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_5_user_set,3,1) & shabik_360_saudi_active_user_set,
#                                'shabik-360_saudi-stc_new-subscriber_active_symbian_5_mod3=2':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_5_user_set,3,2) & shabik_360_saudi_active_user_set,
#                                'shabik-360_saudi-stc_new-subscriber_active_symbian_5_mod3=12':(helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_5_user_set,3,1) | helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_symbian_5_user_set,3,2)) & shabik_360_saudi_active_user_set,
                        
                        'shabik-360_saudi-stc_new-subscriber_active_blackberry':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_blackberry_user_set & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_blackberry_mod3=0':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_blackberry_user_set,3,0) & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_blackberry_mod3=1':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_blackberry_user_set,3,1) & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_blackberry_mod3=2':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_blackberry_user_set,3,2) & shabik_360_saudi_active_user_set,
                            'shabik-360_saudi-stc_new-subscriber_active_blackberry_mod3=12':(helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_blackberry_user_set,3,1) & shabik_360_saudi_active_user_set)|(helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_blackberry_user_set,3,2) & shabik_360_saudi_active_user_set),

                        'shabik-360_saudi-stc_new-subscriber_active_ios':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_ios_user_set & shabik_360_saudi_active_user_set,
                        'shabik-360_saudi-stc_new-subscriber_active_android':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_android_user_set & shabik_360_saudi_active_user_set,

                        'shabik-360_saudi-stc_new-subscriber_active_mod3=0':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_set,3,0),
                        'shabik-360_saudi-stc_new-subscriber_active_mod3=1':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_set,3,1),
                        'shabik-360_saudi-stc_new-subscriber_active_mod3=2':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_set,3,2),
                        'shabik-360_saudi-stc_new-subscriber_active_mod3=12':helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_set,3,1) | helper_math.filter_by_mod(shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_set,3,2),
                    
            'temp_shabik-360_saudi-stc_new-subscriber_active_jme_6.4.0.120426':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_4_0_120426_jme_user_set,
            'temp_shabik-360_saudi-stc_new-subscriber_active_jme_6.4.6.120427':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_4_6_120427_jme_user_set,
            'temp_shabik-360_saudi-stc_new-subscriber_active_jme_6.4.7.120427':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_4_7_120427_jme_user_set,
            'temp_shabik-360_saudi-stc_new-subscriber_active_jme_6.4.7.120510':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_4_7_120510_jme_user_set,
            'temp_shabik-360_saudi-stc_new-subscriber_active_jme_6.4.':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_4_jme_user_set,
            
            'temp_shabik-360_saudi-stc_new-subscriber_active_jme_6.4.0.120521':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_4_0_120521_jme_user_set,
            'temp_shabik-360_saudi-stc_new-subscriber_active_jme_6.4.7.120606':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_4_7_120606_jme_user_set,
            'temp_shabik-360_saudi-stc_new-subscriber_active_jme_no_inmobi_6.4.0.120521':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_4_0_120521_jme_user_set - inmobi_user_set,       
            'temp_shabik-360_saudi-stc_new-subscriber_active_jme_no_inmobi_6.4.7.120606':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_4_7_120606_jme_user_set- inmobi_user_set,
            
            
            'temp_shabik-360_saudi-stc_new-subscriber_active_symbian_6.42.1406':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_42_1406_symbian_user_set,
            'temp_shabik-360_saudi-stc_new-subscriber_active_symbian_6.41.1426':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_41_1426_symbian_user_set,
            
            'temp_shabik-360_saudi-stc_new-subscriber_active_symbian_no_inmobi_6.42.1406':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_42_1406_symbian_user_set- inmobi_user_set,
            'temp_shabik-360_saudi-stc_new-subscriber_active_symbian_no_inmobi_6.41.1426':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_client_6_41_1426_symbian_user_set- inmobi_user_set,
            
                                                                                                                                           
#            'temp_shabik-360_saudi-stc_new-subscriber_active_8520':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_8520_set,
#            'temp_shabik-360_saudi-stc_new-subscriber_active_9300':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_9300_set,
#            'temp_shabik-360_saudi-stc_new-subscriber_active_nokia6120c':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_nokia6120c_set,
#            'temp_shabik-360_saudi-stc_new-subscriber_active_9780':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_9780_set,
#            'temp_shabik-360_saudi-stc_new-subscriber_active_nokiac3':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_nokiac3_set,
#            'temp_shabik-360_saudi-stc_new-subscriber_active_nokian70':shabik_360_saudi_stc_new_user_set & shabik_360_saudi_active_user_nokian70_set,
            
            'temp_shabik-360_saudi_app-heroes_new-player':shabik_360_heroes_new_player_set & shabik_360_saudi_active_user_set,

            'temp_shabik-360_saudi_active_user_auto_add_friends_set': shabik_360_saudi_new_user_set & shabik_360_saudi_active_user_set & shabik_360_saudi_active_user_auto_add_friends_set,
            'temp_shabik-360_saudi_active_user_auto_add_no_friends_set': shabik_360_saudi_new_user_set & shabik_360_saudi_active_user_set - shabik_360_saudi_active_user_auto_add_friends_set, 

            ##############################################
            #                                            #
            #   Register new user group definition here  #
            #                                            #
            ##############################################

        }

        for k,user_set in base_user_sets.iteritems():
            
            k=k.replace('*','')

            # calculate total

            print 'user base of',k,':',len(user_set)

            key='daily_new_user_initial_%s_total_unique' % (k,)
            helper_mysql.put_raw_data(oem_name,stat_category,key,'',len(user_set),db_name,current_date)
            
            helper_mysql.put_collection(collection=user_set,oem_name=oem_name,category=stat_category, \
                                    key=key,sub_key='',date=current_date,table_name=db_name)


            """
            # calculate unsub

            unsubbed_user_count=helper_sql_server.fetch_scalar_int(config.conn_mt,sql=r'''
                select count(distinct msisdn)
                from shabik_mt.dbo.accounts with(nolock)
                where 
                is_deleted=1
                and msisdn in (
                    select 
                    replace('+'+replace(user_name,'@shabik.com',''),'+0','+966') as msisdn
                    from db85.mozone_user.dbo.profile with(nolock)
                    where user_id in (%s)
                )
            ''' % (','.join(user_set) or '0'))

            print 'unsub user:',unsubbed_user_count
    
            key='daily_new_user_unsub_%s_unique' % (k,)
            helper_mysql.put_raw_data(oem_name,stat_category,key,'',unsubbed_user_count,db_name,current_date)
            """ 


        # calculate 


        ranges=[(1,56,3),(1,8,1)]

        for r in ranges:
            start=r[0]
            end=r[1]
            step=r[2]

            accumulative_logined_user={
                'shabik-360':set([]),
                'vodafone':set([]),
                'mozat':set([]),
            }
                
            for i in range(start,end,step):

                logined_user={
                    'shabik-360':set([]),
                    'vodafone':set([]),
                    'mozat':set([]),
                }

                for day_delta in range(i,i+step):
                    target_date=helper_regex.date_add(current_date,day_delta)

                    #'shabik-360'
                    collection = helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360', \
                            category='moagent',key='app_page_daily_visitor_unique',sub_key='', \
                            date=target_date, \
                            table_name='raw_data_shabik_360',db_conn=None)

                    logined_user['shabik-360'] = logined_user['shabik-360'] | collection
                    
                    #'vodafone'
                    collection = helper_mysql.get_raw_collection_from_key(oem_name='Vodafone', \
                                                category='moagent',key='app_page_daily_visitor_unique',sub_key='', \
                                                date=target_date, \
                                                table_name='raw_data',db_conn=None)

                    logined_user['vodafone'] = logined_user['vodafone'] | collection
                    
                    #'mozat'
                    collection = helper_mysql.get_raw_collection_from_key(oem_name='Mozat', \
                                                category='moagent',key='app_page_daily_visitor_unique',sub_key='', \
                                                date=target_date, \
                                                table_name='raw_data',db_conn=None)

                    logined_user['mozat'] = logined_user['mozat'] | collection
                

                for k1,v1 in logined_user.iteritems():
                    accumulative_logined_user[k1] |= v1

                for k,user_set in base_user_sets.iteritems():

                    k=k.replace('*','')
                    
                    logined_user_temp=set([])
                    if k.find('shabik-360')>-1:
                        logined_user_temp=logined_user['shabik-360']
                        accumulative_logined_user_temp=accumulative_logined_user['shabik-360']
                    elif k.find('vodafone')>-1:
                        logined_user_temp=logined_user['vodafone']
                        accumulative_logined_user_temp=accumulative_logined_user['vodafone']
                    elif k.find('mozat')>-1 or  k.find('thai')>-1:
                        logined_user_temp=logined_user['mozat']
                        accumulative_logined_user_temp=accumulative_logined_user['mozat']
                    

                    base_user_logined_user= user_set & logined_user_temp
                    key='daily_new_user_'+str(step)+'_day_logined_%s_total_unique' % (k,)
                    helper_mysql.put_raw_data(oem_name,stat_category,key,i,len(base_user_logined_user),db_name,current_date)
                    
                    #helper_mysql.put_collection(collection=base_user_logined_user,oem_name=oem_name,category=stat_category, \
                    #                    key=key,sub_key=i,date=current_date,table_name=db_name)
               

                    base_user_no_logined_user= user_set - accumulative_logined_user_temp
                    key='daily_new_user_'+str(step)+'_day_no_login_%s_total_unique' % (k,)
                    helper_mysql.put_raw_data(oem_name,stat_category,key,i,len(base_user_no_logined_user),db_name,current_date)
                    
                    #helper_mysql.put_collection(collection=base_user_logined_user,oem_name=oem_name,category=stat_category, \
                    #                    key=key,sub_key=i,date=current_date,table_name=db_name)



    #update view definition
    
    new_options=generate_html_discription_of_base_user_sets()
    sql_group_name=generate_sql_group_name_of_base_user_sets()
    sql_where=generate_sql_where_of_base_user_sets()

    #print new_options
    #print sql_group_name
    #print sql_where

    def get_new_view_description(old_description):
        import re
        new_view_description=helper_regex.regex_replace(re.compile(r'(<ol>(\n|\r|[^\n\r])*?(?:</ol>))',re.MULTILINE),'<ol>'+new_options+'</ol>',old_description)

        return new_view_description
    
    def get_new_sql(old_sql):
        import re
        new_view_sql=helper_regex.regex_replace(re.compile(r'((?:/\*group name def start\*/)(\n|\r|[^\n\r])*?(?:/\*group name def end\*/))',re.MULTILINE), \
                                                        '/*group name def start*/\n'+sql_group_name+'\n/*group name def end*/', \
                                                        old_sql)
        new_view_sql=helper_regex.regex_replace(re.compile(r'((?:/\*group where def start\*/)(\n|\r|[^\n\r])*?(?:/\*group where def end\*/))',re.MULTILINE), \
                                                        '/*group where def start*/\n'+sql_where+'\n/*group where def end*/', \
                                                        new_view_sql)
        return new_view_sql

    helper_view.update_view(view_name='Report Overall New User Login Rate Daily', \
                func_to_replace_description=get_new_view_description, \
                func_to_replace_sql=get_new_sql)

    helper_view.update_view(view_name='Report Overall New User Login Rate Simplified Daily', \
                func_to_replace_description=get_new_view_description, \
                func_to_replace_sql=get_new_sql)

    return



if __name__=='__main__':

    stat_login()

