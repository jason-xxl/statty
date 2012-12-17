import helper_sql_server
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mysql
import helper_user_filter

helper_mysql.quick_insert=False
config.collection_cache_enabled=True


def stat_moagent(my_date):

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    print current_date

    oem_name='Vodafone'
    stat_category='moagent'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143

    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-shabik-360')

    target_keys=[
    
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','friend'],
        ['Vodafone','mochat','send_msg_text_daily_monet_id_unique',''],
        ['Vodafone','chatroom','enter_room_daily_monet_id_unique',''],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','message'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','profile'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','notification'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','photo'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','ocean_age'],
        ['Vodafone','im','user_try_login_daily_monet_id_unique',''],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','circle'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','poll'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','poke'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','setting'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','app_center'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','event'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','invite'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','gomoku'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','rss'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','youtube'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','phone_backup'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','twitter'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','netlog'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','help'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','facebook'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','email'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','happy_barn'],
        ['Vodafone','moagent','app_page_by_app_daily_visitor_unique','homepage'],
        ['Vodafone','moagent','app_page_daily_visitor_unique',''],
    
    ]

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')

    
    sql=r'''

    SELECT [user_id]
    FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [creationDate]>='%s' and [creationDate]<'%s'
    --and lastLogin is not null

    ''' % (start_time,end_time)

    new_user_set=helper_sql_server.fetch_set(conn_config=config.conn_vodafone_88,sql=sql)
    new_user_set=set([str(user_id) for user_id in new_user_set])    

    #init test
    """
    for key in target_keys:
        print key,current_date
        print len(helper_mysql.get_raw_collection_from_key(oem_name=key[0], \
                            category=key[1],key=key[2],sub_key=key[3], \
                            date=current_date, \
                            table_name='raw_data',db_conn=None))
    """

    #calculate
    for key in target_keys:

        result_key='only_retained_user_day_0,1_'+key[2]
        
        app_active_user_set=set([])
        
        for i in range(0,2):
            
            """
            temp=helper_mysql.get_raw_collection_from_key(oem_name=key[0], \
                            category=key[1],key=key[2],sub_key=key[3], \
                            date=helper_regex.date_add(current_date,i), \
                            table_name='raw_data',db_conn=None)
            if not temp:
                print 'ERROR:',key,helper_regex.date_add(current_date,i)
            """

            app_active_user_set |= helper_mysql.get_raw_collection_from_key(oem_name=key[0], \
                            category=key[1],key=key[2],sub_key=key[3], \
                            date=helper_regex.date_add(current_date,i), \
                            table_name='raw_data',db_conn=None)

        app_base_user_set = app_active_user_set & new_user_set
        
        helper_mysql.put_raw_data(oem_name=key[0],category=key[1],key=result_key,sub_key=key[3], \
                                    value=len(app_base_user_set),table_name='raw_data', \
                                    date=current_date)
        for i in range(2,5):

            result_key='only_retained_user_day_'+str(i)+'_'+key[2]

            app_active_user_set = helper_mysql.get_raw_collection_from_key(oem_name=key[0], \
                            category=key[1],key=key[2],sub_key=key[3], \
                            date=helper_regex.date_add(current_date,i), \
                            table_name='raw_data',db_conn=None)

            retained_user_set = app_active_user_set & app_base_user_set

            helper_mysql.put_raw_data(oem_name=key[0],category=key[1],key=result_key,sub_key=key[3], \
                                        value=len(retained_user_set),table_name='raw_data', \
                                        date=current_date)

if __name__=='__main__':

    for i in range(config.day_to_update_stat+30,0+0,-1):
        stat_moagent(time.time()-3600*24*i)


