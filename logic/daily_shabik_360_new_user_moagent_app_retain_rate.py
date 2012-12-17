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

    oem_name='Shabik_360'
    stat_category='moagent'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143

    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-shabik-360')

    target_keys=[
    
        ['All','im','only_shabik_360_user_try_login_daily_monet_id_unique',''],
        ['All','mochat','only_shabik_360_send_msg_text_daily_monet_id_unique',''],
        ['Shabik_360','chatroom','enter_room_only_shabik_360_daily_monet_id_unique',''],
        ['Shabik_360','moagent','app_page_daily_visitor_unique',''],
    
    ]

    target_apps=['profile','greeting_cards','football_war','help','saying','ocean_age_world','photo','twitter','mochat','photo_server','ocean_age','im','gomoku','new_user_wizard','message','homepage_old_version','event','tab_apps','poke','matrix','billing','notification','app_center','leader_board','happy_barn','client_prefetch','setting','poll','location','chatroom','circle','flickr','homepage','email','friend','status','hot_photo','invite','recent_visitor','star_user','netlog','texas_holdem','facebook','linkedin','baloot','rss','aladdin','phone_backup','level_system','register','youtube','public_photo','browser']
    for app in target_apps:
        target_keys.append(['Shabik_360','moagent','app_page_by_app_daily_visitor_unique',app])

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')

    
    sql=r'''

    SELECT [user_id]
    FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [creationDate]>='%s' and [creationDate]<'%s'
    and user_name like '%%shabik.com%%'
    --and lastLogin is not null

    ''' % (start_time,end_time)

    new_user_set=helper_sql_server.fetch_set(conn_config=config.conn_stc,sql=sql)
    new_user_set=set([str(user_id) for user_id in new_user_set])    

    #init test
    """
    for key in target_keys:
        print key,current_date
        print len(helper_mysql.get_raw_collection_from_key(oem_name=key[0], \
                            category=key[1],key=key[2],sub_key=key[3], \
                            date=current_date, \
                            table_name='raw_data_shabik_360',db_conn=None))
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
                            table_name='raw_data_shabik_360',db_conn=None)
            if not temp:
                print 'ERROR:',key,helper_regex.date_add(current_date,i)
            """
            
            app_active_user_set |= helper_mysql.get_raw_collection_from_key(oem_name=key[0], \
                            category=key[1],key=key[2],sub_key=key[3], \
                            date=helper_regex.date_add(current_date,i), \
                            table_name='raw_data_shabik_360',db_conn=None)

        app_base_user_set = app_active_user_set & new_user_set
        
        helper_mysql.put_raw_data(oem_name=key[0],category=key[1],key=result_key,sub_key=key[3], \
                                    value=len(app_base_user_set),table_name='raw_data_shabik_360', \
                                    date=current_date)
        for i in range(2,5):

            result_key='only_retained_user_day_'+str(i)+'_'+key[2]

            app_active_user_set = helper_mysql.get_raw_collection_from_key(oem_name=key[0], \
                            category=key[1],key=key[2],sub_key=key[3], \
                            date=helper_regex.date_add(current_date,i), \
                            table_name='raw_data_shabik_360',db_conn=None)

            retained_user_set = app_active_user_set & app_base_user_set

            helper_mysql.put_raw_data(oem_name=key[0],category=key[1],key=result_key,sub_key=key[3], \
                                        value=len(retained_user_set),table_name='raw_data_shabik_360', \
                                        date=current_date)

if __name__=='__main__':

    for i in range(config.day_to_update_stat+20,0+0,-1):
        stat_moagent(time.time()-3600*24*i)


