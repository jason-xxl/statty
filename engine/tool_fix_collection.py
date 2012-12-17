
import config
import glob
import re
import helper_regex
import helper_mysql
import helper_math
import helper_mysql


config.collection_cache_enabled=True

def fix(target_date):
    
    set_jme=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='JME', \
                                    date=target_date,table_name='raw_data_shabik_360',db_conn=None)
    
    set_blackberry=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='BlackBerry', \
                                    date=target_date,table_name='raw_data_shabik_360',db_conn=None)

    helper_mysql.put_collection(collection=set_jme,oem_name='Shabik_360',category='moagent', \
                                    key='expired_app_page_by_morange_version_type_daily_user_unique_collection_id',sub_key='JME', \
                                    date=target_date,table_name='raw_data_shabik_360',db_conn=None)

    print len(set_jme)
    print len(set_blackberry)
    print len(set_jme & set_blackberry)

if __name__=='__main__':

    for i in range(1,300):
        target_date=helper_regex.date_add(helper_regex.get_date_str_now(),-i)
        fix(target_date)
