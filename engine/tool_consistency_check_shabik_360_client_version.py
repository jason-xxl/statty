import helper_sql_server
import glob
import re
import os
import helper_regex
from helper_mysql import db
import _mysql
import helper_mysql
import helper_file
import config



def check():

    table_name='raw_data_shabik_360'
    target_date='2012-06-04'

    client_type_keys=[
        ["Shabik_360","moagent","app_page_by_morange_version_type_daily_user_unique","JME"],
        ["Shabik_360","moagent","app_page_by_morange_version_type_daily_user_unique","BlackBerry"],
        ["Shabik_360","moagent","app_page_by_morange_version_type_daily_user_unique","S60"],
        ["Shabik_360","moagent","app_page_by_morange_version_type_daily_user_unique","S60-3"],
        ["Shabik_360","moagent","app_page_by_morange_version_type_daily_user_unique","S60-5"],
        ["Shabik_360","moagent","app_page_by_morange_version_type_daily_user_unique","Android"],
        ["Shabik_360","moagent","app_page_by_morange_version_type_daily_user_unique","iOS"],
    ]

    result=[]
    
    for i in client_type_keys:
        set_i=helper_mysql.get_raw_collection_from_key(oem_name=i[0],category=i[1],key=i[2],sub_key=i[3],date=target_date,table_name=table_name)
        
        for j in client_type_keys:
            set_j=helper_mysql.get_raw_collection_from_key(oem_name=j[0],category=j[1],key=j[2],sub_key=j[3],date=target_date,table_name=table_name)
            
            if set_i==set_j:
                continue

            result.append('\t'.join([i[3],j[3],str(len(set_i & set_j)),str(len(set_i)),str(len(set_j))]))
        
    for r in result:
        print r
        


if __name__=='__main__':

    check()

