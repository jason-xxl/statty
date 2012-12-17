import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_viva
import config
import re
import glob
import helper_mail

found_paths={}
disappeared_paths={}
date=None
pass_paths=['E:\\','\\\\192.168.0.130\\130_concurrent_log\\','\\\\192.168.1.37\\checkbot\\']

stat_plan=None

def record_log_path(line='',exist='',group_key=''):
    exist=re.sub(r'[^\\]*$','',exist)
    if exist:
        found_paths[exist]=stat_plan.current_file
    
def check_log_path():
    for i,j in found_paths.iteritems():
        log_files=None
        try:
            log_files=glob.glob(i+'*')
        except Exception as e:
            disappeared_paths[i]=j
            print str(e)
            continue
        
        if i in pass_paths:
            continue
        
        if not log_files or not check_file_exist(log_files):
            disappeared_paths[i]=j
        

def check_file_exist(log_files):
    date_str_1=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    date_str_2=datetime.fromtimestamp(my_date).strftime('%y%m%d')
    date_str_3=datetime.fromtimestamp(my_date).strftime('%Y_%m_%d')
    for i in log_files:
        if i.find(date_str_1)>-1 or i.find(date_str_2)>-1 or i.find(date_str_3)>-1:
            return True
    print date_str_1,date_str_2,date_str_3
    print log_files
    return False    
        
def alert_disappeared_paths():
    print disappeared_paths
    if disappeared_paths:
        log_source_list=[]
        for i,j in disappeared_paths.iteritems():
            log_source_list.append(j+': '+i+'\n')
        log_source_list.sort()
        helper_mail.send_mail(title='stat portal: data source check report',\
                              content_html='<br/>\n'.join(log_source_list))



def path_check(my_date):
    
    global stat_plan
    date=my_date
    
    stat_plan=Stat_plan()

    #log_files_2=r'\\192.168.0.79\logsMoIM\morange.log.' \
    #             +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
    #             +'*'#+'-10'#

    stat_sql_process_path=Stat_sql(process_exist={'log_path_1':{'pattern':r"r(?:'|\")((?:\\\\\d+\.\d+\.\d+\.\d+|\w:)[^']*?(?:\\[\w\.\d]*'))", \
                                                              'process':record_log_path}})
    
    stat_plan.add_stat_sql(stat_sql_process_path)
    
    py_files=r'E:\RoutineScripts\*.py'
    
    stat_plan.add_log_source(py_files)

    stat_plan.run()    

    





if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        path_check(my_date)
        print found_paths

        check_log_path()
        alert_disappeared_paths()
