import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_mysql
import helper_file
import config

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date

temp_ua={}
temp_error={}
temp_crash_time={}

current_key=''

def process_line(line='',exist='',group_key=''):
    global current_date,temp_ua,temp_error,temp_crash_time,current_key
    #print line

    # ### Stat_Sql: File Path: \\192.168.1.52\upload_log_client_symbian\voda_egypt-Morange(6.0.2)-CS60(3.7.149)-S60(52)\2011-07-21\55624283-01-17-52(0).ua ###
    if helper_regex.extract(line,r'( ###)'):
        current_key=line.replace('.ua ###','').replace('.log ###','').replace('### Stat_Sql: File Path: ','')
        #print 'current_key:',current_key

    if helper_regex.extract(line,r'(P)I/\w+ Domain/@voda_egypt'):
        temp_ua[current_key]=helper_regex.regex_replace(r'(P)I/\w+ Domain/@voda_egypt','',line).strip(' ')
        #print 'temp_ua:',temp_ua[current_key]

    if helper_regex.extract(line,r'(\bexit:)'):
        temp_error[current_key]=helper_regex.extract(line,r'(\bexit:[^\n]*)').strip(' ')
        temp_crash_time[current_key]=helper_regex.extract(line,r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})').strip(' ').replace('-','_')
        #print 'temp_error:',temp_error[current_key],temp_crash_time[current_key]


def stat_moagent(my_date):

    oem_name='Mozat'
    stat_category='client_crash_only_globe'
    key='symbian_crash_log'

    global current_date,temp_ua,temp_error,temp_crash_time,current_key
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    ## extract raw info

    stat_plan=Stat_plan(plan_name='daily-client-crash')
    
    # clear space

    helper_mysql.clear_raw_data_space(oem_name=oem_name,category=stat_category,key=None,sub_key=None,date=current_date,table_name='raw_data_client_crash')
    temp_ua={}
    temp_error={}
    temp_crash_time={}
    current_key=''
    
    # \\192.168.0.110\upload_log\fast_globe-Morange(6.0.5)-CS60(3.7.3994)-S60(30)\2011-08-30\35548003-12-34-40(0).ua

    stat_sql_file_name_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                      process_exist={'process_line': {'pattern':r'(.)','process':process_line}},                                   
                                      db_name='raw_data_debug')

    stat_plan.add_stat_sql(stat_sql_file_name_daily)
    
    base_path=r'\\192.168.0.110\upload_log'

    dirs=helper_file.get_sub_dir_list_from_dir(base_path=base_path,name_pattern='(\\\\fast_globe-)')

    for i in dirs:
        files=helper_file.get_filtered_file_list_from_dir_tree(base_path=i+'\\'+current_date)
        stat_plan.add_log_source(files)
        print 'base_path:',i+'\\'+current_date
        #break

    stat_plan.run()    
    """
    """

    print temp_ua
    print temp_error
    print temp_crash_time

    """
    temp_ua={}
    temp_error={}
    temp_crash_time={}
    """

    ## analyze the result

    stat_plan=Stat_plan()

    stat_sql_send_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'uid:(\d+)'}, \
                                     group_by={'daily':get_current_date}, \
                                     db_name='raw_data_debug')

    stat_plan.add_stat_sql(stat_sql_send_msg_daily, mixed_group_bys={ \
                            '1.error-type':lambda line:helper_regex.extract(line,r'err:(.*?)(?:_uid:)').lower(), \
                            '2.cli-ver':lambda line:helper_regex.extract(line,r'cv:(.*?)(?:_err:)').lower(), \
                            '3.os':lambda line:helper_regex.extract(line,r' (S\d0\D\d+)').lower(), \
                            '4.model':lambda line:helper_regex.extract(line,r'\-:\-_(.*?)/').lower() \
                            })

    stat_plan.reset()

    for k in temp_ua.keys():
        if not temp_error.has_key(k) or not temp_error[k]:
            temp_error[k]='none'
        
        if not temp_crash_time.has_key(k) or not temp_crash_time[k]:
            temp_crash_time[k]='none'

        # client version->error type->user id->time crash->ua,date=log date
        # \\192.168.1.52\upload_log_client_symbian\voda_egypt-Morange(6.0.2)-CS60(3.7.149)-S60(52)\2011-07-21\55624283-01-17-52(0).ua
        
        line='_'.join([
                        'cv:'+helper_regex.extract(temp_ua[k],r'CS60\D*([\d\.]+)'),
                        'err:'+temp_error[k],
                        'uid:'+helper_regex.extract(k,r'\\(\d+)\-\d+\-\d+\-\d+\('),
                        'time:'+temp_crash_time[k],
                        '-:-',
                        temp_ua[k]
                        ]).replace('\n','').replace('\r','')

        stat_plan.process_line(line)

    stat_plan.do_calculation()



if __name__=='__main__':

    """
    print process_line(r'\\192.168.1.52\upload_log_client_symbian\voda_egypt-Morange(6.0.2)-CS60(3.7.149)-S60(52)\2011-07-21\55624283-01-17-52(0).ua')
    print process_line(r'Unknown_Device/Unknown_FVersion Encoding/UTF-8 Lang/en Caps/223 Morange/6.0.2 CS60/3.7.149 S60/52 PI/fda27f136dfc14aed0e13e4f69362e87 Domain/@voda_egypt')
    print process_line(r'2011-07-20 17:09:54 540500  : Ui exit: Terminate : 0 , 1')
    
    exit()
    """

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)
