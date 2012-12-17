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
import urllib2
import urllib
import httplib
import pymssql
import string

config.collection_cache_enabled=True
helper_mysql.quick_insert=True

base_user_sets={}

conn_stc_public_domain_mssql={
    'host':'192.168.0.211',
    'user':'morange2',
    'password':'morange2',
    'database':'public_domain'
}

conn_stc_public_domain_object = pymssql.connect(host=conn_stc_public_domain_mssql['host'],
                                             user=conn_stc_public_domain_mssql['user'],
                                             password=conn_stc_public_domain_mssql['password'],
                                             database=conn_stc_public_domain_mssql['database'])

conn_stc_billing_stc_broadcast_mssql={
    'host':'192.168.0.86',
    'user':'sa',
    'password':'m0z@tm0r@nge',
    'database':'stc_broadcast'
}
conn_stc_billing_stc_broadcast_object = pymssql.connect(host=conn_stc_billing_stc_broadcast_mssql['host'],
                                             user=conn_stc_billing_stc_broadcast_mssql['user'],
                                             password=conn_stc_billing_stc_broadcast_mssql['password'],
                                             database=conn_stc_billing_stc_broadcast_mssql['database'])

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
        ret.append(r'   or `oem_name`="Shabik_360" and `category`="login_retain_experiment" and case when :key%s="1" then `key` like "%%_%s_total%%" else 0 end' % (idx,k))
        
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

def get_str_user_id_list_by_msisdn_list(msisdn_list):
    '''
    get user id by user's msisdn
    '''
    return_value_user_id_list = []

    if len(msisdn_list)==0:
        return return_value_user_id_list
    
    cur_stc_public_domain = conn_stc_public_domain_object.cursor()

    sql_condition_user_name_list = 'user_name in ('
    flag_no_valid_msisdn = 1
    for msisdn in msisdn_list:
        msisdn = string.strip(msisdn)
        if string.find(msisdn, '\'') != int(-1) or string.find(msisdn, '\"') != -1 :
            continue
        flag_no_valid_msisdn = 0
        user_name = string.replace(msisdn,'+966', '0') + '@shabik.com'
        sql_condition_user_name_list = sql_condition_user_name_list + "'%s',"%user_name
        
    sql_condition_user_name_list = sql_condition_user_name_list[:-1] + ')'
    #print sql_condition_user_name_list
    sql='''
    select user_id, user_name 
    from user_info 
    where %s 
    '''%sql_condition_user_name_list

    if flag_no_valid_msisdn:
        print 'no valid msisn'
        return return_value_user_id_list
    
    try:
        cur_stc_public_domain.execute(sql)
        row = cur_stc_public_domain.fetchone()
        while row:
            return_value_user_id_list.append(str(row[0]))
            row = cur_stc_public_domain.fetchone()
    except:
        print 'sql error in get_user_id_list_by_msisdn_list'
        print sql[:100]

    return return_value_user_id_list
    

def stat_login():

    global temp_first_access_date,temp_last_access_date,temp_profile_create_date, \
    temp_profile_last_login_date,date_min,date_max,dict_date_to_collection_id,base_user_sets

    oem_name='Shabik_360'
    stat_category='login_retain_experiment'
    db_name='raw_data_login_trend'
    

    # you can change day range

    #date_max=helper_regex.date_add(helper_regex.get_date_str_now(),-1)
    date_max='2012-07-03'
    #date_min=helper_regex.date_add(date_max,-30)
    date_min='2012-06-23'

   
    for i in range(1,10000):

        current_date=helper_regex.date_add(date_min,i)

        prev_date = helper_regex.date_add(current_date,-1)
        next_date = helper_regex.date_add(current_date,1)

        print 'current date:',current_date
        
        #print date_min
        #print current_date

        if current_date>date_max:
            break
        
        # get daily subscriber 
        start_time=prev_date  +' 05:00:00'
        end_time=current_date +' 05:00:00'
        

        #shabik saudi new user

        shabik_360_saudi_new_user_set=common_shabik_360.get_user_ids_created_in_time_range(current_date)
 
        test_set={'2012-06-01':set(['20010',]),}

        #SMS call back users:

        #########################################
        # get msisdn from list_history
        cur=conn_stc_billing_stc_broadcast_object.cursor()

        sql='''
        select msisdn, sms_id, [from],[key],[createdon] from list_history as t1
        where createdon > '%s' and createdon < '%s'
        and t1.[key] not like '%%_unsub_%%'
        '''%(start_time, end_time)

        cur.execute(sql)
        sql=''
        row = cur.fetchone()
        list_msisdn = []
        while row:
            list_msisdn.append(row[0])
            row = cur.fetchone()

        if len(list_msisdn) ==0:
            continue

        # get user_id

        int_interval=10000
        int_start=0
        int_end=int_start+int_interval
        sms_user_id_set=set([])
        while int_end<len(list_msisdn)+1:
            user_id_list=get_str_user_id_list_by_msisdn_list(list_msisdn[int_start:int_end])
            sms_user_id_set=sms_user_id_set|set(user_id_list)
            int_start=int_end
            int_end=int_end+int_interval        

        # get daily_login_set
        login_set_in_5_hours = helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360', \
                            category='moagent',key='app_page_daily_visitor_unique',sub_key='', \
                            date=prev_date, \
                            table_name='raw_data_shabik_360',db_conn=None)

        login_set_1day_after = helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360', \
                            category='moagent',key='app_page_daily_visitor_unique',sub_key='', \
                            date=current_date, \
                            table_name='raw_data_shabik_360',db_conn=None)
        daily_login_set = login_set_in_5_hours | login_set_1day_after
        
        # get daily callback set
        daily_callback_set = sms_user_id_set & daily_login_set
        #########################################
        #print current_date
        #print type(iter(sms_user_id_set).next())
        #print type(iter(daily_login_set).next())
        print 'len(sms_user_id_set):', len(sms_user_id_set)
        print 'len(daily_login_set):', len(daily_login_set)
        print 'len(daily_callback_set):', len(daily_callback_set)
        os.system('pause')
        # calculate
        base_user_sets={

            'shabik-360_saudi-stc_new-subscriber':shabik_360_saudi_new_user_set,
            'shabik-360_test_set':test_set.get(current_date,set([])),
            ##############################################
            'shabik-360_inactive_callback_set':daily_callback_set,
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
            }
                
            for i in range(start,end,step):

                logined_user={
                    'shabik-360':set([]),
                }

                for day_delta in range(i,i+step):
                    target_date=helper_regex.date_add(current_date,day_delta)

                    #'shabik-360'
                    collection = helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360', \
                            category='moagent',key='app_page_daily_visitor_unique',sub_key='', \
                            date=target_date, \
                            table_name='raw_data_shabik_360',db_conn=None)

                    logined_user['shabik-360'] = logined_user['shabik-360'] | collection
                    

                for k1,v1 in logined_user.iteritems():
                    accumulative_logined_user[k1] |= v1

                for k,user_set in base_user_sets.iteritems():

                    k=k.replace('*','')
                    
                    logined_user_temp=set([])
                    if k.find('shabik-360')>-1:
                        logined_user_temp=logined_user['shabik-360']
                        accumulative_logined_user_temp=accumulative_logined_user['shabik-360']
                                        

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


    helper_view.update_view(view_name='Report Shabik_360 New User Login Rate Daily', \
                func_to_replace_description=get_new_view_description, \
                func_to_replace_sql=get_new_sql)

    """
    helper_view.update_view(view_name='Report Shabik_360 New User Login Rate Simplified Daily', \
                func_to_replace_description=get_new_view_description, \
                func_to_replace_sql=get_new_sql)
    """

    return



if __name__=='__main__':

    stat_login()
    conn_stc_billing_stc_broadcast_object.close()
    conn_stc_public_domain_object.close()