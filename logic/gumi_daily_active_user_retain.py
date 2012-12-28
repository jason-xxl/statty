import os, sys
from random import randint

ENGINE_ROOT = os.path.join(os.path.dirname(__file__),'../engine')
sys.path.insert(0, os.path.join(ENGINE_ROOT, "."))

from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
from celery.task import task
from datetime import date, datetime, timedelta
import time
import helper_regex
import helper_mysql
import helper_math
import helper_view
import gumi_helper_user
import config

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
        ret.append(r"       when `key` like '%%_%s_total%%' then '%s'" % (k,key_name))
        
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
        ret.append(r'   or `oem_name`="All" and `category`="daily_active_user_retain" and case when :key%s="1" then `key` like "%%_%s_total%%" else 0 end' % (idx,k))
        
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

@task
def stat_login():
    global date_min,date_max,base_user_sets

    oem_name='All'
    stat_category='daily_active_user_retain'
    db_name='raw_data_login_trend'

    # you can change day range (30 days)
    date_max=helper_regex.date_add(helper_regex.get_date_str_now(),-1)
    date_min=helper_regex.date_add(date_max,-30)

    for i in range(1,10000):

        current_date=helper_regex.date_add(date_min,i)
        print 'current date',current_date
        
        if current_date>date_max:
            break
        
        # new user set from db (overall daily active user)
        new_user_set=gumi_helper_user.get_user_ids_created_by_date(current_date)
        # daily active user SG
        active_user_sg = helper_mysql.get_raw_collection_from_key(oem_name='Gumi_puzzle', \
                        category='user',key='live_log_by_country_daily_uid_unique_collection_id',sub_key='SG', \
                        date=current_date, \
                        table_name='raw_data',db_conn=None)
        # daily active user US
        active_user_us = helper_mysql.get_raw_collection_from_key(oem_name='Gumi_puzzle', \
                        category='user',key='live_log_by_country_daily_uid_unique_collection_id',sub_key='US', \
                        date=current_date, \
                        table_name='raw_data',db_conn=None)
        # daily active user PL
        active_user_pl = helper_mysql.get_raw_collection_from_key(oem_name='Gumi_puzzle', \
                        category='user',key='live_log_by_country_daily_uid_unique_collection_id',sub_key='PL', \
                        date=current_date, \
                        table_name='raw_data',db_conn=None)
        # daily active user Unknow IP
        active_user_zz = helper_mysql.get_raw_collection_from_key(oem_name='Gumi_puzzle', \
                        category='user',key='live_log_by_country_daily_uid_unique_collection_id',sub_key='ZZ', \
                        date=current_date, \
                        table_name='raw_data',db_conn=None)

        base_user_sets={
            'pt-new-user-':new_user_set,
            'pt-new-user-SG':new_user_set & active_user_sg,
            'pt-new-user-US':new_user_set & active_user_us,
            'pt-new-user-PL':new_user_set & active_user_pl,
            'pt-new-user-ZZ':new_user_set & active_user_zz
        }
        for k,user_set in base_user_sets.iteritems():
            k=k.replace('*','')
            # calculate total
            print 'user base of',k,':',len(user_set)
            key='active_user_initial_%s_total_unique' % (k,)
            #sub_key = k[-2:]
            #if sub_key.find('-')>-1:
            #    sub_key=''
            helper_mysql.put_raw_data(oem_name,stat_category,key,'',len(user_set),db_name,current_date)
            helper_mysql.put_collection(collection=user_set,oem_name=oem_name,category=stat_category, \
                                    key=key,sub_key='',date=current_date,table_name=db_name)

        # calculate 
        ranges=[(1,8,1),(1,30,7),(1,60,14)]

        for r in ranges:
            start=r[0]
            end=r[1]
            step=r[2]

            accumulative_logined_user={
                'pt':set([]),
            }
                
            for i in range(start,end,step):
                print start
                print end
                logined_user={
                    'pt':set([]),
                }

                for day_delta in range(i,i+step):
                    target_date=helper_regex.date_add(current_date,day_delta)
                    collection = helper_mysql.get_raw_collection_from_key(oem_name='Gumi_puzzle', \
                        category='user',key='live_log_daily_uid_unique_collection_id',sub_key='', \
                        date=target_date, \
                        table_name='raw_data',db_conn=None) 
                    logined_user['pt'] = logined_user['pt'] | collection

                for k1,v1 in logined_user.iteritems():
                    accumulative_logined_user[k1] |= v1

                for k,user_set in base_user_sets.iteritems():
                    k=k.replace('*','')
                    
                    logined_user_temp=set([])

                    if k.find('pt')>-1:
                        logined_user_temp=logined_user['pt']
                        accumulative_logined_user_temp=accumulative_logined_user['pt']

                    base_user_logined_user= user_set & logined_user_temp
                    key='daily_active_user_'+str(step)+'_day_logined_%s_total_unique' % (k,)
                    helper_mysql.put_raw_data(oem_name,stat_category,key,i,len(base_user_logined_user),db_name,current_date)
                    
                    base_user_no_logined_user= user_set - accumulative_logined_user_temp 
                    key='daily_active_user_'+str(step)+'_day_no_logined_%s_total_unique' % (k,)
                    helper_mysql.put_raw_data(oem_name,stat_category,key,i,len(base_user_no_logined_user),db_name,current_date)

    return



if __name__=='__main__':
    stat_login()