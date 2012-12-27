import os, sys
from random import randint

ENGINE_ROOT = os.path.join(os.path.dirname(__file__),'../engine')
sys.path.insert(0, os.path.join(ENGINE_ROOT, "."))

from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
from celery.task import task
import time
import helper_regex
import helper_mysql
import helper_math
import helper_view
import gumi_helper_user
import config
from datetime import date, datetime, timedelta


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
    global temp_first_access_date,temp_last_access_date,temp_profile_create_date, \
    temp_profile_last_login_date,date_min,date_max,dict_date_to_collection_id,base_user_sets

    oem_name='All'
    stat_category='daily_active_user_retain'
    db_name='raw_data_login_trend'
    

    # you can change day range

    date_max=helper_regex.date_add(helper_regex.get_date_str_now(),-1)
    date_min=helper_regex.date_add(date_max,-30)

    list_of_country = ['SG']
    for country in list_of_country:

        for i in range(1,10000):

            current_date=helper_regex.date_add(date_min,i)
            print 'current date',current_date
            
            if current_date>date_max:
                break
            
            # get daily subscriber 

            start_time=current_date+' 00:00:00' # cos Mozat is 0:00:00-23:59:59, so AIS is the same
            end_time=helper_regex.date_add(current_date,1)+' 00:00:00'
            new_user_set=gumi_helper_user.get_user_ids_created_by_date(current_date)

            if country == '':
                daily_active_user_set = helper_mysql.get_raw_collection_from_key(oem_name='Gumi_puzzle', \
                                    category='user',key='live_log_by_country_daily_uid_unique_collection_id', \
                                    date=current_date, \
                                    table_name='raw_data',db_conn=None)
            else:
                daily_active_user_set = helper_mysql.get_raw_collection_from_key(oem_name='Gumi_puzzle', \
                                    category='user',key='live_log_by_country_daily_uid_unique_collection_id',sub_key=country, \
                                    date=current_date, \
                                    table_name='raw_data',db_conn=None)

            if not daily_active_user_set:
                print "NO Data Set for %s"%current_date
                pass
            
            new_active_user_set = new_user_set & daily_active_user_set
            # calculate

            print 'start calculate'


            base_user_sets={

                'new-subscriber':new_user_set,
                'new-subscriber_active':new_active_user_set
                ##############################################
                #                                            #
                #   Register new user group definition here  #
                #                                            #
                #   key name should contain "ais"            #
                #                                            #
                ##############################################

            }

            for k,user_set in base_user_sets.iteritems():
                
                k=k.replace('*','')

                # calculate total

                print 'user base of',k,':',len(user_set)

                key='daily_active_user_initial_%s_total_unique' % (k,)
                helper_mysql.put_raw_data(oem_name,stat_category,key,country,len(user_set),db_name,current_date)
                
                helper_mysql.put_collection(collection=user_set,oem_name=oem_name,category=stat_category, \
                                        key=key,sub_key=country,date=current_date,table_name=db_name)
            
            # calculate 


            ranges=[(1,8,1)]

            for r in ranges:
                start=r[0]
                end=r[1]
                step=r[2]

                accumulative_logined_user={
                    'dau':set([]),
                }
                    
                for i in range(start,end,step):
                    print start
                    print end
                    logined_user={
                        'dau':set([]),
                    }

                    for day_delta in range(i,i+step):
                        target_date=helper_regex.date_add(current_date,day_delta)

                        #'dau'
                        if country == '':
                            print "**************************************************************************"
                            collection = helper_mysql.get_raw_collection_from_key(oem_name='Gumi_puzzle', \
                                category='user',key='live_log_by_country_daily_uid_unique_collection_id',\
                                date=target_date, \
                                table_name='raw_data',db_conn=None)
                        else:
                            collection = helper_mysql.get_raw_collection_from_key(oem_name='Gumi_puzzle', \
                                category='user',key='live_log_by_country_daily_uid_unique_collection_id',sub_key=country, \
                                date=target_date, \
                                table_name='raw_data',db_conn=None)
                        
                        #if not collection:
                        #    raise Exception('ais_daily_active_user_set collection empty! '+target_date)

                        logined_user['dau'] = logined_user['dau'] | collection


                    for k1,v1 in logined_user.iteritems():
                        accumulative_logined_user[k1] |= v1

                    for k,user_set in base_user_sets.iteritems():

                        k=k.replace('*','')
                        
                        logined_user_temp=set([])

                        if k.find('new-subscriber')>-1:
                            
                            logined_user_temp=logined_user['dau']
                            accumulative_logined_user_temp=accumulative_logined_user['dau']
                                       

                        base_user_logined_user= user_set & logined_user_temp
                        key='daily_active_user_'+str(step)+'_day_logined_%s_total_unique' % (k,)
                        helper_mysql.put_raw_data(oem_name,stat_category,key,"%d_%s"%(i,country),len(base_user_logined_user),db_name,current_date)
                        
                        #helper_mysql.put_collection(collection=base_user_logined_user,oem_name=oem_name,category=stat_category, \
                        #                    key=key,sub_key=i,date=current_date,table_name=db_name)
                        base_user_no_logined_user= user_set - accumulative_logined_user_temp 
                        key='daily_active_user_'+str(step)+'_day_no_logined_%s_total_unique' % (k,)
                        helper_mysql.put_raw_data(oem_name,stat_category,key,"%d_%s"%(i,country),len(base_user_no_logined_user),db_name,current_date)
                        
                        #helper_mysql.put_collection(collection=base_user_logined_user,oem_name=oem_name,category=stat_category, \
                        #                    key=key,sub_key=i,date=current_date,table_name=db_name)



    #update view definition
    
    new_options=generate_html_discription_of_base_user_sets()
    sql_group_name=generate_sql_group_name_of_base_user_sets()
    sql_where=generate_sql_where_of_base_user_sets()


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

    print 'generate view'

    #helper_view.update_view(view_name='Report Daily Active Login Retain Rate', \
    #            func_to_replace_description=get_new_view_description, \
    #            func_to_replace_sql=get_new_sql)
    return



if __name__=='__main__':
    stat_login()

