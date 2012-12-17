import common_shabik_360
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mysql
import common_shabik_360

helper_mysql.quick_insert=True

def stat_user_behavior(begin_creation_date,end_creation_date,begin_observation_date,end_observation_date,inactive_day_threshold=7,marketing_key=''):

    oem_name='Shabik_360'
    category='analysis_user'

    current_date=end_observation_date
    

    ## prepare

    # get new user

    new_user_to_creation_date_dict={}
    for d in helper_regex.date_iterator(begin_creation_date,end_creation_date,step=1):
        temp_new_user_set=common_shabik_360.get_user_ids_created_in_time_range(target_date=d,only_stc=None)
        for u in temp_new_user_set:
            new_user_to_creation_date_dict[u]=d
            
    new_user_set=set(new_user_to_creation_date_dict.keys())

    # get ever active new user

    new_user_to_last_login_date_dict={}
    for d in helper_regex.date_iterator(begin_observation_date,end_observation_date,step=1):
        temp_active_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                                key='app_page_daily_visitor_unique',sub_key='',date=d, \
                                                table_name='raw_data_shabik_360',db_conn=None)
        temp_active_user_set &= new_user_set
        for u in temp_active_user_set:
            new_user_to_last_login_date_dict[u]=d

    ever_active_user_set=set(new_user_to_creation_date_dict.keys()) & set(new_user_to_last_login_date_dict.keys())

    key='daily_ever_active_new_user_unique'
    helper_mysql.put_collection(collection=ever_active_user_set,oem_name=oem_name,category=category,key=key, \
                                sub_key=marketing_key, table_name='raw_data_shabik_360', \
                                date=end_observation_date,created_on=None,db_conn=None)
    






    ## different user types

    # get inactive user
    
    inactive_user_dict=dict((u,last_login) for u,last_login in new_user_to_last_login_date_dict.iteritems() \
                            if helper_regex.get_day_diff_from_date_str(end_observation_date,last_login)>=inactive_day_threshold)

    key='daily_inactive_user_unique'
    helper_mysql.put_collection(collection=inactive_user_dict.keys(),oem_name=oem_name,category=category,key=key, \
                                sub_key=marketing_key, table_name='raw_data_shabik_360', \
                                date=end_observation_date,created_on=None,db_conn=None)

    # get in-sub inactive user

    in_sub_inactive_user_dict=common_shabik_360.check_user_id_in_sub_status(inactive_user_dict.keys())
    in_sub_inactive_user_set=set([user_id for user_id,sub_status in in_sub_inactive_user_dict.iteritems()  \
                                  if sub_status==False]) # False for in-sub

    key='daily_in_sub_inactive_user_unique'
    helper_mysql.put_collection(collection=in_sub_inactive_user_set,oem_name=oem_name,category=category,key=key, \
                                sub_key=marketing_key, table_name='raw_data_shabik_360', \
                                date=end_observation_date,created_on=None,db_conn=None)
    
    # get unsubb'ed inactive user

    unsubbed_inactive_user_set=set([user_id for user_id,sub_status in in_sub_inactive_user_dict.iteritems() \
                                    if sub_status==True]) # True for unsubb'ed

    key='daily_unsubbed_inactive_user_unique'
    helper_mysql.put_collection(collection=unsubbed_inactive_user_set,oem_name=oem_name,category=category,key=key, \
                                sub_key=marketing_key, table_name='raw_data_shabik_360', \
                                date=end_observation_date,created_on=None,db_conn=None)
    
    # get non-sub inactive user

    non_sub_inactive_user_set=set(inactive_user_dict.keys())-in_sub_inactive_user_set-unsubbed_inactive_user_set

    key='daily_non_sub_inactive_user_unique'
    helper_mysql.put_collection(collection=non_sub_inactive_user_set,oem_name=oem_name,category=category,key=key, \
                                sub_key=marketing_key, table_name='raw_data_shabik_360', \
                                date=end_observation_date,created_on=None,db_conn=None)


    # active still user

    still_active_user_dict =dict((u,last_login) for u,last_login in new_user_to_last_login_date_dict.iteritems() \
                            if helper_regex.get_day_diff_from_date_str(end_observation_date,last_login)<inactive_day_threshold)

    key='daily_still_active_new_user_unique'
    helper_mysql.put_collection(collection=still_active_user_dict.keys(),oem_name=oem_name,category=category,key=key, \
                                sub_key=marketing_key, table_name='raw_data_shabik_360', \
                                date=end_observation_date,created_on=None,db_conn=None)
    


    



    ## calculate user behavior favor


    target_user_group_name_to_user_set_dict={
        'in_sub_inactive_user':in_sub_inactive_user_set,
        'inactive_user':inactive_user_dict.keys(),
        'still_active_user':still_active_user_dict.keys(),
    }

    
    for user_group_name,target_user_set in target_user_group_name_to_user_set_dict.iteritems():
        
        category='analysis_'+user_group_name

        # get online days of tareget user

        target_user_to_online_days_dict={}
        for d in helper_regex.date_iterator(begin_observation_date,end_observation_date,step=1):
            temp_active_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                                    key='app_page_daily_visitor_unique',sub_key='',date=d, \
                                                    table_name='raw_data_shabik_360',db_conn=None)
            
            for u in target_user_set:
                if u in temp_active_user_set:
                    target_user_to_online_days_dict[u]=target_user_to_online_days_dict.get(u,0)+1
                
        day_ranges=((1,2),(3,7),(8,14),(15,10000))    
        
        for day_range in day_ranges:
            temp_user_set=set([u for u,days in target_user_to_online_days_dict.iteritems() \
                               if days>=day_range[0] and days<=day_range[1]])
            key='daily_by_online_days_'+user_group_name+'_unique'
            sub_key=marketing_key+'_'+str(day_range)
            helper_mysql.put_collection(collection=temp_user_set,oem_name=oem_name,category=category,key=key, \
                                    sub_key=sub_key, table_name='raw_data_shabik_360', \
                                    date=end_observation_date,created_on=None,db_conn=None)


        # get app active percentage by day of in-sub inactive user

        target_apps=(
            ('friend_profile','raw_data_shabik_360','Shabik_360','moagent','app_page_by_app_daily_visitor_unique','friend'),    
            ('mochat','raw_data_shabik_360','All','mochat','only_shabik_360_send_msg_text_daily_monet_id_unique',''),    
            ('chatroom','raw_data_shabik_360','Shabik_360','chatroom','only_shabik_360_send_msg_daily_monet_id_unique',''),    
            ('private_message','raw_data_shabik_360','Shabik_360','moagent','app_page_by_app_daily_visitor_unique','message'),    
            ('photo','raw_data_shabik_360','Shabik_360','moagent','app_page_by_app_daily_visitor_unique','photo'),    
            ('ocean_age','raw_data_shabik_360','Shabik_360','moagent','app_page_by_app_daily_visitor_unique','ocean_age'),    
            ('im','raw_data_shabik_360','All','im','only_shabik_360_user_send_msg_daily_monet_id_unique',''),    
            ('circle','raw_data_shabik_360','Shabik_360','moagent','app_page_by_app_daily_visitor_unique','circle'),    
            ('happy_barn','raw_data_shabik_360','Shabik_360','moagent','app_page_by_app_daily_visitor_unique','happy_barn'),    
            ('nearby','raw_data_shabik_360','Shabik_360','moagent','app_page_by_app_daily_visitor_unique','location'),    
        )

        target_user_preference_dict={}

        for app in target_apps:
            
            app_name=app[0]
            data_existence_counter=0

            for d in helper_regex.date_iterator(begin_observation_date,end_observation_date,step=1):
                
                temp_active_user_in_app_dict=helper_mysql.get_dict_of_raw_collection_from_key(oem_name=app[2],category=app[3], \
                                                        key=app[4],sub_key=app[5],date=d, \
                                                        table_name=app[1],db_conn=None)
                
                data_existence_counter+=len(temp_active_user_in_app_dict)

                for u in target_user_to_online_days_dict.keys():
                    if temp_active_user_in_app_dict.has_key(u):
                        target_user_preference_dict.setdefault(u,{})
                        target_user_preference_dict[u].setdefault(app_name,[0,0,])
                        temp=target_user_preference_dict[u][app_name]
                        temp[0]+=1
                        temp[1]+=temp_active_user_in_app_dict[u]

            if data_existence_counter==0:
                raise Error(app+' key error!')
                exit()
            
        #print target_user_preference_dict

        sorted_target_user_preference_dict=dict((u,sorted([(v[0],v[1],app) for app,v in preference_dict.iteritems()],reverse=True)) \
                                    for u,preference_dict in target_user_preference_dict.iteritems())

        print sorted_target_user_preference_dict

        target_user_preference_by_app={}

        for u,p in sorted_target_user_preference_dict.iteritems():
            top_1_app=p[0][2]
            target_user_preference_by_app.setdefault(top_1_app,[])
            target_user_preference_by_app[top_1_app].append(u)

        for app,unique_user_set in target_user_preference_by_app.iteritems():
            print app,unique_user_set
            key='daily_ranking_by_online_days_in_app_'+user_group_name+'_unique'
            sub_key=marketing_key+'_'+app
            helper_mysql.put_collection(collection=unique_user_set,oem_name=oem_name,category=category,key=key, \
                                    sub_key=sub_key, table_name='raw_data_shabik_360', \
                                    date=end_observation_date,created_on=None,db_conn=None)




if __name__=='__main__':
    

    """
    begin_creation_date='2012-03-01'
    end_creation_date='2012-04-11'

    begin_observation_date='2012-03-01'
    #end_observation_date=helper_regex.date_add(helper_regex.get_date_str_now(),-1)

    inactive_day_threshold=14
    marketing_sign=''

    #for i in range(config.day_to_update_stat,0,-1):
    for i in range(1,2):
        my_date=time.time()-3600*24*i
        end_observation_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
        stat_user_behavior(begin_creation_date,end_creation_date, \
                           begin_observation_date,end_observation_date,inactive_day_threshold, \
                           marketing_key='mar_1_apr_11')
    """

    begin_creation_date='2012-03-15'
    end_creation_date='2012-03-16'

    begin_observation_date='2012-03-15'
    #end_observation_date=helper_regex.date_add(helper_regex.get_date_str_now(),-1)

    inactive_day_threshold=7

    for i in range(50,51):
        my_date=time.time()-3600*24*i
        end_observation_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
        stat_user_behavior(begin_creation_date,end_creation_date, \
                           begin_observation_date,end_observation_date,inactive_day_threshold, \
                           marketing_key='mar_1_test_apr_11')
    
    pass
