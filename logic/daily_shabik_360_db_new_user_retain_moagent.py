import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
from datetime import date, datetime, timedelta


config.collection_cache_enabled=True


def prepare():

    global temp_first_access_date,temp_last_access_date,temp_profile_create_date, \
    temp_profile_last_login_date,date_min,date_max,dict_date_to_collection_id
    



    # load first access / last access time from 360 moagent, profile creation time

    temp_first_access_date={}
    temp_last_access_date={}
    temp_profile_create_date={} # from db
    temp_profile_last_login_date={} # from db

    dict_date_to_collection_id=helper_mysql.fetch_dict(sql=r"""
    
    select date,`value`
    from `raw_data_shabik_360`
    where `oem_name`='Shabik_360' 
    and category='moagent' 
    and `key`='app_page_daily_visitor_unique_collection_id'
    
    """)

    dates=dict_date_to_collection_id.keys()
    date_min=min(dates)
    date_max=max(dates)





    #fetch temp_profile_create_date

    rows_profile=helper_sql_server.fetch_rows_dict(conn_config=config.conn_stc,sql=r"""
    
    select user_id,creationDate
    from mozone_user.dbo.profile with(nolock)
    where creationDate>='%s'

    """ % (date_min + ' 00:00:00',))

    for user_id,row in rows_profile.iteritems():
        if row['creationDate']:        
            temp_profile_create_date[user_id]=(row['creationDate']+timedelta(hours=-5)).strftime("%Y-%m-%d")




    #fetch temp_profile_last_login_date

    rows_profile=helper_sql_server.fetch_rows_dict(conn_config=config.conn_stc,sql=r"""
    
    select user_id,lastLogin
    from mozone_user.dbo.profile with(nolock)
    where creationDate>='%s'

    """ % (date_min + ' 00:00:00',))

    for user_id,row in rows_profile.iteritems():
        if row['lastLogin']:        
            temp_profile_last_login_date[user_id]=(row['lastLogin']+timedelta(hours=-5)).strftime("%Y-%m-%d")




    #fetch temp_first_access_date,temp_last_access_date

    for i in range(1,10000):

        current_date=helper_regex.date_add(date_min,i)
        print current_date

        if current_date>date_max:
            break
        if not dict_date_to_collection_id.has_key(current_date):
            continue
        
        print dict_date_to_collection_id[current_date]


        #get daily active collection

        moagent_access_collection=helper_mysql.get_raw_collection_by_id(dict_date_to_collection_id[current_date])
        print 'moagent_access_collection:',len(moagent_access_collection)

        if not moagent_access_collection:
            continue
        
        for monet_id in moagent_access_collection:
            if not temp_first_access_date.has_key(monet_id):
                temp_first_access_date[monet_id]=current_date
                temp_last_access_date[monet_id]=current_date
        
            if temp_first_access_date[monet_id]>current_date:
                temp_first_access_date[monet_id]=current_date

            if temp_last_access_date[monet_id]<current_date:
                temp_last_access_date[monet_id]=current_date

    
    print 'valid temp_first_access_date:',len(temp_first_access_date)
    print 'valid temp_last_access_date:',len(temp_last_access_date)
    print 'valid temp_profile_create_date:',len(set(temp_profile_create_date.keys()) & set(temp_first_access_date.keys()))
    print 'valid temp_profile_last_login_date:',len(set(temp_profile_last_login_date.keys()) & set(temp_last_access_date.keys()))

    #_dump([i for i in temp_first_access_date.keys() if temp_profile_last_login_date.has_key(i) and temp_last_access_date[i]<temp_profile_last_login_date[i]])
    #_dump([i for i in temp_first_access_date.keys()])
    #exit()


def _dump(user_ids):
    global temp_first_access_date,temp_last_access_date,temp_profile_create_date, \
    temp_profile_last_login_date,date_min,date_max,dict_date_to_collection_id
    
    print "===="
    
    for user_id in user_ids:
        print user_id, \
        '\t',temp_profile_create_date[user_id] if temp_profile_create_date.has_key(user_id) else "----------", \
        '\t',temp_first_access_date[user_id] if temp_first_access_date.has_key(user_id) else "----------", \
        '\t',temp_last_access_date[user_id] if temp_last_access_date.has_key(user_id) else "----------", \
        '\t',temp_profile_last_login_date[user_id] if temp_profile_last_login_date.has_key(user_id) else "----------"
        
        
        
        
    

def stat_login(my_date):

    global temp_first_access_date,temp_last_access_date,temp_profile_create_date, \
    temp_profile_last_login_date,date_min,date_max,dict_date_to_collection_id

    oem_name='Shabik_360'
    stat_category='login_retain'
    db_name='raw_data_test'

    date_min=helper_regex.date_add(date_max,-30)

    for i in range(1,10000):

        current_date=helper_regex.date_add(date_min,i)
        print 'current date:',current_date
        
        if current_date>date_max:
            break
        
        # prepare user bases by different conditions

        new_users_total=set(user_id for user_id,first_access_date in temp_first_access_date.iteritems() \
                        if first_access_date==current_date)
        
        shabik_5_users_0d=helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_only_shabik_5_daily_visitor_unique',sub_key='', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data',db_conn=None)
        shabik_5_users_1d=helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_only_shabik_5_daily_visitor_unique',sub_key='', \
                                    date=helper_regex.date_add(current_date,1),table_name='raw_data',db_conn=None)
        shabik_5_users_2d=helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_only_shabik_5_daily_visitor_unique',sub_key='', \
                                    date=helper_regex.date_add(current_date,2),table_name='raw_data',db_conn=None)
        shabik_5_users_3d=helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                    key='app_page_only_shabik_5_daily_visitor_unique',sub_key='', \
                                    date=helper_regex.date_add(current_date,3),table_name='raw_data',db_conn=None)

        new_users_from_old=set(user_id for user_id in new_users_total \
                        if temp_profile_create_date.has_key(user_id) \
                        and temp_profile_create_date[user_id]<temp_first_access_date[user_id] \
                        or not temp_profile_create_date.has_key(user_id))

        new_users_fresh=new_users_total-new_users_from_old
        
        if shabik_5_users_0d:
            new_users_filtered_shabik_5_0d=new_users_fresh-shabik_5_users_0d
        else:
            new_users_filtered_shabik_5_0d=set([])
        
        if shabik_5_users_0d and shabik_5_users_1d and shabik_5_users_2d and shabik_5_users_3d:
            new_users_filtered_shabik_5_3d=new_users_fresh-shabik_5_users_0d-shabik_5_users_1d-shabik_5_users_2d-shabik_5_users_3d
        else:
            new_users_filtered_shabik_5_3d=set([])

        new_users_fresh_mod_3_0=set(user_id for user_id in new_users_total \
                        if int(user_id) % 3==0 and int(user_id)>40012604)
        new_users_fresh_mod_3_1=set(user_id for user_id in new_users_total \
                        if int(user_id) % 3==1 and int(user_id)>40012604)
        new_users_fresh_mod_3_2=set(user_id for user_id in new_users_total \
                        if int(user_id) % 3==2 and int(user_id)>40012604)

        client_jme_users=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique',sub_key='JME', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)

        client_symbian_users=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique',sub_key='S60', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)

        client_android_users=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique',sub_key='Android', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)

        client_ios_users=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique',sub_key='iOS', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)

        client_blackberry_users=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_by_morange_version_type_daily_user_unique',sub_key='BlackBerry', \
                                    date=helper_regex.date_add(current_date,0),table_name='raw_data_shabik_360',db_conn=None)

        new_users_fresh_jme=new_users_fresh & client_jme_users
        new_users_fresh_symbian=new_users_fresh & client_symbian_users
        new_users_fresh_android=new_users_fresh & client_android_users
        new_users_fresh_ios=new_users_fresh & client_ios_users
        new_users_fresh_blackberry=new_users_fresh & client_blackberry_users

        client_jme_fresh_users_mod_3_0=set([user_id for user_id in new_users_fresh_jme if int(user_id) % 3 == 0])
        client_jme_fresh_users_mod_3_1=set([user_id for user_id in new_users_fresh_jme if int(user_id) % 3 == 1])
        client_jme_fresh_users_mod_3_2=set([user_id for user_id in new_users_fresh_jme if int(user_id) % 3 == 2])

        client_symbian_fresh_users_mod_3_0=set([user_id for user_id in new_users_fresh_symbian if int(user_id) % 3 == 0])
        client_symbian_fresh_users_mod_3_1=set([user_id for user_id in new_users_fresh_symbian if int(user_id) % 3 == 1])
        client_symbian_fresh_users_mod_3_2=set([user_id for user_id in new_users_fresh_symbian if int(user_id) % 3 == 2])


        if current_date=='2011-12-18':
            fresh_user_6pm=helper_sql_server.fetch_set(conn_config=config.conn_stc,sql=r"""
    
            select user_id
            from mozone_user.dbo.profile with(nolock)
            where 
            creationDate>='2011-12-18 18:00:00'
            and creationDate<'2011-12-19 05:00:00'

            """)
            
            fresh_user_6pm=set([str(user_id) for user_id in fresh_user_6pm])

            client_jme_fresh_users_mod_3_0=client_jme_fresh_users_mod_3_0 & fresh_user_6pm
            client_jme_fresh_users_mod_3_1=client_jme_fresh_users_mod_3_1 & fresh_user_6pm
            client_jme_fresh_users_mod_3_2=client_jme_fresh_users_mod_3_2 & fresh_user_6pm

            client_symbian_fresh_users_mod_3_0=client_symbian_fresh_users_mod_3_0 & fresh_user_6pm
            client_symbian_fresh_users_mod_3_1=client_symbian_fresh_users_mod_3_1 & fresh_user_6pm
            client_symbian_fresh_users_mod_3_2=client_symbian_fresh_users_mod_3_2 & fresh_user_6pm

        # init na
        # init quick
        # init medium
        # init slow
        # init problematic

        if current_date=='2011-11-30':
            group_init_1=set(['40113533','40113556','40113565','40113567','40113745','40113869','40113886','40113938','40113945','40113981','40113994','40114039','40114122','40114159','40114168','40114240','40114396','40114448','40114479','40114488','40114512','40114636','40114832','40115198'])
            group_init_2=set(['40113539','40113782','40114072','40114100','40114310','40115299','40113734','40113852','40114063','40114155','40114364','40114381','40114413','40114420','40114487','40115114','40115141','40115191','40115223','40113616','40113769','40113788','40113816','40114030','40114110','40114180','40114247','40114268','40114317','40114325','40114354','40114431','40114768','40114891','40115164','40115219','40115286','40115308','40113797','40114085','40114297','40114478','40114548','40114803','40113937','40114568','40114266','40115089','40114010','40114854','40114484','40113798','40113521','40114028','40113712','40115109','40113864'])
            group_init_3=set(['40113659','40114143','40113771','40115225','40114101','40115096','40115032','40114440','40114469','40114879','40113919','40114535','40113818','40114462','40113555','40114234','40113476','40114195','40114577','40113783','40113963','40113986','40114876','40115260','40113952','40115091','40113851','40114171','40115253','40113719'])
            group_init_4=set(['40113645','40113826','40115296','40113900','40115304','40114591','40114073','40115021','40114157','40113759','40114523','40115272','40114027','40114006','40115281','40115276','40113730','40114270','40114445','40114119','40114672','40114433','40114166','40114493'])
            group_init_5=set(['40113962','40114410','40114515','40114156','40113492','40113677','40113604','40113608','40113643','40113711','40113976'])
        else:
            group_init_1=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent',key='new_user_by_init_time_unique',sub_key='lt0',date=current_date,table_name='raw_data_shabik_360',db_conn=None)
            group_init_2=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent',key='new_user_by_init_time_unique',sub_key='ge0_lt30',date=current_date,table_name='raw_data_shabik_360',db_conn=None)
            group_init_3=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent',key='new_user_by_init_time_unique',sub_key='ge30_lt120',date=current_date,table_name='raw_data_shabik_360',db_conn=None)
            group_init_4=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent',key='new_user_by_init_time_unique',sub_key='ge120_lt1000',date=current_date,table_name='raw_data_shabik_360',db_conn=None)
            group_init_5=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent',key='new_user_by_init_time_unique',sub_key='ge1000',date=current_date,table_name='raw_data_shabik_360',db_conn=None)

        new_users_fresh_init_1 = new_users_fresh & group_init_1
        new_users_fresh_init_2 = new_users_fresh & group_init_2
        new_users_fresh_init_3 = new_users_fresh & group_init_3
        new_users_fresh_init_4 = new_users_fresh & group_init_4
        new_users_fresh_init_5 = new_users_fresh & group_init_5


        # calculate

        base_user_sets={
            'total':new_users_total,
            'from_old':new_users_from_old,
            'fresh':new_users_fresh,
            'filtered_shabik_5_0d':new_users_filtered_shabik_5_0d,
            'filtered_shabik_5_3d':new_users_filtered_shabik_5_3d,
            'fresh_mod_3_0':new_users_fresh_mod_3_0,
            'fresh_mod_3_1':new_users_fresh_mod_3_1,
            'fresh_mod_3_2':new_users_fresh_mod_3_2,
            'fresh_jme':new_users_fresh_jme,
            'fresh_symbian':new_users_fresh_symbian,
            'fresh_android':new_users_fresh_android,
            'fresh_ios':new_users_fresh_ios,
            'fresh_blackberry':new_users_fresh_blackberry,
            'init_1':new_users_fresh_init_1,
            'init_2':new_users_fresh_init_2,
            'init_3':new_users_fresh_init_3,
            'init_4':new_users_fresh_init_4,
            'init_5':new_users_fresh_init_5,

            'jm30':client_jme_fresh_users_mod_3_0,
            'jm31':client_jme_fresh_users_mod_3_1,
            'jm32':client_jme_fresh_users_mod_3_2,
            'jm3_12':client_jme_fresh_users_mod_3_1 | client_jme_fresh_users_mod_3_2,

            'sm30':client_symbian_fresh_users_mod_3_0,
            'sm31':client_symbian_fresh_users_mod_3_1,
            'sm32':client_symbian_fresh_users_mod_3_2,
            'sm3_12':client_symbian_fresh_users_mod_3_1 | client_symbian_fresh_users_mod_3_2,
        }


        for k,user_set in base_user_sets.iteritems():
            
            # calculate total

            print 'user base of',k,':',len(user_set)

            key='daily_new_360_user_initial_%s_unique' % (k,)
            helper_mysql.put_raw_data(oem_name,stat_category,key,'',len(user_set),db_name,current_date)
            
            helper_mysql.put_collection(collection=user_set,oem_name=oem_name,category=stat_category, \
                                    key=key,sub_key='',date=current_date,table_name=db_name)

            # calculate unsub

            unsubbed_user_count=helper_sql_server.fetch_scalar_int(config.conn_mt,sql=r'''
                select count(distinct msisdn)
                from stc_integral.dbo.accounts with(nolock)
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
    
            key='daily_new_360_user_unsub_%s_unique' % (k,)
            helper_mysql.put_raw_data(oem_name,stat_category,key,'',unsubbed_user_count,db_name,current_date)
            
            #fetch set
            #helper_mysql.put_collection(collection=unsubbed_user,oem_name=oem_name,category=stat_category, \
            #                        key=key,sub_key='',date=current_date,table_name=db_name)
        


            # calculate switch to old / lost

            new_users_switched_back=set(user_id for user_id in user_set \
                                    if temp_profile_last_login_date.has_key(user_id) \
                                    and temp_profile_last_login_date[user_id]>helper_regex.date_add(temp_last_access_date[user_id],1))

            print 'switched user:',len(new_users_switched_back)

            key='daily_new_360_user_switced_back_%s_unique' % (k,)
            helper_mysql.put_raw_data(oem_name,stat_category,key,'',len(new_users_switched_back),db_name,current_date)
            
            #helper_mysql.put_collection(collection=new_users_switched_back,oem_name=oem_name,category=stat_category, \
            #                        key=key,sub_key='',date=current_date,table_name=db_name)


        # calculate continuous 3 day's login

        step=3
        for i in range(0,56):

            logined_user=set([])

            for day_delta in range(i,i+step):
                target_date=helper_regex.date_add(current_date,day_delta)
                if dict_date_to_collection_id.has_key(target_date):
                    collection=helper_mysql.get_raw_collection_by_id(dict_date_to_collection_id[target_date])
                    logined_user=logined_user | collection
            
            for k,user_set in base_user_sets.iteritems():

                new_user_logined_user= user_set & logined_user
                key='daily_new_360_user_3_day_logined_%s_unique' % (k,)
                helper_mysql.put_raw_data(oem_name,stat_category,key,i,len(new_user_logined_user),db_name,current_date)
                
                #helper_mysql.put_collection(collection=new_user_logined_user,oem_name=oem_name,category=stat_category, \
                #                    key=key,sub_key=i,date=current_date,table_name=db_name)
           
        # calculate continuous 1 day's login

        step=1
        for i in range(0,10):

            logined_user=set([])

            for day_delta in range(i,i+step):
                target_date=helper_regex.date_add(current_date,day_delta)
                if dict_date_to_collection_id.has_key(target_date):
                    collection=helper_mysql.get_raw_collection_by_id(dict_date_to_collection_id[target_date])
                    logined_user=logined_user | collection
 
            for k,user_set in base_user_sets.iteritems():

                new_user_logined_user= user_set & logined_user
                key='daily_new_360_user_1_day_logined_%s_unique' % (k,)
                helper_mysql.put_raw_data(oem_name,stat_category,key,i,len(new_user_logined_user),db_name,current_date)
                
                #helper_mysql.put_collection(collection=new_user_logined_user,oem_name=oem_name,category=stat_category, \
                #                    key=key,sub_key=i,date=current_date,table_name=db_name)


    return



if __name__=='__main__':

    prepare()

    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat_login(my_date)

