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


def export(date_length=30):
    

    user_login_history={}
    user_last_login_date={}



    today=helper_regex.date_add(helper_regex.get_date_str_now(),-17)

    start_time=helper_regex.date_add(today,-date_length)+' 05:00:00'
    end_time=helper_regex.date_add(today,-1)+' 05:00:00'



    # user_id -> msisdn

    sql=r'''

    SELECT [user_id],replace([user_name],'@shabik.com','') as msisdn
    FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [creationDate]>='%s' and [creationDate]<'%s'
    and user_name like '%%shabik.com%%'

    ''' % (start_time,end_time)

    user_id_to_msisdn=helper_sql_server.fetch_dict(conn_config=config.conn_stc,sql=sql)



    # new user user_id

    new_user_collection=user_id_to_msisdn.keys()
    new_user_collection=set([str(user_id) for user_id in new_user_collection])



    # subscription status

    sql=r'''


    select distinct '0'+replace(msisdn,'+966','')+'@shabik.com' as [user_name]
    into #tmp
    from db86.shabik_mt.dbo.accounts with(nolock)
    where 
    is_deleted=0


    SELECT [user_id]
    FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [creationDate]>='%s' and [creationDate]<'%s'
    and user_name like '%%shabik.com%%'
    and user_name in (
		select user_name
		from #tmp
    )

    drop table #tmp

    ''' % (start_time,end_time)

    user_id_in_sub=helper_sql_server.fetch_set(conn_config=config.conn_stc,sql=sql)
    user_id_in_sub=set([str(user_id) for user_id in user_id_in_sub])



    for i in range(date_length,-17,-1):
        
        date_temp=helper_regex.date_add(today,-i)
        
        shabik_5_collection=helper_mysql.get_raw_collection_from_key(oem_name='STC',category='moagent', \
                                        key='app_page_only_shabik_5_daily_visitor_unique',sub_key='', \
                                        date=date_temp,table_name='raw_data',db_conn=None)

        shabik_5_collection=shabik_5_collection & new_user_collection

        for user_id in shabik_5_collection:
            user_login_history.setdefault(user_id,'')
            user_login_history[user_id]+='5'

            user_last_login_date.setdefault(user_id,'')
            user_last_login_date[user_id]=date_temp
            
        shabik_360_collection=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                        key='app_page_daily_visitor_unique',sub_key='', \
                                        date=date_temp,table_name='raw_data_shabik_360',db_conn=None)

        shabik_360_collection=shabik_360_collection & new_user_collection

        for user_id in shabik_360_collection:
            user_login_history.setdefault(user_id,'')
            user_login_history[user_id]+='6'

            user_last_login_date.setdefault(user_id,'')
            user_last_login_date[user_id]=date_temp


        


    #calculate

    """
    target_groups_names=[
        '1.More than 2 weeks users using Shabik 360 (Totally New User to Shabik) [only using 360]',
        '2.Users who Shifted from Shabik360 to Shabik 5 [for each at least using 3 days, still in sub]',
        '3.Unsubscribed users of Shabik 360 [last using 360 for >=7 days and then unsub]',
        '4.Users who uses Shabik 5 more than 2 weeks [actually is online for >=14 days]',
        '5.Users who shifted from Shabik 5 to Shabik 360 [for each at least using 3 days, still in sub]',
        '6.User base of new user in last 50 days, which is used to generate above lists',
    ]

    target_groups=[
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'^(6{14,})$')],
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'(6{3,}5{3,}$)')],
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'(6{7,}$)') and user_id in user_id_in_sub],
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'(5{14,}$)')],
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'(5{3,}6{3,}$)') and user_id in user_id_in_sub],
        [user_id for user_id,sequence in user_login_history.iteritems()],
    ]

    target_groups_names={
        'User only use Shabik 360':
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'^(6+)$')],
        'User only use Shabik 360 [more than 10d]':
        ,
        'User only use Shabik 5',
        'User only use Shabik 5 [more than 10d]',
        'User use both Shabik 360 / Shabik 5',
        'User used both and choosed Shabik 5 [recently used only Shabik 5 for 5d]',
        'User used both and choosed Shabik 5 [recently used only Shabik 360 for 5d]',
    }

    target_groups=[
        
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'^(6{10,})$')],
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'^(5+)$')],
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'^(5{10,})$')],
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'(56|65)')],
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'(56|65)') and  helper_regex.extract(sequence,r'(5{5,})$')],
        [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'(56|65)') and  helper_regex.extract(sequence,r'(6{5,})$')],
    ]
    """

    threshold_of_settle_down='5'

    target_groups={
        '1.new_user':
            [user_id for user_id,sequence in user_login_history.iteritems()],
        '2.new_user_start_from_5':
            [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'^(5)')],
        '3.new_user_start_from_360':
            [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'^(6)')],
        '4.new_user_only_5':
            [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'^(5+)$')],
        '5.new_user_only_360':
            [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'^(6+)$')],
        '6.new_user_both':
            [user_id for user_id,sequence in user_login_history.iteritems() if helper_regex.extract(sequence,r'(56|65)')],
        '7.new_user_both_and_finally_5':
            [user_id for user_id,sequence in user_login_history.iteritems() 
            if helper_regex.extract(sequence,r'(56|65)') and helper_regex.extract(sequence,'(5{'+threshold_of_settle_down+',})$')],
        '8.new_user_both_and_finally_360':
            [user_id for user_id,sequence in user_login_history.iteritems() 
            if helper_regex.extract(sequence,r'(56|65)') and helper_regex.extract(sequence,'(6{'+threshold_of_settle_down+',})$')],
        '9.new_user_both_and_not_stable':
            [user_id for user_id,sequence in user_login_history.iteritems() 
            if helper_regex.extract(sequence,r'(56|65)') 
            and not helper_regex.extract(sequence,'(5{'+threshold_of_settle_down+',})$') 
            and not helper_regex.extract(sequence,'(6{'+threshold_of_settle_down+',})$')],
    }

    #export

    keys=sorted(target_groups.keys())

    for key in keys:

        user_id_collection=target_groups[key]
        print key
        print 'size:',len(user_id_collection)
        
        print '[last login date - msisdn - sub status - login history]'
        
        user_id_collection.sort(key=lambda user_id:user_last_login_date[user_id],reverse=True)
        for user_id in user_id_collection:
            print user_last_login_date[user_id],'\t',user_id_to_msisdn[user_id],'\t','sub' if user_id in user_id_in_sub else 'unsub','\t',user_login_history[user_id]


    for key in keys:

        user_id_collection=target_groups[key]
        print '==',key,'=='
        print 'size:',len(user_id_collection)
        print 'unsub:',len([user_id for user_id in user_id_collection if not user_id in user_id_in_sub])
        
        """
        print '[last login date - msisdn - sub status - login history]'
        
        user_id_collection.sort(key=lambda user_id:user_last_login_date[user_id],reverse=True)
        for user_id in user_id_collection:
            print user_last_login_date[user_id],'\t',user_id_to_msisdn[user_id],'\t','sub' if user_id in user_id_in_sub else 'unsub','\t',user_login_history[user_id]
        """    
    




if __name__=='__main__':

    export()

