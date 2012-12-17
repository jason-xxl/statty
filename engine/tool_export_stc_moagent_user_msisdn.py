import helper_sql_server
import helper_mysql
import helper_regex
import config


def export():

    today=helper_regex.date_add(helper_regex.get_date_str_now(),-1)

    # new user stc

    start_time=helper_regex.date_add(today,-30)+' 05:00:00'
    end_time=helper_regex.date_add(today,-1)+' 05:00:00'

    sql=r'''

    SELECT [user_id],phone
    FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [creationDate]>='%s' and [creationDate]<'%s'
    and user_name like '%%shabik.com%%'
    and phone not like '+966%%'
    and phone<>''

    ''' % (start_time,end_time)

    new_user_msisdn_dict=helper_sql_server.fetch_dict(conn_config=config.conn_stc,sql=sql)
    new_user_msisdn_dict=dict((str(i),j) for i,j in new_user_msisdn_dict.iteritems())
    print len(new_user_msisdn_dict)



    # old user stc

    date_length=30
    start_time=helper_regex.date_add(today,-90)+' 05:00:00'
    end_time=helper_regex.date_add(today,-30)+' 05:00:00'

    sql=r'''

    SELECT [user_id],phone
    FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [creationDate]>='%s' and [creationDate]<'%s'
    and user_name like '%%shabik.com%%'
    and phone not like '+966%%'
    and phone<>''

    ''' % (start_time,end_time)

    old_user_msisdn_dict=helper_sql_server.fetch_dict(conn_config=config.conn_stc,sql=sql)
    old_user_msisdn_dict=dict((str(i),j) for i,j in old_user_msisdn_dict.iteritems())
    print len(old_user_msisdn_dict)



    # daily active user set
    
    date_temp=helper_regex.date_add(today,-1)

    target_sets={

        'JME':helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                        key='app_page_by_morange_version_type_daily_user_unique',sub_key='JME', \
                                        date=date_temp,table_name='raw_data_shabik_360',db_conn=None),
    
        'S60-3':helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                        key='app_page_by_morange_version_type_daily_user_unique',sub_key='S60-3', \
                                        date=date_temp,table_name='raw_data_shabik_360',db_conn=None),
    
        'S60-5':helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                        key='app_page_by_morange_version_type_daily_user_unique',sub_key='S60-5', \
                                        date=date_temp,table_name='raw_data_shabik_360',db_conn=None),
    
        'Android':helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                        key='app_page_by_morange_version_type_daily_user_unique',sub_key='Android', \
                                        date=date_temp,table_name='raw_data_shabik_360',db_conn=None),
    
        'iOS':helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                        key='app_page_by_morange_version_type_daily_user_unique',sub_key='iOS', \
                                        date=date_temp,table_name='raw_data_shabik_360',db_conn=None),
    
        'BlackBerry':helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                        key='app_page_by_morange_version_type_daily_user_unique',sub_key='BlackBerry', \
                                        date=date_temp,table_name='raw_data_shabik_360',db_conn=None),
    
        'All Client':helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                        key='app_page_daily_visitor_unique',sub_key='', \
                                        date=date_temp,table_name='raw_data_shabik_360',db_conn=None),
    
    }



    for k,total_active_collection in target_sets.iteritems():
        
        old_user_msisdn_set=set([msisdn for user_id,msisdn in old_user_msisdn_dict.iteritems() if user_id in total_active_collection])
        new_user_msisdn_set=set([msisdn for user_id,msisdn in new_user_msisdn_dict.iteritems() if user_id in total_active_collection])


        print
        print '## Non-STC Old Users',k
        for msisdn in list(old_user_msisdn_set)[0:300]:
            if len(msisdn)>10:
                print msisdn
        
        
        print    
        print '## Non-STC New Users',k
        for msisdn in list(new_user_msisdn_set)[0:300]:
            if len(msisdn)>10:
                print msisdn
        

if __name__=='__main__':

    export()

