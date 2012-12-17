import helper_sql_server
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import config
import helper_regex 
import helper_mysql
import common_shabik_360

helper_mysql.quick_insert=True


def stat_moagent_wap(my_date):
    
    # INFO 2010-04-08 00:00:00 - [          workThread] (        CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    helper_mysql.clear_raw_data_space(oem_name='Shabik_360',category='moagent_wap_test',key=None,sub_key=None,date=current_date,table_name='raw_data_shabik_360',db_conn=None)

    oem_name='Shabik_360' 
    stat_category='moagent_wap_test'

    stat_plan_prepare=Stat_plan()

    #01 Apr 00:00:41,556 - 13373042298479	16	16	0	-1	http://Moweb-stc.morange.com/mobile_login_poll.ashx?action=fast&auth_id=6285124&unique_id=13372997707361a6.4.0.120329&monetid=10000&cli_ip=212.118.140.28

    #01 Apr 00:01:41,024 - 133640166371425	16	16	0	0	http://moweb-stc-py.morange.com/login.aspx?monetid=10000&moclientwidth=240&userAgent=Nokia2700c-2-09.97+JConf%2FCLDC-1.1+JProf%2FMIDP-2.1+Encoding%2FISO-8859-1+Locale%2Far+Lang%2Far+Caps%2F7516+Morange%2F6.4.0.120329+Domain%2F%40shabik.com+CJME%2F120329+Source%2Fnull+UniqueId%2F13363999265941&moclientheight=280&devicewidth=240&deviceheight=320&isprefetch=0&cli_ip=84.235.73.208

    #01 Apr 00:01:54,586 - 13332097120433	0	0	0	-1	http://moweb-stc-py.morange.com/first_opening_counter.aspx?profilePage13332085917551a6.4.2.120329&monetid=10000&cli_ip=188.55.81.33

    #01 Apr 00:22:00,922 - 134913576022122	0	0	0	-1	http://moweb-stc-py.morange.com/first_opening_counter.aspx?authId=628752013491355767721a6.4.2.120329&monetid=10000&cli_ip=84.235.73.210

    log_buffer=[]

    def get_version(line):
        if '6.4.0.120521' in line: # and 'CBB' not in line:
            return '6.4.0.120521'
        if '6.4.7.120606' in line: # and 'CBB' not in line:
            return '6.4.7.120606'
        if '6.41.1426' in line:
            return '6.41.1426'
        if '6.42.1406' in line:
            return '6.42.1406'
        return ''

    def get_auth_id(line):
        line_lower=line.lower()
        if '6.41.1426' in line or '6.42.1406' in line:
            auth_id=helper_regex.extract(line,r'auth_id=(\d+)[ignorecase]')
            if not auth_id:
                auth_id_candidate=helper_regex.extract(line,r'http:.*?(\D\d{7,8}\D)')
                if auth_id_candidate:
                    log_buffer.append('get_auth_id:'+auth_id_candidate+'-'+line)
            return auth_id or '0'
        elif '6.4.0.120521' in line or '6.4.7.120606' in line:
            if 'first_opening_counter.aspx?authId=' in line and 'a6.4.' in line: #2.120329
                auth_id=helper_regex.extract(line,r'(authId=\d{7,})')
                line=helper_regex.regex_replace(r'authId=\d{7,}','authId='+auth_id+'&unique_id=',line)
            auth_id=helper_regex.extract(line,r'authId=(\d{7,})')
            if auth_id:
                return auth_id
            auth_id=helper_regex.extract(line,r'auth_id=(\d{7,})')
            if auth_id:
                return auth_id
            auth_id=helper_regex.extract(line,r'authid=(\d{7,})')
            if auth_id:
                return auth_id
            auth_id=helper_regex.extract(line,r'gid=(\d{7,})[ignorecase]')
            if auth_id:
                print 'get_auth_id:gid:',line
                return auth_id
            #if 'login.aspx' not in line and 'login_failed.aspx' not in line:
            auth_id_candidate=helper_regex.extract(line,r'http:.*?(\D\d{7,8}\D)')
            if auth_id_candidate:
                log_buffer.append('get_auth_id:'+auth_id_candidate+'-'+line)
        return '0'

    def get_unique_id(line):
        line_lower=line.lower()
        if '6.41.1426' in line or '6.42.1406' in line:
            unique_id=helper_regex.extract(line,r'PI(?:%2F|%2f|\s*)(\w{30,})').lower()
            if not unique_id:
                log_buffer.append('get_unique_id:'+line)
            return unique_id or '0'
        elif '6.4.0.120521' in line or '6.4.7.120606' in line:
            if 'first_opening_counter.aspx?authId=' in line and 'a6.4.' in line: #2.120329
                auth_id=helper_regex.extract(r'authId=(\d{7,})',line)
                line=helper_regex.regex_replace(r'authId=\d{7,}','authId='+auth_id+'&unique_id=',line)
            unique_id=helper_regex.extract(line,r'unique_id=(\d+)')
            if unique_id:
                return unique_id
            unique_id=helper_regex.extract(line,r'j2meid=(\d+)[ignorecase]')
            if unique_id:
                return unique_id
            unique_id=helper_regex.extract(line,r'UniqueId(?:%2F|%2f)(\d+)')
            if unique_id:
                return unique_id
            unique_id=helper_regex.extract(line,r'[\?&]\w+?(\d{11,14})a[0-9\.]{7,}')
            if unique_id:
                return unique_id

            #unique_id=helper_regex.extract(line,r'(\d{11,14})')
            #if unique_id:
            #    return unique_id
            #if ('6.4.0.120521' in line or '6.4.7.120606' in line or '6.41.1426' in line or '6.42.1406' in line):
            #   pass
                #print 'get_unique_id:',line
            log_buffer.append('get_unique_id:'+line)    
        return '0'
 
        
    def is_correct_date(line):
        line=helper_regex.format_date_time_moagent(line)
        time=helper_regex.extract(line,r'(\d+\-\d+\-\d+ \d+:\d+)')
        if not time:
            return False
        match=current_date in helper_regex.time_add(time+':00',-5.0/24)
        return match

    #t=lambda line:len(get_auth_id(line))>6 and len(get_unique_id(line))>5
    #print t(r'13 Jun 00:37:25,388 - 7735544|8151856	15	15	0	-1	http://Moweb-stc.morange.com/mobile_login_poll.ashx?action=fast&auth_id=10648571&SBUA=%20NOKIA-E5-00%2fUnknown_FVersion%20Encoding%2fUTF-8%20Lang%2far%20Caps%2f7519%20Morange%2f6.4.1%20CS60%2f6.41.1426%20S60%2f30%20PI%2f090b0156a679b6f481d45b9d32dfb8ca%20Domain%2f%40shabik.com%20Source%2f&monetid=10000&cli_ip=146.251.121.169')
    #exit()

    #prepare, get unique_id for CBB, which would mix up with the CJME

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'uid':get_unique_id}, \
                        where={'only_blackberry':lambda line:helper_regex.extract(line,r'(CBB\b)')  \
                                                            and not helper_regex.extract(line,r'(CJME\b)') \
                                                            and not helper_regex.extract(line,r'(CS60\b)'), \
                               'filtered':is_correct_date}, \
                        group_by={'daily':lambda line:current_date},
                        db_name='raw_data_shabik_360')

    stat_plan_prepare.add_stat_sql(stat_sql)

    stat_plan_prepare.add_log_source(r'\\192.168.0.107\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    stat_plan_prepare.add_log_source(r'\\192.168.0.117\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    stat_plan_prepare.add_log_source(r'\\192.168.0.118\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    stat_plan_prepare.add_log_source(r'\\192.168.0.75\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    # next date
    stat_plan_prepare.add_log_source(r'\\192.168.0.107\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +helper_regex.date_add(datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'),1))
    stat_plan_prepare.add_log_source(r'\\192.168.0.117\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +helper_regex.date_add(datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'),1))
    stat_plan_prepare.add_log_source(r'\\192.168.0.118\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +helper_regex.date_add(datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'),1))
    stat_plan_prepare.add_log_source(r'\\192.168.0.75\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +helper_regex.date_add(datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'),1))

    #stat_plan_prepare.add_log_source(r'\\192.168.0.107\logs_moagent_wap_shabik_360\internal_perf.log')

    stat_plan_prepare.run()

    blackberry_unique_id_set=helper_mysql.get_raw_collection_from_key('Shabik_360',"moagent_wap_test","filtered_only_blackberry_daily_uid_unique", \
                                                               '',current_date,'raw_data_shabik_360')

    print 'blackberry_unique_id_set:',blackberry_unique_id_set


    # filtering CBB unique_id in the getting version

    def get_filtered_version(line):
        if '6.4.0.120521' in line: # and 'CBB' not in line:
            unique_id=get_unique_id(line)
            if unique_id in blackberry_unique_id_set:
                return 'CBB-6.4.0.120521'
            return 'CJME-6.4.0.120521'
        if '6.4.7.120606' in line: # and 'CBB' not in line:
            unique_id=get_unique_id(line)
            if unique_id in blackberry_unique_id_set:
                return 'CBB-6.4.7.120606'
            return 'CJME-6.4.7.120606'
        if '6.41.1426' in line:
            return 'C60-6.41.1426'
        if '6.42.1406' in line:
            return 'C60-6.42.1406'
        return 'Misc'


    def is_correct_date(line):
        line=helper_regex.format_date_time_moagent(line)
        time=helper_regex.extract(line,r'(\d+\-\d+\-\d+ \d+:\d+)')
        if not time:
            return False
        #print time,helper_regex.time_add(time+':00',-5.0/24)
        match=current_date in helper_regex.time_add(time+':00',-5.0/24)
        #if match:
        #    print helper_regex.time_add(time+':00',-5.0/24)
        #    print current_date,helper_regex.time_add(time+':00',-5),':',line
        print time
        return match

    #main stat

    stat_plan=Stat_plan()

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'uid':get_unique_id, \
                                                          'auth_id':get_auth_id}, \
                        select_count_exist={'action':'(.)'}, \
                        where={'filtered':is_correct_date}, \
                        group_by={'by_version':get_filtered_version, \
                                  'daily':lambda line:current_date}, \
                        db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql)

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'uid':get_unique_id}, \
                        select_count_exist={'action':'(.)'}, \
                        where={'with_auth_id':lambda line:len(get_auth_id(line))>6 and len(get_unique_id(line))>5, \
                               'filtered':is_correct_date}, \
                        group_by={'by_version':get_filtered_version, \
                                  'daily':lambda line:current_date}, \
                        db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql)

    """
    def p(line):
        print 'test:',line
        return ''

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_exist={'action':p}, \
                        where={'test':'(090b0156a679b6f481d45b9d32dfb8ca|c019c01ab874b9b89288a7993ede761b)'}, \
                        db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql)
    """

    stat_plan.add_log_source(r'\\192.168.0.107\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    stat_plan.add_log_source(r'\\192.168.0.117\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    stat_plan.add_log_source(r'\\192.168.0.118\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    stat_plan.add_log_source(r'\\192.168.0.75\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    # next date
    stat_plan.add_log_source(r'\\192.168.0.107\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +helper_regex.date_add(datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'),1))
    stat_plan.add_log_source(r'\\192.168.0.117\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +helper_regex.date_add(datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'),1))
    stat_plan.add_log_source(r'\\192.168.0.118\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +helper_regex.date_add(datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'),1))
    stat_plan.add_log_source(r'\\192.168.0.75\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +helper_regex.date_add(datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'),1))

    #stat_plan.add_log_source(r'\\192.168.0.107\logs_moagent_wap_shabik_360\internal_perf.log')

    stat_plan.run()

    print '\n'.join(sorted(log_buffer))



    sql=r'''
    
    select * 
    from raw_data_shabik_360 
    where `oem_name`="Shabik_360" 
    and `category`="moagent_wap_test" 
    and `key` like '%%_daily_auth_id_unique'
    and `date`='%s'

    ''' % (current_date,)


    target_rows=helper_mysql.fetch_rows(sql)

    for row in target_rows:
        print row
        auth_id_set=helper_mysql.get_raw_collection_from_key(row['oem_name'],row['category'],row['key'],row['sub_key'],row['date'],'raw_data_shabik_360')
        print 'auth_id_set:',auth_id_set
        if auth_id_set:

            auth_id_to_acc=helper_sql_server.fetch_dict_map_to_collection(auth_id_set,sql_template=r'''

            select id,acc
            from shabik_mt.dbo.moweb_nologin_auth with(nolock)
            where id in (%s)

            ''',conn_config=config.conn_mt,step=1000)
            

            auth_success_msisdn_set = set('+966'+str(msisdn).strip()[1:] for msisdn in auth_id_to_acc.values() if msisdn) # auth_success_msisdn_set, remove leading 0
            succ_auth_id_count=len([k for k,v in auth_id_to_acc.iteritems() if v])
            succ_auth_id_count_unique=len(set([v for k,v in auth_id_to_acc.iteritems() if v]))
            succ_auth_id_count_unique2=len(auth_success_msisdn_set)
            if succ_auth_id_count_unique2 != succ_auth_id_count_unique:
                raise Exception('--- error')

            helper_mysql.put_raw_data(row['oem_name'],row['category'],row['key'].replace('auth_id_unique','auth_id_succ_id_unique'),row['sub_key'],succ_auth_id_count,'raw_data_shabik_360',row['date'])

            helper_mysql.put_raw_data(row['oem_name'],row['category'],row['key'].replace('auth_id_unique','auth_id_succ_msisdn_unique'),row['sub_key'],succ_auth_id_count_unique,'raw_data_shabik_360',row['date'])

            
            # auth success new user
            start_time=helper_regex.date_add(current_date,0)+' 03:00:00'
            end_time=helper_regex.date_add(current_date,1)+' 03:00:00'

            new_user_set = common_shabik_360.get_user_ids_created_in_time_range(start_time_zero_timezone=start_time,end_time_zero_timezone=end_time)
            new_user_id_msisdn_dict = common_shabik_360.get_id_msisdn_dict(new_user_set) # +966
            new_user_msisdn_set = set(msisdn for msisdn in new_user_id_msisdn_dict.values() if msisdn) 
            
            new_user_success_auth_set = auth_success_msisdn_set  &  new_user_msisdn_set

            succ_auth_new_user_id_count_unique = len(new_user_success_auth_set)
            helper_mysql.put_raw_data(row['oem_name'],row['category'],row['key'].replace('auth_id_unique','auth_id_succ_msisdn_new_user_unique'),row['sub_key'],succ_auth_new_user_id_count_unique,'raw_data_shabik_360',row['date'])
            



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent_wap(time.time()-3600*24*i)

    