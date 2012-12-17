import helper_sql_server
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import config
import helper_regex 
import helper_mysql

helper_mysql.quick_insert=True


def stat_moagent_wap(my_date):
    
    # INFO 2010-04-08 00:00:00 - [          workThread] (        CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    oem_name='Shabik_360' 
    stat_category='moagent_wap'

    stat_plan=Stat_plan()

    #01 Apr 00:00:41,556 - 13373042298479	16	16	0	-1	http://Moweb-stc.morange.com/mobile_login_poll.ashx?action=fast&auth_id=6285124&unique_id=13372997707361a6.4.0.120329&monetid=10000&cli_ip=212.118.140.28

    #01 Apr 00:01:41,024 - 133640166371425	16	16	0	0	http://moweb-stc-py.morange.com/login.aspx?monetid=10000&moclientwidth=240&userAgent=Nokia2700c-2-09.97+JConf%2FCLDC-1.1+JProf%2FMIDP-2.1+Encoding%2FISO-8859-1+Locale%2Far+Lang%2Far+Caps%2F7516+Morange%2F6.4.0.120329+Domain%2F%40shabik.com+CJME%2F120329+Source%2Fnull+UniqueId%2F13363999265941&moclientheight=280&devicewidth=240&deviceheight=320&isprefetch=0&cli_ip=84.235.73.208

    #01 Apr 00:01:54,586 - 13332097120433	0	0	0	-1	http://moweb-stc-py.morange.com/first_opening_counter.aspx?profilePage13332085917551a6.4.2.120329&monetid=10000&cli_ip=188.55.81.33

    #01 Apr 00:22:00,922 - 134913576022122	0	0	0	-1	http://moweb-stc-py.morange.com/first_opening_counter.aspx?authId=628752013491355767721a6.4.2.120329&monetid=10000&cli_ip=84.235.73.210

    def get_unique_id(line):
        line_lower=line.lower()
        if 'sbua=' in line_lower or 'cs60' in line_lower:# or 'CS60' in line or 'cs60' in line:
            unique_id=helper_regex.extract(line,r'PI(?:%2f|\s*)(\w+)[ignorecase]').lower()
            return unique_id or '(Symbian-no-id)'
        else:
            if 'first_opening_counter.aspx?authId=' in line and 'a6.4.' in line: #2.120329
                auth_id=helper_regex.extract(r'authId=(\d{7,})',line)
                line=helper_regex.regex_replace(r'authId=\d{7,}','authId='+auth_id+'&unique_id=',line)
            unique_id=helper_regex.extract(line,r'unique_id=(\d+)')
            if unique_id:
                return unique_id
            unique_id=helper_regex.extract(line,r'j2meid=(\d+)[ignorecase]')
            if unique_id:
                return unique_id
            unique_id=helper_regex.extract(line,r'UniqueId%2F(\d+)')
            if unique_id:
                return unique_id
            unique_id=helper_regex.extract(line,r'[\?&]\w+?(\d{11,14})a[0-9\.]{7,}')
            if unique_id:
                return unique_id
            #unique_id=helper_regex.extract(line,r'(\d{11,14})')
            #if unique_id:
            #    return unique_id
            return '(no-id)'
        
    def get_version(line):
        line_lower=line.lower()
        if 'sbua=' in line_lower or 'cs60' in line_lower:# or 'CS60' in line or 'cs60' in line:
            version=helper_regex.extract(line_lower,r'cs60(?:%2f|\s*)(6[\.\d]+)[ignorecase]')
            return 'CS60-'+version or '(Symbian-unknown-version)'
        else:  # J2ME
            if 'first_opening_counter.aspx?authId=' in line and 'a6.4.' in line: #2.120329
                auth_id=helper_regex.extract(r'authId=(\d{7,})',line)
                line=helper_regex.regex_replace(r'authId=\d{7,}','authId='+auth_id+'&unique_id=',line)
            version=helper_regex.extract(line,r'unique_id=\d+a([0-9\.]+)')
            if version:
                return version
            version=helper_regex.extract(line,r'j2meid=\d+a([0-9\.]+)[ignorecase]')
            if version:
                return version
            version=helper_regex.extract(line,r'Morange%2F([0-9\.]+)')
            if version:
                return version
            version=helper_regex.extract(line,r'[\?&]\w+?\d{11,14}a([0-9\.]{7,})')
            if version:
                return version
            version=helper_regex.extract(line,r'\d{11,14}a([0-9\.]{7,})')
            if version:
                return version
            version=helper_regex.extract(line,r'nulla([0-9\.]{7,})')
            if version:
                return version
        return '(no-version)'

    def get_auth_id(line):
        line_lower=line.lower()
        if 'sbua=' in line_lower or 'cs60' in line_lower:# or 'CS60' in line or 'cs60' in line:
            auth_id=helper_regex.extract(line,r'auth_id=(\d+)[ignorecase]').lower()
            return auth_id or '0'
        else:
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
                return auth_id
            return '0'

    def process_line(line='',exist='',group_key=''):
        if '6.41.1426' in line or '6.42.1406' in line:
            version=get_version(line)
            auth_id=get_auth_id(line)
            unique_id=get_unique_id(line)
            
            if version not in ('CS60-6.41.1426','CS60-6.42.1406') or auth_id=='0' or 'no' in unique_id:
                print get_version(line),get_auth_id(line),get_unique_id(line),line
        return

    # for consistency check

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        process_exist={'test':{'pattern':r'(.)', \
                                               'process':process_line}}, \
                        db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql)

    #for stat

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'uid':get_unique_id, \
                                                          'auth_id':get_auth_id}, \
                        select_count_exist={'action':'(.)'}, \
                        group_by={'by_version':get_version, \
                                  'daily':lambda line:current_date},
                        db_name='raw_data_shabik_360')
    

    stat_plan.add_stat_sql(stat_sql)

    stat_plan.add_log_source(r'\\192.168.0.107\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    stat_plan.add_log_source(r'\\192.168.0.117\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    stat_plan.add_log_source(r'\\192.168.0.118\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    stat_plan.add_log_source(r'\\192.168.0.75\logs_moagent_wap_shabik_360\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    #stat_plan.add_log_source(r'\\192.168.0.107\logs_moagent_wap_shabik_360\internal_perf.log')

    stat_plan.run()



    sql=r'''
    
    select * 
    from raw_data_shabik_360 
    where `oem_name`="Shabik_360" 
    and `category`="moagent_wap" 
    and `key` like "%%auth_id_unique" 
    and `key` not like "request_%%"
    and `date`='%s'

    ''' % (current_date,)

    target_rows=helper_mysql.fetch_rows(sql)

    for row in target_rows:
        auth_id_set=helper_mysql.get_raw_collection_from_key(row['oem_name'],row['category'],row['key'],row['sub_key'],row['date'],'raw_data_shabik_360')
        
        if auth_id_set:

            auth_id_to_acc=helper_sql_server.fetch_dict_map_to_collection(auth_id_set,sql_template=r'''

            select id,acc
            from shabik_mt.dbo.moweb_nologin_auth with(nolock)
            where id in (%s)

            ''',conn_config=config.conn_mt,step=1000)
            
            succ_auth_id_count=len([k for k,v in auth_id_to_acc.iteritems() if v])
            succ_auth_id_count_unique=len(set([v for k,v in auth_id_to_acc.iteritems() if v]))
            
            #print len(auth_id_to_acc),succ_auth_id_count,succ_auth_id_count_unique
            #exit()

            helper_mysql.put_raw_data(row['oem_name'],row['category'],row['key'].replace('auth_id_unique','auth_id_succ_id_unique'),row['sub_key'],succ_auth_id_count,'raw_data_shabik_360',row['date'])

            helper_mysql.put_raw_data(row['oem_name'],row['category'],row['key'].replace('auth_id_unique','auth_id_succ_msisdn_unique'),row['sub_key'],succ_auth_id_count_unique,'raw_data_shabik_360',row['date'])


    """
    targeted_versions=['6.4.3.120404','6.4.2.120404','6.4.0.120329']


    for version in targeted_versions:
        
        targeted_unique_id_set=helper_mysql.get_raw_collection_from_key('Shabik_360','moagent_wap','by_version_daily_uid_unique',version,current_date,'raw_data_shabik_360')

        active_unique_id_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent',key='app_page_client_type_daily_version_unique_id_unique',sub_key='CJME_'+version,date=current_date,table_name='raw_data_shabik_360')

        active_targeted_user_id_set = targeted_unique_id_set & active_unique_id_set
        print len(targeted_unique_id_set),(active_unique_id_set),len(active_targeted_user_id_set)
        exit()

        helper_mysql.put_raw_data('Shabik_360','moagent_wap','by_version_daily_active_uid_unique',version,len(active_targeted_user_id_set),current_date,'raw_data_shabik_360')
    """



if __name__=='__main__':

    for i in range(config.day_to_update_stat+12,0,-1):
        stat_moagent_wap(time.time()-3600*24*i)

    