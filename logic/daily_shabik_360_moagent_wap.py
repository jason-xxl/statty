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
    
    raise DeprecationWarning('the code is no longer maintained. see daily_shabik_360_moagent_wap_auth_rate.py')
    
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


    def get_url_key(line):
        if 'first_opening_counter.aspx?authId=' in line and 'a6.4.2.120329' in line: #
            auth_id=helper_regex.extract(r'authId=(\d{7})',line)
            line=helper_regex.regex_replace(r'authId=\d{7}','authId='+auth_id+'&unique_id=',line)
        #line=helper_regex.regex_replace(r'(pid=\D)',r'pid(empty)=&',line).replace(r'&&',r'&')
        if not helper_regex.extract(line,r'(pid=\d+)'):
            line=line.replace(r'pid=',r'pid(empty)=')
        line=line.replace(r'isprefetch=0',r'').replace(r'isprefetch=1',r'').replace(r'trace=',r'trace_').replace(r'source=',r'source_').replace(r'&&',r'&')
        line=helper_regex.regex_replace(r'(\d{11,14}a[0-9\.]{7,})',r'',line)
        line=helper_regex.regex_replace(r'(\d{11,14})',r'',line)
        key=helper_regex.get_simplified_url_unique_key(line)
        return key

    def get_unique_id(line):
        if 'first_opening_counter.aspx?authId=' in line and 'a6.4.' in line: #2.120329
            auth_id=helper_regex.extract(r'authId=(\d{7})',line)
            line=helper_regex.regex_replace(r'authId=\d{7}','authId='+auth_id+'&unique_id=',line)
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
        if 'first_opening_counter.aspx?authId=' in line and 'a6.4.' in line: #2.120329
            auth_id=helper_regex.extract(r'authId=(\d{7})',line)
            line=helper_regex.regex_replace(r'authId=\d{7}','authId='+auth_id+'&unique_id=',line)
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
        if 'first_opening_counter.aspx?authId=' in line and 'a6.4.' in line: #2.120329
            auth_id=helper_regex.extract(line,r'(authId=\d{7})')
            line=helper_regex.regex_replace(r'authId=\d{7}','authId='+auth_id+'&unique_id=',line)
        auth_id=helper_regex.extract(line,r'authId=(\d{7})')
        if auth_id:
            return auth_id
        auth_id=helper_regex.extract(line,r'auth_id=(\d{7})')
        if auth_id:
            return auth_id
        auth_id=helper_regex.extract(line,r'authid=(\d{7})')
        if auth_id:
            return auth_id
        auth_id=helper_regex.extract(line,r'gid=(\d{7})')
        if auth_id:
            return auth_id
        return ''

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct={'uid':get_unique_id, \
                                               'ip':r'cli_ip=([0-9\.]+)'}, \
                        select_count_distinct_collection={'auth_id':get_auth_id}, \
                        select_count_exist={'action':'(.)'}, \
                        where={'request':r'(http)'}, \
                        group_by={'by_version':get_version, \
                                  'by_url_pattern':get_url_key, \
                                  'has_id':lambda line:'has_id' if get_unique_id(line)!='(no_id)' else 'has_no_id', \
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

    stat_plan.run()

    sql=r'''
    
    select * 
    from raw_data_shabik_360 
    where `oem_name`="Shabik_360" 
    and `category`="moagent_wap" 
    and `key` like "%%auth_id_unique" 
    and `date`='%s'

    ''' % (current_date,)

    target_rows=helper_mysql.fetch_rows(sql)

    for row in target_rows:
        auth_id_set=helper_mysql.get_raw_collection_from_key(row['oem_name'],row['category'],row['key'],row['sub_key'],row['date'],'raw_data_shabik_360')
        
        if auth_id_set:

            sql=r'''

            select count(*) as [total],count(distinct acc) as [unique]
            from shabik_mt.dbo.moweb_nologin_auth with(nolock)
            where acc is not null
            and id in (%s)

            ''' % (','.join(auth_id_set))

            values=helper_sql_server.fetch_row(config.conn_mt,sql)

            helper_mysql.put_raw_data(row['oem_name'],row['category'],row['key'].replace('auth_id_unique','auth_id_succ_id_unique'),row['sub_key'],values['total'],'raw_data_shabik_360',row['date'])
            helper_mysql.put_raw_data(row['oem_name'],row['category'],row['key'].replace('auth_id_unique','auth_id_succ_msisdn_unique'),row['sub_key'],values['unique'],'raw_data_shabik_360',row['date'])



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent_wap(time.time()-3600*24*i)

    