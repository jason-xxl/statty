import helper_sql_server
import helper_mysql

import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex

import config

number_table={
    'z': '0',
    'a': '1',
    'y': '2',
    'b': '3',
    'x': '4',
    'c': '5',
    'w': '6',
    'd': '7',
    'v': '8',
    'e': '9',
    
    'Z': '0',
    'A': '1',
    'Y': '2',
    'B': '3',
    'X': '4',
    'C': '5',
    'W': '6',
    'D': '7',
    'V': '8',
    'E': '9'
    }

def translate_phone_number_client_download(line):
    global number_table
    #print line
    number=helper_regex.extract(line,r'GET (?:[^ ]+) ([a-zA-Z]{10,}) [ignorecase]')
    if number:
        #print '1:'+number
        for i,j in number_table.iteritems():
            number=number.replace(i,j)
    else:
        number=helper_regex.extract(line,r'GET (?:[^ ]+) ([0-9a-zA-Z]{5,}) [ignorecase]')
        #print '2:'+number
        number=str(helper_regex.base36decode(number)).zfill(10)
        number='628'+str(number)

    #print 'return:'+number        
    return number




def translate_phone_number_open_homepage(line):
    global number_table
    #print line
    number=helper_regex.extract(line,r'GET /([zaybxcwdve]{12,}) [ignorecase]')
    if number:
        #print '1:'+number
        for i,j in number_table.iteritems():
            number=number.replace(i,j)
    else:
        number=helper_regex.extract(line,r'GET /(\w{5,}) [ignorecase]')
        #print '2:'+number
        number=str(helper_regex.base36decode(number)).zfill(10)
        number='628'+str(number)

    #print 'return:'+number        
    return number






def stat_website_fresh_sub_to_login(my_date):

    oem_name='Telk_Armor'
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    

    # step 1: first sub msisdn (with login status)

    stat_category='sub'
        
    key='first_sub_msisdn_with_login_status'
    db='telkomsel_mt'
    db_name='raw_data_user_device'    
    
    sql="""

    select

    '62'+replace([user_name],'@telk_armor','') as msisdn
    ,case when lastLogin is null then 0
    else 1 end as logined

    from [mozone_user].[dbo].[Profile] b with(nolock)
    where user_name in (

        SELECT replace([msisdn],'+62','')+'@telk_armor' COLLATE DATABASE_DEFAULT as [user_name]
        FROM [telkomsel_mt].[dbo].[logs] with(nolock)
        where [CreatedOn]<'%s' 
        and action in (1,2,3,10,11,12)
        group by [msisdn]
        having min([CreatedOn])>='%s'

    )

    """ % (end_time,start_time)
    
    print 'SQL Server:'+sql
    values=helper_sql_server.fetch_dict(config.conn_telk_armor,sql)
    print values
    
    for msisdn,logined in values.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,date_today+'_'+msisdn,logined,table_name=db_name)


    # step 2: homepage access

    oem_name='Telk_Armor'
    stat_category='website'
    db_name='raw_data_user_device'
    

    # 2010-10-18 17:57:12 GET /wyvaywwdxvavb - 114.127.246.110 HTTP/1.1 Mozilla/5.0+(SymbianOS/9.4;+Series60/5.0+Nokia5233/21.1.004;+Profile/MIDP-2.1+Configuration/CLDC-1.1+)+AppleWebKit/525+(KHTML,+like+Gecko)+Version/3.0+BrowserNG/7.2.5.2+3gpp-gba - 302 464 93
    # 2010-10-18 18:05:56 GET /ywily4 - 114.127.246.45 HTTP/1.1 Mozilla/5.0+(SymbianOS/9.2;+U;+Series60/3.1+NokiaE63-1/410.21.010;+Profile/MIDP-2.0+Configuration/CLDC-1.1+)+AppleWebKit/413+(KHTML,+like+Gecko)+Safari/413 - 302 450 109

    stat_plan=Stat_plan(plan_name='daily-website-telk-armor')
    
    # download uv
    
    stat_sql_user_agent_str=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_text_value={'native_user_agent':r'HTTP\/1\.\d ([^ ]+) \- '}, \
                                   where={'from_homepage_link':r'(GET \/[0-9a-zA-Z]{5,} )'}, \
                                   group_by={'by_phone_number':translate_phone_number_open_homepage, \
                                             'by_date':r'(^[\d\-]{10})'}, \
                                   db_name=db_name)
    
    stat_plan.add_stat_sql(stat_sql_user_agent_str)
    
    
    stat_plan.add_log_source(r'\\192.168.100.21\log_armor.co.id\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'.log')

    stat_plan.run()


    

    # step 3: download client

    oem_name='Telk_Armor'
    stat_category='website'
    db_name='raw_data_user_device'

    # 2010-10-13 19:18:22 GET /j2me/armorlife.jad lq9vm1 114.127.246.44 HTTP/1.1 Nokia7610/2.0+(7.0642.0)+SymbianOS/7.0s+Series60/2.1+Profile/MIDP-2.0+Configuration/CLDC-1.0 - 200 1105 296

    stat_plan=Stat_plan(plan_name='daily-website-telk-armor')
    
    # download uv
    
    stat_sql_lownloaded_link=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'url':r'GET ([^ ]+) (?:[0-9a-zA-Z]{5,}|[a-zA-Z]{10,}) '}, \
                                   where={'opened_download_link':r'GET ([^ ]+) (?:[0-9a-zA-Z]{5,}|[a-zA-Z]{10,}) '}, \
                                   group_by={'by_phone_number':translate_phone_number_client_download, \
                                             'by_date':r'(^[\d\-]{10})', \
                                             'by_url':r'GET ([^ ]+) (?:[0-9a-zA-Z]{5,}|[a-zA-Z]{10,}) '}, \
                                   db_name=db_name, \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)
    
    stat_plan.add_stat_sql(stat_sql_lownloaded_link)
    
    stat_plan.add_log_source(r'\\192.168.100.21\W3SVC1_download_log\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'.log')

    stat_plan.run()



    # step 4: calculated daily user that accessed homepage

    oem_name='Telk_Armor'
    stat_category='sub'
    db_name='raw_data'

    key='fresh_sub_user_access_download_homepage_unique'

    sql="""

    select count(*)
    from (

        select

        sub_key as `msisdn`

        ,sum(if(`oem_name`="Telk_Armor" 
        and `category`="website" 
        and `key`="from_homepage_link_by_date_by_phone_number_native_user_agent_first_text_value"
        ,1,0))
        *
        sum(if(`oem_name`="Telk_Armor" 
        and `category`="sub" 
        and `key`="first_sub_msisdn_with_login_status"
        ,1,0))
        as `accessed_homepage`   
        
        from `raw_data_user_device`
        where (
            `oem_name`="Telk_Armor" 
            and `category`="website" 
            and `key`="from_homepage_link_by_date_by_phone_number_native_user_agent_first_text_value"
            and date>='%s'
            
            or

            `oem_name`="Telk_Armor" 
            and `category`="sub" 
            and `key`="first_sub_msisdn_with_login_status"
            and date='%s'
        )
        group by `sub_key`
        having `accessed_homepage`>0
    
    ) a
    """ % (date_today,date_today)
    
    print 'Mysql:'+sql
    value=helper_mysql.get_one_value_int(sql)
    
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name=db_name)


    



    # step 5: calculated daily fresh user that download client

    oem_name='Telk_Armor'
    stat_category='sub'
    db_name='raw_data'

    key='fresh_sub_user_download_client_unique'

    sql="""

    select count(*)
    from (

        select

        sub_key as `msisdn`

        ,sum(if(`oem_name`="Telk_Armor" 
        and `category`="website" 
        and `key`="opened_download_link_by_date_by_phone_number_by_url_url_count"
        ,1,0))
        *
        sum(if(`oem_name`="Telk_Armor" 
        and `category`="sub" 
        and `key`="first_sub_msisdn_with_login_status"
        ,1,0))
        as `downloaded_client`   
        
        from `raw_data_user_device`
        where (
            `oem_name`="Telk_Armor" 
            and `category`="website" 
            and `key`="opened_download_link_by_date_by_phone_number_by_url_url_count"
            and date>='%s'
            
            or

            `oem_name`="Telk_Armor" 
            and `category`="sub" 
            and `key`="first_sub_msisdn_with_login_status"
            and date='%s'
        )
        group by substring(`sub_key`,1,13)
        having `downloaded_client`>0
    
    ) a
    """ % (date_today,date_today)
    
    print 'Mysql:'+sql
    value=helper_mysql.get_one_value_int(sql)
    
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name=db_name)

    

    # step 6: calculated daily fresh user

    oem_name='Telk_Armor'
    stat_category='sub'
    db_name='raw_data'

    key='fresh_sub_user_unique'

    sql="""

    select count(distinct `sub_key`)
    from `raw_data_user_device`
    where (
        `oem_name`="Telk_Armor" 
        and `category`="sub" 
        and `key`="first_sub_msisdn_with_login_status"
        and date='%s'
    )

    """ % (date_today,)
    
    print 'Mysql:'+sql
    value=helper_mysql.get_one_value_int(sql)
    
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name=db_name)


    # step 7: calculated daily user that succeeded to login

    oem_name='Telk_Armor'
    stat_category='sub'
    db_name='raw_data'

    key='fresh_sub_user_succeeded_to_login_unique'

    sql="""

    select count(distinct `sub_key`)
    from `raw_data_user_device`
    where (
        `oem_name`="Telk_Armor" 
        and `category`="sub" 
        and `key`="first_sub_msisdn_with_login_status"
        and `value`>0
        and date='%s'
    )

    """ % (date_today,)
    
    print 'Mysql:'+sql
    value=helper_mysql.get_one_value_int(sql)
    
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value,table_name=db_name)



    # step 8: FINALLY unsub in last day but no login
    
    key='unsub_for_no_login'
    
    db='telkomsel_mt'
    sql="""
    
    select count(*) from (
        SELECT replace([msisdn],'+62','')+'@telk_armor' COLLATE DATABASE_DEFAULT as [user_name]
          FROM [telkomsel_mt].[dbo].[logs] with(nolock)
        group by [msisdn]
        having max([CreatedOn])>='%s' and max([CreatedOn])<'%s'
        and max(id*100+action)-max(id*100)in (4,5,6,7,8,9)
    ) a 
    left join [mozone_user].[dbo].[Profile] b with(nolock)
    on a.[user_name] = b.[user_name]
    where b.lastLogin is null

    """ % (start_time,end_time)
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_telk_armor,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)








    # step 9: calculated daily fresh user that download client, by client type


    oem_name='Telk_Armor'
    stat_category='sub'
    db_name='raw_data'

    key='fresh_sub_user_download_client_by_client_type_unique'

    sql="""

    select 

    substring(`sub_key`,15) as `download_link`
    ,count(distinct substring(`sub_key`,1,13)) as `fresh_user_count`

    from `raw_data_user_device`
    where (

        `oem_name`="Telk_Armor" 
        and `category`="website" 
        and `key`="opened_download_link_by_date_by_phone_number_by_url_url_count"
        and `date`>='%s'

    ) and substring(`sub_key`,1,13) in (

        select distinct `sub_key`
        from `raw_data_user_device`
        where (
            `oem_name`="Telk_Armor" 
            and `category`="sub" 
            and `key`="first_sub_msisdn_with_login_status"
            and `date`='%s'
        )
    )

    group by `download_link`

    """ % (date_today,date_today)
    
    print 'Mysql:'+sql
    values=helper_mysql.fetch_dict(sql)


    for client_url,user_count in values.iteritems():
        
        print client_url+':'+user_count
        helper_mysql.put_raw_data(oem_name,stat_category,key,date_today+'_'+client_url,user_count,table_name=db_name)





if __name__=='__main__':

    for i in range(1,3+config.day_to_update_stat): #default refresh last 3 days
        stat_website_fresh_sub_to_login(time.time()-3600*24*i)


