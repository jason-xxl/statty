import glob
import re
import helper_regex
import helper_mysql 
import _mysql






db_name='raw_data'
oem_name='Vodafone'
category='sub'
key='daily_fresh_subscriber_include_unsub'
sub_key=''
date='2011-11-04'
value='1024'

helper_mysql.put_raw_data(oem_name=oem_name,category=category,key=key,sub_key=sub_key, \
value=value,table_name=db_name,date=date,created_on=None,db_conn=None)

exit()

helper_mysql.db.query(r'''
delete from `%s` where oem_name='%s' and category='%s' and `key`='%s' and `sub_key`='%s' and `date`='%s'
''' % (db_name,oem_name,category,key,sub_key,date))

print 'matched rows:'+str(helper_mysql.get_one_value_int(r'''
select count(*) from `%s` where oem_name='%s' and category='%s' and `key`='%s' and `date`='%s'
''' % (db_name,oem_name,category,key,date)))

print 'first value:'+str(helper_mysql.get_one_value_int(r'''
select `value` from `%s` where oem_name='%s' and category='%s' and `key`='%s' and `date`='%s'
''' % (db_name,oem_name,category,key,date)))

helper_mysql.db.query(r'''replace INTO `mozat_stat`.`%s` (`oem_name` ,`category` ,`key` ,`sub_key` ,`date` ,`value` ,`created_on`)VALUES ('%s', '%s', '%s', '%s', '%s', '%s',CURRENT_TIMESTAMP);''' \
         % (db_name,oem_name,category,key,sub_key,value,date))

print 'updated value:'+str(helper_mysql.get_one_value_int(r'''
select `value` from `%s` where oem_name='%s' and category='%s' and `key`='%s' and `date`='%s'
''' % (db_name,oem_name,category,key,date)))




exit()

for i in range(1,100,1):
    sql=r'''
    
    select `sub_key`,`value`
    from raw_data_url_pattern
    where `oem_name`='STC' and `category`='moagent' and `key`='app_page_by_url_pattern_daily_visitor_unique'
    and `date`="%s"
    and `sub_key` like '%%jit%%'
    order by date desc;
        
    ''' % (helper_regex.date_add('2011-06-01',i),)

    #print sql
    print helper_regex.date_add('2011-07-01',i)
    result=helper_mysql.fetch_dict(sql)
    for k,v in result.iteritems():
        print k,v
        

exit()