#!C:\Python26\python.exe -u
import cgi
import cgitb; cgitb.enable()  # for troubleshooting
import gzip
import hashlib
import sys
sys.path.append(r'E:\RoutineScripts')

import config
import helper_regex
import helper_mysql
import helper_file
import helper_collection

form = cgi.FieldStorage()

db_name = form.getvalue("db_name", "raw_data")
oem_name = form.getvalue("oem_name", "Vodafone")
category = form.getvalue("category", "chatroom")
key = form.getvalue("key", "enter_room_daily_monet_id_unique_collection_id")
sub_key = form.getvalue("sub_key", "")
date_begin = form.getvalue("date_begin", "")
date_end = form.getvalue("date_end", "")

hash_code = form.getvalue("hash_code", "")

config.collection_cache_enabled=True

#pre-condition
if not date_begin or not date_end:
    content='document.write("-");'
    #print "Content-type: application/x-unknown"
    #print "Content-Disposition: attachment; filename=data.txt"
    print "Content-type: application/javascript"
    print "Content-Length: "+str(len(content))
    print ''
    print content

    exit()


#fetch collection id

sql=r'''
    select `date`,`value`
    from `%s`
    where `oem_name`='%s'
    and `category`='%s'
    and `key`='%s'
    and `sub_key`='%s'
    and `date`>='%s'
    and `date`<='%s'
''' % (db_name,oem_name,category,key,sub_key,date_begin,date_end)

target_collection_ids = helper_mysql.fetch_dict(sql)

sql=r'''
    select `date`,`value`
    from `%s`
    where `oem_name`='%s'
    and `category`='%s'
    and `key`='%s'
    and `sub_key`='%s'
    and `date`!=''
''' % (db_name,oem_name,category,key,sub_key)

base_collection_ids = helper_mysql.fetch_dict(sql)


#target collection

target_user_set=set([])

for date_str,collection_id in target_collection_ids.iteritems():
    collection_temp=helper_mysql.get_raw_collection_by_id(collection_id)
    target_user_set|=collection_temp


#start / end date

start_date={}
end_date={}

date_order=base_collection_ids.keys()[:]
date_order.sort()


for date_str in date_order:
    collection_id=base_collection_ids[date_str]
    collection_temp=helper_mysql.get_raw_collection_by_id(collection_id)
    
    for user_id in collection_temp:  
        if user_id not in target_user_set:
            continue
        if not start_date.has_key(user_id):
            start_date[user_id]=date_str
        if not end_date.has_key(user_id):
            end_date[user_id]=date_str
        elif end_date[user_id]<date_str:
            end_date[user_id]=date_str

#calculate life length

user_life_length={}

for user_id in start_date.keys():
    user_life_length[user_id]=helper_regex.get_day_diff_from_date_str(end_date[user_id],start_date[user_id])+1

#calculate dispersion

user_life_length_dispersion={}

for user_id,length in user_life_length.iteritems():
    if not user_life_length_dispersion.has_key(length):
        user_life_length_dispersion[length]=0
    user_life_length_dispersion[length]+=1

#calculate churn rate

churn_rate_result={4:0,7:0,14:0,21:0,28:0,35:0,42:0,49:0,56:0}
day_levels=churn_rate_result.keys()
day_levels.sort()

for length,count in user_life_length_dispersion.iteritems():
    for i in day_levels:
        if length>=i:
            churn_rate_result[i]+=count

content=''+str(1.0*churn_rate_result[4]/len(target_user_set))[0:4]+' | '
content+=''+str(1.0*churn_rate_result[7]/len(target_user_set))[0:4]+' | '
content+=''+str(1.0*churn_rate_result[14]/len(target_user_set))[0:4]+' | '
content+=''+str(1.0*churn_rate_result[21]/len(target_user_set))[0:4]+' | '
content+=''+str(1.0*churn_rate_result[28]/len(target_user_set))[0:4]+' | '
content+=''+str(1.0*churn_rate_result[35]/len(target_user_set))[0:4]+' | '
content+=''+str(1.0*churn_rate_result[42]/len(target_user_set))[0:4]+' | '
content+=''+str(1.0*churn_rate_result[49]/len(target_user_set))[0:4]+' | '
content+=''+str(1.0*churn_rate_result[56]/len(target_user_set))[0:4]+' | '
content+='Total: '+str(len(target_user_set))

content='document.write("<div style=\\"float:left;\\">'+content+'</div>");'

#print "Content-type: application/x-unknown"
#print "Content-Disposition: attachment; filename=data.txt"
print "Content-type: application/javascript"
print "Content-Length: "+str(len(content))
print ''
print content

try:

    pass
except:
    content=r'''
    This download looks out of date. PLease refresh the statistics page and try download again.
    For any question please email xianli@mozat.com, thanks for your kind assistance.
    '''

    print "Content-type: text/plain"
    print "Content-Length: "+str(len(content))
    print ''
    print content


