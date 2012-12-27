#!C:\Python26\python.exe -u
import sys,os
sys.path.append(r'E:\RoutineScripts')

import helper_sql_server
import helper_mysql
import helper_math
import config

import cgi
import cgitb; cgitb.enable()  # for troubleshooting
import gzip
import hashlib

temp_stdout=sys.stdout
sys.stdout=open(os.devnull,"w")

try:
    form = cgi.FieldStorage()
    
    table=form.getvalue("table", "raw_data")
    oem_name=form.getvalue("oem_name", "Mozat")
    category=form.getvalue("category", "moagent")
    key=form.getvalue("key", "app_page_only_ais_hourly_visitor_unique_collection_id")
    sub_key=form.getvalue("sub_key", "")
    date_start=form.getvalue("date_start", "2012-04-03 00")
    date_end=form.getvalue("date_end", "2012-04-16 23")
    sign=form.getvalue("sign", "")
    
    signed_key=','.join([table,oem_name,category,key,sub_key,date_start,date_end,config.url_sign_key])
    expected_sign=helper_math.md5(signed_key)

    if expected_sign!=sign:
        content='Auth Failed. Don\'t try to modify the download url.'
    else:
        sql=r'''
            select `value`
            from %s
            where oem_name='%s'
            and category='%s'
            and `key`='%s'
            and sub_key='%s'
            and date>='%s'
            and date<='%s'
        ''' % (table,oem_name,category,key,sub_key,date_start,date_end)
        
        collection_ids=helper_mysql.fetch_set(sql)
        
        #print collection_ids
        result_set=set([])
        for id in collection_ids:
            temp_set=helper_mysql.get_raw_collection_by_id(int(id))
            result_set|=temp_set
    
        content='\n'.join(sorted([str(i) for i in result_set]))

    sys.stdout=temp_stdout
    print "Content-type: application/x-unknown"
    print "Content-Disposition: attachment; filename=data.txt"
    print "Content-Length: "+str(len(content))
    print ''
    print content

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


