#!C:\Python26\python.exe -u
import sys,cgi
sys.path.append(r'E:\RoutineScripts')

import helper_mysql
import cgitb; cgitb.enable()  # for troubleshooting
import gzip
import hashlib
import config

config.hide_sys_output(True)

form = cgi.FieldStorage()

base_key = form.getvalue("base_key", "raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|photo|2012-06-06")
filter_key = form.getvalue("filter_key", base_key)


try:
    base_key=base_key.split('|')
    filter_key=filter_key.split('|')

    if not filter_key[5] or filter_key[5]=='-':
        filter_key[5]=base_key[5]

    if not '~' in base_key[5]:
        base_key[5]=base_key[5]+'~'+base_key[5]

    base_key.append(base_key[5].split('~')[0])
    base_key.append(base_key[5].split('~')[1])

    if not '~' in filter_key[5]:
        filter_key[5]=filter_key[5]+'~'+filter_key[5]

    filter_key.append(filter_key[5].split('~')[0])
    filter_key.append(filter_key[5].split('~')[1])

    base_collection_dict=helper_mysql.get_dict_of_raw_collection_from_key_date_range(table_name=base_key[0], \
                                                                        oem_name=base_key[1], \
                                                                        category=base_key[2], \
                                                                        key=base_key[3], \
                                                                        sub_key=base_key[4], \
                                                                        begin_date=base_key[6], \
                                                                        end_date=base_key[7], \
                                                                        db_conn=None)

    filter_collection_dict=helper_mysql.get_dict_of_raw_collection_from_key_date_range(table_name=filter_key[0], \
                                                                        oem_name=filter_key[1], \
                                                                        category=filter_key[2], \
                                                                        key=filter_key[3], \
                                                                        sub_key=filter_key[4], \
                                                                        begin_date=filter_key[6], \
                                                                        end_date=filter_key[7], \
                                                                        db_conn=None)

    result_collection_dict=dict((id,value) for id,value in base_collection_dict.iteritems() if filter_collection_dict.has_key(id))

    result_collection_pv=sum(result_collection_dict.values())
    result_collection_uv=len(result_collection_dict)
    result_collection_avg=(1.0*result_collection_pv/result_collection_uv) if result_collection_uv>0 else 0

    content='%s | %.2f' % (result_collection_uv,result_collection_avg)
except:
    content='- | -'
    

content='document.write("'+content+'");'

config.hide_sys_output(False)

#print "Content-type: application/x-unknown"
#print "Content-Disposition: attachment; filename=data.txt"
print "Content-type: application/javascript"
print "Cache-Control: max-age=31556926"
print "Cache-Control: public"
print "ETag: a_unique_version_string"
print "Last-Modified: Fri, 16 Mar 2007 04:00:25 GMT"

#print "Expires: Fri, 30 Oct 2014 14:19:41 GMT"
print "Content-Length: "+str(len(content))
print ''
print content



