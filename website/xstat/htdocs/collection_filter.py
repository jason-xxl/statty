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

collection_id=form.getvalue("collection_id", "3121468")

result_sql=form.getvalue("base_sql", r'''

select `text`

from raw_data_user_info a
left join value_text_dict b
on a.`value`=b.`id`

where `oem_name`="Vodafone" 
and `category`="moagent" 
and `key`="from_app_request_by_monet_id_user_agent_first_text_value" 
and `sub_key`='%s'

''')

result_sql_signature=form.getvalue("result_sql_signature", "")

collection=helper_mysql.get_raw_collection_by_id(collection_id)
#collection=list(collection)[0:10]

content=['<table style=\\"margin:0; padding:0; border:0; text-align:left;\\">']
for i in collection:
    tmp_sql=result_sql % (i,)
    result=helper_mysql.get_one_value_string(tmp_sql)
    content.append('<tr><td>'+str(i)+'</td><td><div style=\\"text-align:left;\\">'+str(result)+'</div></td></tr>')

content.append('</table>')    
    
content=''.join(content)
content='document.write("<div style=\\"float:left;\\">'+content+'</div>");'

#print "Content-type: application/x-unknown"
#print "Content-Disposition: attachment; filename=data.txt"
print "Content-type: application/javascript"
print "Content-Length: "+str(len(content))
print ''
print content

