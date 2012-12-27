#!C:\Python26\python.exe -u
sys.path.append(r'E:\RoutineScripts')

import helper_sql_server
import helper_mysql
import cgi























import cgitb; cgitb.enable()  # for troubleshooting
import gzip
import hashlib

def unzip_from_file(full_file_path):
    content=''
    try:
        file_handler=gzip.open(full_file_path,"rb")
        content=file_handler.read()
        file_handler.close()
    except:
        pass
    return content

def md5(str):
    m=hashlib.md5()
    m.update(str)
    return m.hexdigest()


def get_directory_level(collection_id,root_dir='E:\\CollectionPool',step=3):
    file_name=str(collection_id)
    path=root_dir

    if not file_name:
        return path
    directory_levels=[file_name[i:i+step] for i in range(0, len(file_name)-step, step)]
    
    for level in directory_levels:
        path+='\\'+level

    return path+'\\'+str(collection_id)+'.zipped'

form = cgi.FieldStorage()

collection_ids = form.getvalue("collection_ids", "1822927|1806472|1790693")
hash_code = form.getvalue("hash_code", "")

whole_collection=set([])
element_existance_counters={}

for collection_id in collection_ids.split('|'):
    
    file_path=get_directory_level(collection_id)
    content=unzip_from_file(file_path)#.replace(',','\r\n')
    collection=set(content.split(','))
    whole_collection|=collection
    for i in collection:
        if not element_existance_counters.has_key(i):
            element_existance_counters[i]=0
        element_existance_counters[i]+=1


reversed_element_existance_counters={}

for element,existance in element_existance_counters.iteritems():
    if not reversed_element_existance_counters.has_key(existance):
        reversed_element_existance_counters[existance]=0
    reversed_element_existance_counters[existance]+=1

content='<tr><td>Times</td><td>UV</td><td>Percentage</td></tr>'

order=reversed_element_existance_counters.keys()[:]
order.sort()

for existance in order:
    content+='<tr><td>'+str(existance)+'</td><td>'+str(reversed_element_existance_counters[existance])+'</td><td>'+format(reversed_element_existance_counters[existance]*1.0/len(whole_collection), '.2%')+'</td></tr>'

content+='<tr><td>Total Unique</td><td>'+str(len(whole_collection))+'</td><td>100.00%</td></tr>'

content='document.write("<table border=0>'+content+'</table>");'

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


