#!C:\Python26\python.exe -u
import cgi
import cgitb; cgitb.enable()  # for troubleshooting
import gzip
import hashlib

def unzip_from_file(full_file_path):
    file_handler=gzip.open(full_file_path,"rb")
    content=file_handler.read()
    file_handler.close()
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


try:
    form = cgi.FieldStorage()

    collection_id = form.getvalue("collection_id", "1")
    hash_code = form.getvalue("hash_code", "")
    root_ip = form.getvalue("root_ip", "")
    if root_ip=='111':
        root_dir='\\\\192.168.0.111\\CollectionPool'
    elif root_ip=='158_2':
        root_dir=r'\\192.168.0.158\CollectionPool_2'
    else:
        root_dir='E:\\CollectionPool'

    if md5("SimPleKey"+str(collection_id))!=hash_code:
        #print 'Access denied.'
        #exit()
        pass

    file_path=get_directory_level(collection_id,root_dir)
    content=unzip_from_file(file_path).replace(',','\r\n')

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


