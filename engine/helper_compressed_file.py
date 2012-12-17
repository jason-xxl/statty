import os
import urllib
import helper_regex
import urllib2
import cookielib
import gzip
import config
import helper_regex
import helper_file


def zip_file_to_storage(source_file_smb_path,storage_root):

    target_path=storage_root.rstrip('\\')+'\\'+helper_regex.extract(source_file_smb_path,'\\(\\(?:[^\\]+\\)*)')
    file_name=helper_regex.extract(source_file_smb_path,'([^\\]+)$')
    
    helper_file.prepare_directory_on_windows(target_path)

    







def export_to_file(full_file_path,content):
    file_handler=open(full_file_path,"w")
    file_handler.write(content)
    file_handler.close()

def zip_to_file(full_file_path,content):
    file_handler=gzip.open(full_file_path,"wb")
    file_handler.write(content)
    file_handler.close()

def unzip_from_file(full_file_path):
    file_handler=gzip.open(full_file_path,"rb")
    content=file_handler.read()
    file_handler.close()
    return content

def get_content_from_file(full_file_path):
    file_handler=open(full_file_path,"r")
    content=file_handler.read()
    file_handler.close()
    return content

def prepare_directory_level(file_full_name,root_dir=config.conn_stat_portal['collection_root_dir'],step=3):
    file_name=helper_regex.extract(file_full_name,r'^\s*(.*?)(?:\.\w+)$')
    path=root_dir
    if not file_name:
        return path
    directory_levels=[file_name[i:i+step] for i in range(0, len(file_name)-step, step)]
    for level in directory_levels:
        path+='\\'+level
        if not os.path.exists(path):
            os.makedirs(path)
    return path


def get_file_size(full_file_path):
    return os.path.getsize(full_file_path)

def get_web_file_size(full_file_path):
    try:
        length=int(helper_regex.extract(str(urllib.urlopen(full_file_path).info()),r'Content\-Length:\s*(\d+)'))
    except:
        length=-1
    return length

def get_http_response_size(full_file_path):
    try:
        #print len(urllib.urlopen(full_file_path).info())
        #print str(urllib.urlopen(full_file_path).info())
        length=int(helper_regex.extract(str(urllib.urlopen(full_file_path).info()),r'Content\-Length:\s*(\d+)'))
    except:
        length=-1
    return length

def get_http_content(url):
    content=''
    try:
        response = urllib2.urlopen(url)
        content = response.read()
        print 'get_http_content: '+url
        #print content
    except:
        print 'get_http_content error: '+url
    return content

def get_http_content_with_cookie(url,cookies=None,referer=''):
    
    #cookies=[{'name':'Name', 'value':'1', 'domain':'www.example.com', 'path':'/', 'expires':None, 'port':None, 'port_specified':False, 'domain_specified':False, 'domain_initial_dot':False, 'path_specified':True, 'secure':False, 'discard':True, 'comment':None, 'comment_url':None, 'rest':{'HttpOnly': None}, 'version':0, 'rfc2109':False}]

    content=''
    cj = cookielib.CookieJar()
    if cookies:
        for args in cookies:
            cookie_tpl={'name':'L', 'value':'en', 'domain':'morange.com', 'path':'/', 'expires':None, 'port':None, 'port_specified':False, 'domain_specified':False, 'domain_initial_dot':False, 'path_specified':True, 'secure':False, 'discard':True, 'comment':None, 'comment_url':None, 'rest':{'HttpOnly': None}, 'version':0, 'rfc2109':False}
            args=dict(cookie_tpl,**args)
            ck = cookielib.Cookie(**args)
            cj.set_cookie(ck)

    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    opener.addheaders = [
                        ('User-Agent', 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.2.11) Gecko/20101012 Firefox/3.6.11'),
                        ('Referer', referer),
                        ]
    response = opener.open(url)
    content = response.read()
    print 'get_http_content_with_cookie: '+url
    try:
        pass
        #print content
    except:
        print 'get_http_content_with_cookie error: '+url
    return content
     
def put_zipped_collection(collection_id,element_str,suffix='.zipped'):
    directory=prepare_directory_level(file_full_name=str(collection_id)+suffix)
    path=directory+'\\'+str(collection_id)+suffix
    zip_to_file(path,element_str)
    print 'write collection file: '+path
    return path
     
def get_zipped_collection(collection_id,suffix='.zipped'):
    path=prepare_directory_level(file_full_name=str(collection_id)+suffix)+'\\'+str(collection_id)+suffix
    try:
        content=unzip_from_file(path)
        print 'read collection file: '+path
    except:
        content=''
        print 'read collection file (not exist): '+path

    return content
     
def delete_zipped_collection(collection_id,suffix='.zipped'):
    path=prepare_directory_level(file_full_name=str(collection_id)+suffix)+'\\'+str(collection_id)+suffix
    try:
        os.unlink(path)
        print 'remove collection file: '+path
    except:
        print 'remove collection file (not exist): '+path
    return path

def get_filtered_file_list_from_dir_tree(base_path=os.curdir,name_pattern='(.)'):
    base_path=base_path.rstrip('/').rstrip('\\')
    result_file_list=[]
    for path, dirs, files in os.walk(os.path.abspath(base_path)):
        #print path, dirs, files
        for file_name in files:
            if helper_regex.extract(path+r'\\'+file_name,name_pattern):
                result_file_list.append(os.path.join(path, file_name))
    return result_file_list


def get_sub_dir_list_from_dir(base_path=os.curdir,name_pattern='(.)'):
    base_path=base_path.rstrip('/').rstrip('\\')
    result_sub_dir_list=[]
    dirs = [name for name in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, name))]
    for d in dirs:
        if helper_regex.extract(os.path.join(base_path, d),name_pattern):
            result_sub_dir_list.append(os.path.join(base_path, d))
    return result_sub_dir_list


if __name__=='__main__':
    #print put_zipped_collection(collection_id=-1000,element_str=r'1,2,3,4,5,6,7,8,9,0')
    #print get_zipped_collection(collection_id=-10000)
    #print delete_zipped_collection(collection_id=-1000)
    #print prepare_directory_level(file_full_name=r'123456789044.txt')
    #print get_file_size(r'e:\RoutineScripts\helper_file.py')
    #print get_web_file_size(r'http://shabik.net.sa/download/shabik.jar')
    #print get_http_response_size(r'http://shabik.net.sa/download/shabik-5.sisx')
    #print get_http_content(r'http://192.168.1.52:7810/?action=getTotalAccount&accounttype=1')
    """
    print get_http_content_with_cookie(r'http://angel.morange.com/rrd/img/149/download?start=2011-05-02&end=2011-05-04',cookies=[
            {'name':'L', 'value':'en', 'domain':'morange.com', 'path':'/'},
            {'name':'csrftoken', 'value':'5f842bf4da706e5edb71937b65ec3bf3', 'domain':'angel.morange.com', 'path':'/'},            
            {'name':'sessionid', 'value':'b7d24a66b7ab78f81c6cc942081bc733', 'domain':'angel.morange.com', 'path':'/'},
        ],referer='http://angel.morange.com/rrd/149/widget/192/graph')
    """
    #2011-03-27 16:00:13 GET /download/shabik-5.sisx - 212.118.143.146 HTTP/1.1 Mozilla/5.0+(SymbianOS/9.4;+U;+Series60/5.0+Nokia5800d-1/21.0.025;+Profile/MIDP-2.1+Configuration/CLDC-1.1+)+AppleWebKit/413+(KHTML,+like+Gecko)+Safari/413 http://shabik.net.sa/download.aspx 200 516503 112466

    #print get_filtered_file_list_from_dir_tree(base_path='\\\\192.168.1.52\\upload_log_client_symbian\\',name_pattern='(\\\\2011\\-07\\-21\\\\)')
    print get_sub_dir_list_from_dir(base_path=r'\\192.168.1.52\upload_log_client_symbian',name_pattern='(fdsf)')
    

    prepare_directory_on_windows(full_dir_path='c:\\test\\test\\set\\')


