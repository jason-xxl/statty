import os
import urllib
import urllib2
import cookielib
import gzip
import config
import helper_regex
import shutil

def remove_directory(path):
    shutil.rmtree(path, True)

def write_big_integer_set_to_file(full_file_path,data_set):
    file_handler=open(full_file_path,"w")
    for i in data_set:
        file_handler.write(str(i)+os.linesep)
    file_handler.close()

def read_big_integer_set_from_file(full_file_path):
    ret=set([])
    file_handler=open(full_file_path,"r")
    for line in file_handler:
        ret.add(int(line))
    file_handler.close()
    return ret

def write_big_string_set_to_file(full_file_path,data_set):
    file_handler=open(full_file_path,"w")
    for i in data_set:
        file_handler.write(str(i)+os.linesep)
    file_handler.close()

def read_big_string_set_from_file(full_file_path):
    ret=set([])
    file_handler=open(full_file_path,"r")
    for line in file_handler:
        ret.add(line.strip(os.linesep).strip())
    file_handler.close()
    return ret
        
def export_to_file(full_file_path,content):
    file_handler=open(full_file_path,"w")
    file_handler.write(content)
    file_handler.close()
        
def append_to_file(full_file_path,content):
    file_handler=open(full_file_path,"a")
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

def put_content_to_file(content,full_file_path):
    file_handler=open(full_file_path,"w")
    file_handler.write(content)
    file_handler.close()
    return content

    
def prepare_directory_on_windows(full_dir_path):
    levels=full_dir_path.strip('\\').split('\\')
    for i in range(1,len(levels)+1):
        p='\\'.join(levels[0:i])
        if not os.path.exists(p):
            os.makedirs(p)

def prepare_directory_level(file_full_name,root_dir=None,step=3):
    if not root_dir:
        root_dir=config.conn_stat_portal['collection_root_dir']

    file_name=helper_regex.extract(file_full_name,r'^\s*(.*?)(?:\.\w+)$')
    path=root_dir
    if not file_name:
        return path
    directory_levels=[file_name[i:i+step] for i in range(0, len(file_name)-step, step)]
    for level in directory_levels:
        print path
        path+='\\'+level
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except:
                pass
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
     
def put_zipped_collection(collection_id,element_str,suffix='.zipped',step=3,root_dir=None):
    directory=prepare_directory_level(file_full_name=str(collection_id)+suffix,step=step,root_dir=root_dir)
    path=directory+'\\'+str(collection_id)+suffix
    zip_to_file(path,element_str)
    print 'write collection file: '+path
    return path
     
def get_zipped_collection(collection_id,suffix='.zipped',step=3,root_dir=None):
    path=prepare_directory_level(file_full_name=str(collection_id)+suffix,step=step,root_dir=root_dir)+'\\'+str(collection_id)+suffix
    try:
        content=unzip_from_file(path)
        print 'read collection file: '+path
    except:
        content=''
        print 'read collection file (not exist): '+path

    return content
     
def delete_zipped_collection(collection_id,suffix='.zipped',step=3,root_dir=None):
    path=prepare_directory_level(file_full_name=str(collection_id)+suffix,step=step,root_dir=root_dir)+'\\'+str(collection_id)+suffix
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


file_size_pool={}

def get_cached_http_response_size(full_file_path):
    if not file_size_pool.has_key(full_file_path):
        size=get_http_response_size(full_file_path)
        file_size_pool[full_file_path]=size
    return file_size_pool[full_file_path]


def sort_file(full_file_path,translate_func=lambda line:line):
    line_set=read_big_string_set_from_file(full_file_path)
    try:
        ret_list=sorted([translate_func(line) for line in line_set])
    except:
        ret_list=sorted([line for line in line_set])
        print 'Error: translate error in processing ',full_file_path
    write_big_string_set_to_file(full_file_path,ret_list)

def sort_file_in_dir(base_path,name_pattern='(.)',translate_func=lambda line:line):
    target_files=get_filtered_file_list_from_dir_tree(base_path=base_path,name_pattern=name_pattern)
    counter=0
    for full_file_path in target_files:
        counter+=1
        print counter,full_file_path
        sort_file(full_file_path,translate_func=translate_func)
        

def merge_file(base_path,target_file_path,name_pattern='(.)'):
    target_files=get_filtered_file_list_from_dir_tree(base_path=base_path,name_pattern=name_pattern)
    remove_file(target_file_path)

    for f in target_files:
        print f
        append_to_file(target_file_path,get_content_from_file(f))
        
def remove_file(target_file_path):
    try:
        os.unlink(target_file_path)
        print 'remove file: '+target_file_path
    except:
        print 'remove file (not exist): '+target_file_path
    return target_file_path

def get_current_script_file_name():
    import os,sys
    return os.path.basename(sys.argv[0])


def copy(src_file, dst_file):
    import shutil, errno
    try:
        shutil.copy2(src_file, dst_file)
    except OSError as exc: # python >2.5
        if exc.errno == errno.ENOTDIR:
            shutil.copy(src_file, dst_file)
        else: raise


if __name__=='__main__':
    
    #sort_file_in_dir(base_path='E:\\TestData\\shabik_360_moagent\\2011-12-19\\',name_pattern='(.)',translate_func=helper_regex.format_date_time_moagent)
    merge_file(base_path='E:\\TestData\\shabik_360_moagent\\2011-12-20\\',target_file_path='E:\\TestData\\shabik_360_moagent\\2011-12-20\\all.txt',name_pattern='(\d{7,})')

    #sort_file(full_file_path='E:\\TestData\\shabik_360_moagent\\2011-11-28\\40105087.txt',translate_func=helper_regex.format_date_time_moagent)

    #print put_zipped_collection(collection_id=-1000,element_str=r'1,2,3,4,5,6,7,8,9,0')
    #print get_zipped_collection(collection_id=-10000)
    #print delete_zipped_collection(collection_id=-1000)
    #print prepare_directory_level(root_dir=config._conn_stat_portal_158_2['collection_root_dir'],file_full_name=r'123456789044.txt')
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
    #print get_sub_dir_list_from_dir(base_path=r'\\192.168.1.52\upload_log_client_symbian',name_pattern='(fdsf)')

    """
    s=set([])
    for i in range(5000000):
        s.add(i)
        
    write_big_integer_set_to_file('test.txt',s)
    print len(read_big_integer_set_from_file('test.txt'))
    """
    for i in range(10) :
        append_to_file('a.txt','abc\n')
