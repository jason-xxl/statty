import socket
import os, sys

ENGINE_ROOT = os.path.join(os.path.dirname(__file__),'../engine')
sys.path.insert(0, os.path.join(ENGINE_ROOT, "."))

import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

day_to_update_stat=1

"""
ip_win=socket.gethostbyname(socket.gethostname())

ip_linux=''
tmp_socket= socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
tmp_socket.connect(("google.com",80))
ip_linux=tmp_socket.getsockname()[0]
tmp_socket.close()

day_to_update_stat=1
hour_to_update_stat=1

default_hour=' 05:00:00'

if "MAIN_RUN_DAYS" in os.environ:
    day_to_update_stat = int(os.environ["MAIN_RUN_DAYS"])

if ip_win=='192.168.0.158':
    execute_dir=r'E:\RoutineScripts'
    default_filter_path="E:\\RoutineScripts\\user_id_filter\\"

elif ip_win=='192.168.0.157':
    execute_dir=r'D:\RoutineScripts'
    default_filter_path="D:\\RoutineScripts\\user_id_filter\\"

elif ip_win=='192.168.1.47' or ip_win=='192.168.1.55':
    execute_dir=r'C:\RoutineScripts'
    default_filter_path="C:\\RoutineScripts\\user_id_filter\\"

elif ip_win=='192.168.12.104':
    execute_dir=r'C:\RoutineScripts'
    default_filter_path="C:\\RoutineScripts\\user_id_filter\\"

elif ip_linux=='192.168.6.111':
    execute_dir=r'/home/mozat/RoutineScripts'
    default_filter_path="/home/mozat/RoutineScripts/user_id_filter/"

else:
    execute_dir=r'.'
    default_filter_path=".\\user_id_filter\\"
"""


execute_dir=ENGINE_ROOT
#default_filter_path="/home/mozat/RoutineScripts/user_id_filter/"

collection_filename_pattern=r'(\.\.\/engine\/\.\/\.\.\/engine\/\.\.\/data\/collection\\\d+\.zipped)'
#../engine/./../engine/../data/collection\\733\\741\\7337416.zipped
#collection_filename_pattern=r'(\.\.\/engine\/\.\/\.\.\/engine\/\.\.\/data\/collection\\\d+\\\d+\\\d+\.zipped)'
#tmp_download_dir=r'\\192.168.0.158\WebStatShare\StatPortalDownload'

#print execute_dir


#stat_db_host='192.168.0.158'
#stat_db_user='admin'
#stat_db_pwd='admin'
#stat_db_name='mozat_stat'
#crawl_db_name='mozat_clustering'


#collection cache

collection_cache_enabled=False
collection_cache={}
#collection_root_dir=os.path.join(ENGINE_ROOT, "../data")

#internal db

_conn_stat_portal_158={
    'host':'localhost',
    'account':'root',
    'pwd':'gumi.asia123',
    'db':'sttaty',
    'collection_root_dir':os.path.join(ENGINE_ROOT, "../data/collection"),
    'named_collection_root_dir':os.path.join(ENGINE_ROOT, "../data/named_collection"),
    'db_type':'mysql',
}

conn_stat_portal = {
    'host':'localhost',
    'account':'root',
    'pwd':'gumi.asia123',
    'db':'statty_data',
    'collection_root_dir':os.path.join(ENGINE_ROOT, "../data/collection"),
    'named_collection_root_dir':os.path.join(ENGINE_ROOT, "../data/named_collection"),
    'db_type':'mysql',
}

conn_stat_gumi_live = {
    'host':'localhost',
    'account':'root',
    'pwd':'gumi.asia123',
    'db':'gumi_live',
    'collection_root_dir':os.path.join(ENGINE_ROOT, "../data/collection"),
    'named_collection_root_dir':os.path.join(ENGINE_ROOT, "../data/named_collection"),
    'db_type':'mysql',
}

mail_from='jason.xu@gumi.sg'
mail_targets=['jason.xu@gumi.sg']
smtp_server='smtp.gmail.com'

def default_exception_handler(type, value, tb):
    import helper_mail
    import helper_regex
    import traceback

    title='stat python exception: '+helper_regex.get_script_file_name()
    content='<br/>'.join(traceback.format_exception(type, value, tb))
    #print content

    helper_mail.send_mail(title=title,content_html=content)

sys.excepthook = default_exception_handler

def d(obj,obj_name=''):
    print 'exit:',obj_name,str(obj)
    exit()

def get_file_path_with_correct_slash(path):
    if 'linux' in sys.platform:
        return path.replace('\\','/')
    return path

url_sign_key='fhdjslhfj'

def main_execute(stat_py_filenames):
    import os,time
    global execute_dir
    os.chdir(execute_dir)

    current_date=time.strftime('%Y-%m-%d')
    if isinstance(stat_py_filenames,str):
        stat_py_filenames=[stat_py_filenames]

    for f in stat_py_filenames:
        command=f
        if not '>' in command: 
            tf = 'log\\'+current_date+'\\'+f+'.log'
            folder = os.path.split(tf)[0]
            if not os.path.exists(folder):
                print('create folder: {0}'.format(folder))
                os.makedirs(folder)
            
            command=f+'>'+tf #'log\\'+current_date+'\\'+f+'.log'
        print command
        os.system(command)

standard_output=None
def hide_sys_output(hide_output=True):
    global standard_output
    import sys
    if hide_output and not standard_output:
        standard_output=sys.stdout
        from cStringIO import StringIO 
        sys.stdout=StringIO()
    elif not hide_output and standard_output:
        sys.stdout=standard_output
        standard_output=None


conn_stat_portal_redis={    
    'host':'192.168.0.111',
    'port':6379,
    'account':'',
    'pwd':'',
    'db':0,
    'db_type':'redis',
}




if __name__=='__main__':
    
    #main_execute('common_ais.py>common_ais.py.log')
    pass

