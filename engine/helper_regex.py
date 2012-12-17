import re
from datetime import datetime
import config
import time
from datetime import date
from datetime import timedelta
import os
import sys
import traceback
import helper_ip
import helper_math
import unicodedata, re
import urllib

_day=r"(\d{4}-\d{2}-\d{2})"
_dayHour=r"(\d{4}-\d{2}-\d{2} \d{2})"

app_page_pattern=r'(\.aspx|\.ashx|\.php|\.erl|\.jsp|\.html|\.htm|\/\w+(?:\/\s|\s|\/$|$|\?|\/+\?))'
client_file_pattern=r'(\.jad|\.jar|\.cod|\.apk|\.sis|\.sisx|\.plist)[ignorecase]'

def is_app_page(line):
    line=extract(line,r'(http:\/\/.*?)\s').lower()
    if line.find('/staticfile') \
        +line.find('/image') \
        +line.find('/img') \
        +line.find('.gif') \
        +line.find('.jpeg') \
        +line.find('.jpg') \
        +line.find('.png')>-7:
        
        return ''
    return extract(line,app_page_pattern)

re_cache={}

apps_mapping_rule=[ # 0: search on domain 1: path + script_name 2: query string

    ("client_prefetch",2,"isprefetch=1"),

    ("web_api",1,"webapi/"),
    ("web_api",0,"mozoneapi"),
    ("ocean_age",0,"oa."),
    ("ocean_age_world",0,"oaw."),
    ("matrix",0,"matrix-interface-mozat."),
    ("matrix",0,"matrix-web"),
    ("baloot",0,"baloot."),
    ("football_war",0,"football."),
    ("football_war",0,"footballwar."),
    ("status",0,"status."),
    ("twitter",0,"motwitter."),
    ("twitter",0,"twitter."),
    ("netlog",0,"netlog"),
    ("rss",0,"rss"),
    ("twitter",0,"twitter."),
    ("youtube",0,"youtube"),
    ("facebook",0,"facebook"),
    ("gomoku",0,"gomoku"),
    ("happy_barn",0,"happybarn"),
    ("flickr",0,"flickr"),
    ("email",0,"momail"),
    ("billing",0,"billing"),
    ("invite",0,"invitation"),
    ("aladdin",0,"aladdin"),
    ("heroes",0,"heroes"),

    ("photo_server",1,"/staticfile"),
    ("photo_server",1,"/image"),
    ("photo_server",1,"/img"),
    ("photo_server",1,".gif"),
    ("photo_server",1,".jpeg"),
    ("photo_server",1,".jpg"),
    ("photo_server",1,".png"),
    ("photo",1,"photo/"),
    
    ("friend",1,"mobile_sone.aspx",2,"action=search"),
    ("new_user_wizard",1,"setupimaccount_t"),
    ("profile",1,"mobile_sone.aspx",2,"action=sone_"),
    ("friend",1,"mobile_sone.aspx",2,"userid="),
    ("profile",1,"mobile_sone.aspx"),
    ("friend",1,"tellfriend.aspx"),
    ("status",1,"view_item"),
    ("location",1,"/lookaround/"),
    ("location",1,"api",2,"action=lookaround"),
    ("location",1,"api",2,"action=sendmsgaddfriend"),
    ("new_user_wizard",1,"addfriends_t.aspx"),
    ("help",1,"faq_"),
    ("status",1,"filtering.aspx"),
    ("status",1,"filtering_action.aspx"),
    #("banner",1,"mobile_banner"),
    ("message",1,"tab_msgbox"),
    ("profile",1,"my_activity"),
    ("im",1,"setupimaccount"),
    ("friend",1,"tab_discover"),
    ("profile",1,"mobileweb/profile/tabdiscover"),
    ("status",1,"tab_system"),
    ("profile",1,"batchprofile"),
    ("ocean_age",1,"/oceanage"),
    ("texas_holdem",1,"/texasholdem"),
    ("message",1,"_msg.aspx"),
    ("event",1,"_myEvents.aspx"),
    ("notification",1,"_requests.aspx"),
    ("app_center",1,"accessory.aspx"),
    ("app_center",1,"mobileweb/appstore"),
    ("invite",1,"aftersmsinvite"),
    ("photo",1,"album.aspx"),
    ("billing",1,"billing.aspx"),
    ("friend",1,"bindphone_sent.aspx"),
    ("browser",1,"browser_hotlinks.aspx"),
    ("profile",1,"change_password"),
    ("message",1,"conversation.aspx"),
    ("event",1,"events"),
    ("friend",1,"explorepeople.aspx"),
    ("new_user_wizard",1,"facebooktwitter_t.aspx"),
    ("facebook",1,"facebook"),
    ("flickr",1,"flickr"),
    ("football_war",1,"footballwar.i."),
    ("friend",1,"friend.morange.com"),
    ("friend",1,"friends.aspx"),
    ("friend",1,"mobileweb/profile/searchform"),
    ("friend",1,"mobileweb/profile/search"),
    ("friend",1,"/friendship/"),
    ("friend",1,"gettingstartfindingfrienddetails"),
    ("invite",1,"gettingstartsmsinviteredirect"),
    ("gomoku",1,"gomoku"),
    ("invite",1,"invite_outer_user"),
    ("greeting_cards",1,"jit.mozat.net/mobilecards"),
    ("leader_board",1,"lbtelk"),
    ("linkedin",1,"linkedin"),
    ("message",1,"mobile_4tab_msgBox"),
    ("app_center",1,"mobile_allapps"),
    ("app_center",1,"mobile_app"),
    ("app_center",1,"mobile_app_setting"),
    ("profile",1,"mobile_changelang"),
    ("profile",1,"mobile_changeprivacy"),
    ("chatroom",1,"mobile_chatroom"),
    ("chatroom",1,"mobile_chatroom_settings"),
    ("circle",1,"mobile_circle"),
    ("circle",1,"mobile_circlemember"),
    ("circle",1,"mobile_circlephoto"),
    ("app_center",1,"mobile_dock_setting"),
    ("profile",1,"mobile_editprofile"),
    ("email",1,"mobile_email"),
    ("circle",1,"mobile_forum"),
    ("help",1,"mobile_help"),
    ("homepage_old_version",1,"mobile_homepage"),
    ("setting",1,"mobile_homepage_setting"),
    ("im",1,"mobile_im"),
    ("invite",1,"mobile_invitation"),
    ("invite",1,"mobile_invitation_phonecontact"),
    ("message",1,"mobile_message"),
    ("saying",1,"mobile_moblog"),
    ("mochat",1,"mobile_mochat"),
    ("photo",1,"mobile_mophoto"),
    ("event",1,"mobile_newsfeeds"),
    ("notification",1,"mobile_notification"),
    ("notification",1,"unreadnoticount"),
    ("profile",1,"mobile_personaleventprivacy"),
    ("phone_backup",1,"mobile_phonebook"),
    ("poke",1,"mobile_poke"),
    ("poll",1,"mobile_poll"),
    ("profile",1,"mobile_privacysetting"),
    ("level_system",1,"mobile_rank"),
    ("level_system",1,"levelsys/"),
    ("recent_visitor",1,"lastvisits"),
    ("recent_visitor",1,"mobile_recent_visitors"),
    #("recent_visitor",1,"webapi/profile/lastvisit"),
    ("friend",1,"mobile_recommand"),
    ("setting",1,"mobile_select_timezone"),
    ("profile",1,"mobile_setting"),
    ("friend",1,"mobile_sone"),
    #("like",1,"webapi/like/"),
    ("star_user",1,"mobile_star"),
    ("app_center",1,"mobile_store"),
    ("email",1,"momail"),
    ("photo",1,"mophoto_album"),
    ("photo",1,"mophoto_albumlist"),
    ("photo",1,"mophoto_comments"),
    ("photo",1,"mophoto_create_album"),
    ("photo",1,"mophoto_edit_album"),
    ("photo",1,"mophoto_edit_photo"),
    ("photo",1,"mophoto_module"),
    ("photo",1,"mophoto_more"), #public_photo
    ("photo",1,"mophoto_multiphotos"),
    ("photo",1,"mophoto_photo"),
    #("photo",1,"mophoto_popular_photos"), # also called cool_photo
    ("hot_photo",1,"mophoto_popular_photos"), # also called cool_photo
    ("photo",1,"mophoto_public_photo"), # public_photo
    ("photo",1,"mophoto_submit"),
    ("photo",1,"mophoto_upload_photo"),
    ("message",1,"msgbox.aspx"),
    ("friend",1,"multiuser.aspx"),
    ("photo",1,"myphoto.aspx"),
    ("poll",1,"mypoll.aspx"),
    ("saying",1,"mysaying.aspx"),
    ("netlog",1,"netlog"),
    ("new_user_wizard",1,"new_user_welcome"),
    ("photo",1,"photo.aspx"),
    ("profile",1,"profile.aspx"),
    ("rss",1,"rss"),
    ("invite",1,"smsinvite"),
    ("app_center",1,"tab_apps"),
    ("event",1,"tab_events"),
    ("homepage",1,"tab_home"),
    ("homepage",1,"mobileweb/home/"),
    ("homepage",1,"mobileweb/gridhomepage"),
    ("profile",1,"tab_me"),
    ("profile",1,"mobileweb/profile/tabme"),
    #("profile",1,"webapi/profile/"),
    ("notification",1,"tab_notification"),
    ("youtube",1,"youtube"),
    ("help",1,"tab_feedback"),
    #("help",1,"webapi/feedbacksurvey/"),
    #("comment",1,"webapi/comment/"),
    #("banner",1,"webapi/banner/"),
    ("star_user",1,"mobileweb/userstar"),
    ("register",1,"mobile_post_reg"),
    ("status",1,"viewitem.ashx"),
    ("status",1,"blockitem.ashx"),
    ("status",1,"mobileweb/status/"),
    ("profile",1,"mobileweb/profile/"),
    ("billing",0,"tab-overdue"),
    #("status",1,"webapi/status/"),
    ("aladdin",1,"mobile_banner"),

]
 

def export_app_name():
    app_set=set([])
    for i in apps_mapping_rule:
        app_set.add(i[0])
    print "','".join(app_set)


def recognize_app_from_moagent_log_line(line):
    
    #prepare
    line=line.lower()
    _domain_start=line.find('http://')
    if _domain_start==-1:
        return 'unrecognized'
    if line.find('//',_domain_start)>=-1:
        line=line.replace(r'//',r'/')
        _domain_start+=6
    else:
        _domain_start+=7
    _path_start=line.find('/',_domain_start)
    if _path_start==-1:
        return 'unrecognized'
    #_path_start+=1 #to include the leading '/'
    _query_string_start=line.find('?',_path_start)
    _end=line.find(' ',_path_start)
    if _query_string_start==-1:
        _query_string_start=_end

    #apply rule
    
    for rule in apps_mapping_rule:
        _match=True
        for i in range(1,len(rule),2):
            if rule[i]==0 and line.find(rule[i+1],_domain_start,_path_start)==-1:
                _match=False
                break
            elif rule[i]==1 and line.find(rule[i+1],_path_start,_query_string_start)==-1:
                _match=False
                break
            elif rule[i]==2 and line.find(rule[i+1],_query_string_start,_end)==-1:
                _match=False
                break
        if _match:
            return rule[0]

    return 'unrecognized'


def recognize_client_version_number(line):
    version=extract(line,r'&userAgent=[^&=]*Morange%2F(\d+)[ignorecase]')
    if version and version=='6':
        return '6'
    return '5'

def recognize_client_version_type_name(line):
    if extract(line,r'userAgent=[^&=]*?(CS60)[ignorecase]'):
        return 'CS60'
    if extract(line,r'userAgent=[^&=]*?(CJME)[ignorecase]'):
        return 'CJME'
    if extract(line,r'userAgent=[^&=]*?(CAndroid)[ignorecase]'):
        return 'CAndroid'
    if extract(line,r'userAgent=[^&=]*?(CiOS)[ignorecase]'):
        return 'CiOS'
    if extract(line,r'userAgent=[^&=]*?(CBB)[ignorecase]'):
        return 'CBB'
    if extract(line,r'userAgent=[^&=]*?(CWM)[ignorecase]'):
        return 'CWM'
    return 'unknown'

def recognize_client_version_type_int_value(line):
    if extract(line,r'userAgent=[^&=]*?(CS60)[ignorecase]'):
        return 1
    if extract(line,r'userAgent=[^&=]*?(CJME)[ignorecase]'):
        return 2
    if extract(line,r'userAgent=[^&=]*?(CAndroid)[ignorecase]'):
        return 3
    if extract(line,r'userAgent=[^&=]*?(CiOS)[ignorecase]'):
        return 4
    if extract(line,r'userAgent=[^&=]*?(CBB)[ignorecase]'):
        return 5
    if extract(line,r'userAgent=[^&=]*?(CWM)[ignorecase]'):
        return 6
    return 0


def recognize_phone_platform_name(line):
    if extract(line,r'userAgent=[^&=]*?(S60)[ignorecase]'):
        return 'S60'
    if extract(line,r'userAgent=[^&=]*?(JConf|JProf)[ignorecase]'):
        return 'J2ME'
    if extract(line,r'userAgent=[^&=]*?(Android)[ignorecase]'):
        return 'Android'
    if extract(line,r'userAgent=[^&=]*?(iOS)[ignorecase]'):
        return 'iOS'
    if extract(line,r'userAgent=[^&=]*?(CBB|RIM|BlackBerry)[ignorecase]'):
        return 'BlackBerry'
    if extract(line,r'userAgent=[^&=]*?(WinCE)[ignorecase]'):
        return 'WinCE'
    if extract(line,r'userAgent=[^&=]*?(DotNETCF)[ignorecase]'):
        return 'DotNETCF'
    return 'unknown' 


def recognize_phone_platform_int_value(line):
    if extract(line,r'userAgent=[^&=]*?(S60)[ignorecase]'):
        return 1
    if extract(line,r'userAgent=[^&=]*?(JConf|JProf)[ignorecase]'):
        return 2
    if extract(line,r'userAgent=[^&=]*?(Android)[ignorecase]'):
        return 3
    if extract(line,r'userAgent=[^&=]*?(iOS)[ignorecase]'):
        return 4
    if extract(line,r'userAgent=[^&=]*?(CBB|RIM|BlackBerry)[ignorecase]'):
        return 5
    if extract(line,r'userAgent=[^&=]*?(WinCE)[ignorecase]'):
        return 6
    if extract(line,r'userAgent=[^&=]*?(DotNETCF)[ignorecase]'):
        return 7
    return 0

def extract_useragent_key(line,flags=[1,2,4,8,16,32,64,128,256,512,1024]):
    ret=extract(line,r'\buserAgent=(.*?)[&\n$=][ignorecase]')
    if not ret:
        return ''
    ret=ret.replace(r'%2F',r'/').replace(r'%2f',r'/').replace(r'%40',r'@').replace(r'%20',r' ').replace(r'+',r' ')
    ret=regex_replace(r' PI/\w+','',ret)
    caps=extract(ret,r' Caps/(\d+)')
    if caps:
        ret=regex_replace(r' Caps/\w+','',ret)
        try:
            caps=int(caps)
        except:
            traceback.print_stack()
            print line
            print ret
            raise Excetion('extract_useragent_key failed')
            exit()
        suffix=' Caps/|'
        for i in range(len(flags)):
            if caps & flags[i]:
                suffix+=str(i+1)+'|'
        ret+=suffix

    return ret.lower()

def extract(source,regex):
    if not re_cache.has_key(regex):
        if isinstance(regex,str) and regex.endswith('[ignorecase]'):        
            re_cache[regex]=re.compile(regex.replace('[ignorecase]',''),re.IGNORECASE)
        else:
            re_cache[regex]=re.compile(regex)
        
    m=re_cache[regex].search(source)
    if m and m.group(1):
        return m.group(1)
    else:
        return ""

def merge_keys_sorted(dict,seperator='_'):
    if not dict:
        return ''
    keys=dict.keys()
    keys.sort()
    keys = [k for k in keys if k]# remove empty keys
    return seperator.join(keys)

"""
def recognize_app_from_url(url):
    url=url.lower()
    if url.find('isprefetch=1')>-1:
        return 'client_prefetch'

    #for k,v in apps.iteritems():
    #    if url.find(k)>-1:
    #        return v

    for i in apps_mapping_rule:
        qmark=url.find('?')
        if i[0]==0:
            if url.find(i[1],0,None if qmark==-1 else qmark)>-1:
                return i[2]
        elif i[0]==1 and qmark!=-1:
            if url.find(i[1],qmark)>-1:
                return i[2]
        
    #return extract(url,r'http://([^\?]+)')
    return 'unrecognized'
    pass    
"""

def get_time_str_now():
    return time.strftime('%Y-%m-%d %H:%M:%S')

def get_date_str_now():
    return time.strftime('%Y-%m-%d')

def get_time_stamp(source):

    date_time_str=extract(source,r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
    if date_time_str:
        return time.mktime(time.strptime(date_time_str,'%Y-%m-%d %H:%M:%S'))
    return -1


def get_time_stamp_ms(source):
    ret=-1
    date_time_str=extract(source,r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
    if date_time_str:
        ret=time.mktime(time.strptime(date_time_str,'%Y-%m-%d %H:%M:%S'))
    
    ms_str=extract(source,r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},(\d{3})')
    if ms_str:
        #print 0.001*int(ms_str)
        ret+=0.001*int(ms_str)
    return ret


def generate_date_range_for_raw_data_query(day_range=70):
    current_date=get_date_str_now()
    date_array=[]
    for i in range(0,day_range):
        date_temp=date_add(current_date,-i)
        date_array.append('"'+date_temp+'"')
    date_sql='and `date` in ('+','.join(date_array)+')'
    return date_sql

def generate_date_range_for_data_query(day_range=70):
    current_date=get_date_str_now()
    date_array=[]
    for i in range(0,day_range):
        date_temp=date_add(current_date,-i)
        date_array.append(date_temp.replace('-',''))
    date_sql='and `date` in ('+','.join(date_array)+')'
    return date_sql

def get_time_clock_time():
    return time.clock()

def get_script_file_name():
    return sys.argv[0]


re_action_value=re.compile(r'\baction=')
re_src_value=re.compile(r'\bsrc=')
re_isprefetch_value=re.compile(r'\bisprefetch=1')
re_evflg_value=re.compile(r'\bevflg=')

re_digit=re.compile(r'(\d+)')
re_slash=re.compile(r'(\/{2,})')


re_url=re.compile(r'http://([^\s]+)')
re_before_http=re.compile(r'(monetid|moclientwidth|userAgent|moclientheight|cli_ip)?=[^&$]*&?')

#re_clear=re.compile(r'(monetid|moclientwidth|userAgent|moclientheight|cli_ip)&?')

re_url_path=re.compile(r'http://([^\s\?#]+)')
re_url_query_string=re.compile(r'http://[^\s\?]+\?([^\s#]+)')
re_url_arg_name=re.compile(r'(?:\?|&)([\w_]+)')

query_args_excluded=('monetid','moclientwidth','moclientheight','devicewidth','deviceheight','cli_ip','useragent')

def get_url_unique_key(line):
    # 08 Apr 17:27:09,956	7678648	406	390	16	-1	http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147
    
    key=extract(line,re_url)
    key=re.sub(re_action_value,r'action_\1',key,1)
    key=re.sub(re_before_http,'',key).lower()
    #line=re.sub(re_clear,'',line)
    return key

def get_simplified_url_unique_key(line):
    # 08 Apr 17:27:09,956	7678648	406	390	16	-1	http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147
    # 08 Apr 17:27:09,956	7678648	406	390	16	-1	http://mobileshabik.morange.com/mobile_homepage/13032541
    line=line.lower()
    path=extract(line,re_url_path)
    path=re.sub(re_digit,r'[digits]',path)

    query_arg=line

    query_arg=re.sub(re_isprefetch_value,r'isprefetch_1',query_arg,1)
    query_arg=re.sub(re_evflg_value,r'evflg_',query_arg,1)
    query_arg=re.sub(re_action_value,r'action_',query_arg,1)
    query_arg=re.sub(re_src_value,r'src_',query_arg,1)
    query_arg=re.sub(re_slash,r'/',query_arg)

    matches=re.finditer(re_url_arg_name,query_arg)
    keys=set([match.group(1) for match in matches if not match.group(1) in query_args_excluded])

    ret=path+'?'+'&'.join(keys)
    ret=re.sub(re_slash,r'/',ret)

    return ret


def extract_complete_time_key(original_key):
    key,date_key=extract_complete_time(original_key)
    if date_key:
        return key,date_key
        
    key,date_key=extract_date_hour(original_key)
    if date_key:
        return key,date_key
    
    key,date_key=extract_date(original_key)
    return key,date_key



def extract_date_hour_key(original_key):
    key,date_key=extract_date_hour(original_key)
    if not date_key:
        key,date_key=extract_date(original_key)
    return key,date_key


def extract_log_date(line):
    #line='2011-06-10 05:19:50,220 offline: 55505991'
    match=extract(line,r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?)')
    if not match:
        line=format_date_time_moagent(line)
        match=extract(line,r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?)')
    return match

"""

def extract_date(str):
    #str='data_2010-04-03_data'
    #str='zzz_08 Apr_ccc'
    #str='zzz_May 23_ccc'
    match=''
    ret=extract(str,r'(\d{4}-\d{2}-\d{2})')
    if not ret:
        ret=extract(str,r'(\d{2} [A-Za-z]{3,4})')
        if ret:
            match=ret
            ret+=' '+datetime.fromtimestamp(time.time()-3600*24).strftime('%Y')
            ret=datetime.strptime(ret, '%d %b %Y').strftime('%Y-%m-%d')
        else:
            ret=extract(str,r'([A-Za-z]{3,4} \d{2})')
            if ret:
                match=ret
                ret+=' '+datetime.fromtimestamp(time.time()-3600*24).strftime('%Y')
                ret=datetime.strptime(ret, '%b %d %Y').strftime('%Y-%m-%d')

    else:
        match=ret

    if match:
        str=str.replace(match,'')
        if str.startswith('_'): str=str[1:]
        if str.endswith('_'): str=str[:-1]
        str=str.replace('__','_')
        
    return str,ret

def extract_date_hour(str):
    #str='data_2010-04-03 12_data'
    #str='zzz_08 Apr 12_ccc'
    #str='zzz_May 23 23_ccc'
    match=''
    ret=extract(str,r'(\d{4}-\d{2}-\d{2} \d{2})')
    if not ret:
        ret=extract(str,r'(\d{2} [A-Za-z]{3,4} \d{2})')
        if ret:
            match=ret
            date=extract(str,r'(\d{2} [A-Za-z]{3,4}) \d{2}')+' '+datetime.fromtimestamp(time.time()-3600*24).strftime('%Y')
            ret=datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d')
            ret+=' '+extract(str,r'\d{2} [A-Za-z]{3,4} (\d{2})')
        else:
            ret=extract(str,r'([A-Za-z]{3,4} \d{2} \d{2})')
            if ret:
                match=ret
                date=extract(str,r'([A-Za-z]{3,4} \d{2}) \d{2}')+' '+datetime.fromtimestamp(time.time()-3600*24).strftime('%Y')
                ret=datetime.strptime(date, '%b %d %Y').strftime('%Y-%m-%d')
                ret+=' '+extract(str,r'[A-Za-z]{3,4} \d{2} (\d{2})')

    else:
        match=ret

    if match:
        str=str.replace(match,'')
        if str.startswith('_'): str=str[1:]
        if str.endswith('_'): str=str[:-1]
        str=str.replace('__','_')
        
    return str,ret

"""




def extract_date(str):
    
    #str='data_2010-04-03_data'
    #str='zzz_08 Apr_ccc'
    #str='_zzz_May 23_ccc_'
    
    match=''
    ret=extract(str,r'((?:19|20)\d{2}-[01]\d-[0123]\d)')
    if not ret:
        ret=extract(str,r'([0123]\d (?:ja|fe|ma|ap|ma|ju|ju|au|se|oc|no|de)[A-Za-z]{1,2})[ignorecase]')
        if ret:
            match=ret
            ret+=' '+datetime.fromtimestamp(time.time()-3600*24).strftime('%Y')
            ret=datetime.strptime(ret, '%d %b %Y').strftime('%Y-%m-%d')
        else:
            ret=extract(str,r'((?:ja|fe|ma|ap|ma|ju|ju|au|se|oc|no|de)[A-Za-z]{1,2} [0123]\d)[ignorecase]')
            if ret:
                match=ret
                ret+=' '+datetime.fromtimestamp(time.time()-3600*24).strftime('%Y')
                ret=datetime.strptime(ret, '%b %d %Y').strftime('%Y-%m-%d')

    else:
        match=ret

    str=str.replace(match,'').strip('_').replace('__','_')
    return str,ret

def extract_date_hour(str):
    
    #str='data_2010-04-03 12_data'
    #str='zzz_08 Apr 12_ccc'
    #str='zzz_May 23 23_ccc'
    
    match=''
    ret=extract(str,r'((?:19|20)\d{2}-[01]\d-[0123]\d (?:0\d|1\d|20|21|22|23))')
    if not ret:
        ret=extract(str,r'([0123]\d (?:ja|fe|ma|ap|ma|ju|ju|au|se|oc|no|de)[A-Za-z]{1,2} (?:0\d|1\d|20|21|22|23))[ignorecase]')
        if ret:
            match=ret
            date=extract(str,r'(\d{2} [A-Za-z]{3,4}) \d{2}')+' '+datetime.fromtimestamp(time.time()-3600*24).strftime('%Y')
            ret=datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d')+' '+extract(str,r'\d{2} [A-Za-z]{3,4} (\d{2})')
        else:
            ret=extract(str,r'((?:ja|fe|ma|ap|ma|ju|ju|au|se|oc|no|de)[A-Za-z]{1,2} [0123]\d (?:0\d|1\d|20|21|22|23))[ignorecase]')
            if ret:
                match=ret
                date=extract(str,r'([A-Za-z]{3,4} \d{2}) \d{2}')+' '+datetime.fromtimestamp(time.time()-3600*24).strftime('%Y')
                ret=datetime.strptime(date, '%b %d %Y').strftime('%Y-%m-%d')+' '+extract(str,r'[A-Za-z]{3,4} \d{2} (\d{2})')
    else:
        match=ret

    str=str.replace(match,'').strip('_').replace('__','_')
    return str,ret


format_date_time_moagent_last_date_time=''

def format_date_time_moagent(line='',current_year='2012'):
    global format_date_time_moagent_last_date_time
    #line=r"""29 Nov 04:59:48,405 - 132251398241631	250	250	0	0	0	528	http://sbk-football.mozat.com/league/list/1.shtml?monetid=40105087&moclientwidth=360&userAgent=NokiaN8-00-024.001-sw_platform%3DS60%3Bsw_platform_version%3D5.2%3Bjava_build_version%3D2.2.54+JConf%2FCLDC-1.1+JProf%2FMIDP-2.1+Encoding%2FISO-8859-1+Locale%2Far+Lang%2Far+Caps%2F7644+Morange%2F6.2.5.111122+Domain%2F%40shabik.com+CJME%2F111122+PI%2F25f43214f04a16dea1d80d42b87e963f&moclientheight=588&devicewidth=360&deviceheight=640&isprefetch=1&cli_ip=84.235.73.234"""
    date_time_str=extract(line,r'(\d{1,2} \w+ \d{2}:\d{2}:\d{2})')
    if not date_time_str:
        print line
    try:
        formated_date_time_str=datetime.strptime(date_time_str, '%d %b %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S').replace('1900',current_year)
        format_date_time_moagent_last_date_time=formated_date_time_str
    except:
        try:
            print 'error in format_date_time_moagent:',line
        except:
            pass
        formated_date_time_str=format_date_time_moagent_last_date_time
    return line.replace(date_time_str,formated_date_time_str)


def get_log_time_stamp_ms_moagent(line='',current_year='2012'):
    #line=r"""29 Nov 04:59:48,405 - 132251398241631	250	250	0	0	0	528	http://sbk-football.mozat.com/league/list/1.shtml?monetid=40105087&moclientwidth=360&userAgent=NokiaN8-00-024.001-sw_platform%3DS60%3Bsw_platform_version%3D5.2%3Bjava_build_version%3D2.2.54+JConf%2FCLDC-1.1+JProf%2FMIDP-2.1+Encoding%2FISO-8859-1+Locale%2Far+Lang%2Far+Caps%2F7644+Morange%2F6.2.5.111122+Domain%2F%40shabik.com+CJME%2F111122+PI%2F25f43214f04a16dea1d80d42b87e963f&moclientheight=588&devicewidth=360&deviceheight=640&isprefetch=1&cli_ip=84.235.73.234"""
    date_time_str=extract(line,r'(\d{1,2} \w{3} \d{2}:\d{2}:\d{2})')
    formated_date_time_str=datetime.strptime(date_time_str, '%d %b %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S').replace('1900',current_year)
    ms=int(extract(line,r'\d{1,2} \w{3} \d{2}:\d{2}:\d{2},(\d+)'))
    return time_str_to_timestamp(formated_date_time_str)*1000+ms 


def extract_complete_time(str):
    
    #str='zzz_2010-12-10 15:00:01_ccc'
    
    ret=extract(str,r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')

    if ret:
        str=str.replace(ret,'')
        if str.startswith('_'): str=str[1:]
        if str.endswith('_'): str=str[:-1]
        str=str.replace('__','_')
        
    return str,ret

def time_floor(my_time):
    date=datetime.fromtimestamp(my_time).strftime('%Y-%m-%d')
    return time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(date,'%Y-%m-%d'))

def date_add(my_date,day_delta):
    return datetime.fromtimestamp(time.mktime(time.strptime(my_date,'%Y-%m-%d'))+3600*24*day_delta).strftime('%Y-%m-%d')

def time_add(my_date,day_delta):
    return datetime.fromtimestamp(time.mktime(time.strptime(my_date,'%Y-%m-%d %H:%M:%S'))+3600*24*day_delta).strftime('%Y-%m-%d %H:%M:%S')

def time_add_by_hour(my_date,hour_delta):
    return datetime.fromtimestamp(time.mktime(time.strptime(my_date,'%Y-%m-%d %H:%M:%S'))+3600*hour_delta).strftime('%Y-%m-%d %H:%M:%S')

def time_floor_timestamp(my_time,hour_delta=0):
    date=datetime.fromtimestamp(my_time).strftime('%Y-%m-%d')
    return time.mktime(time.strptime(date,'%Y-%m-%d'))+3600*hour_delta

def time_str_to_timestamp(time_str,format='%Y-%m-%d %H:%M:%S'):
    #date=datetime.fromtimestamp(time_str).strftime('%Y-%m-%d %H:%M:%S')
    return time.mktime(time.strptime(time_str,format))

def time_ceil(my_time):
    date=datetime.fromtimestamp(my_time+3600*24).strftime('%Y-%m-%d')
    return time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(date,'%Y-%m-%d'))

def time_ceil_timestamp(my_time,hour_delta=0):
    date=datetime.fromtimestamp(my_time+3600*24).strftime('%Y-%m-%d')
    return time.mktime(time.strptime(date,'%Y-%m-%d'))+3600*hour_delta


def log10_moperf_span_level(value):
    if value<1000:
        return 3
    if value>99999:
        return 6
    return len(str(value))

def available_moperf_span_level(value):
    if value<1000:
        return 'ignored'
    if value>99999:
        return 'ignored'
    return 'available'

#re_get_data_usage_mosession=re.compile(r'(?:[^,]*,){4}(\d+),(\d+),')

def join(sequence,separator="','",default="'0'"):
    if not sequence:
        return default
        
    return "'"+separator.join(sequence)+"'"

def join_for_sql_server_in_clause(sequence,separator="','",default="0"):
    if not sequence:
        return default
    ret="'"
    
    for i in sequence:
        ret=ret+i.replace("'","''")+"','"
    ret=ret+default+"'"
    
    return ret

def extract_client_screen_size(line):
    return extract(line,r'moclientwidth=(\d+)') + '*' \
           + extract(line,r'moclientheight=(\d+)')

def extract_client_phone_model(line):
    return extract(line,r'userAgent=([^\+]*)\+')


def extract_client_morange_version(line):
    #return extract(line,r'\+(Morange[^&\s]+)')
    _version=extract(line,r'userAgent=.*?Morange(?:%2F25|%2F)+([^&\s\+%]+)')
    _type=extract(line,r'userAgent=.*?(JME|Android|RIM|BlackBerry|iOS|S60|MIDP|WinCE|MPP)[ignorecase]')
    has_ua=line.find('userAgent=')

    if (not _version or not _type) and has_ua>-1:
        #print line
        #print 'Unknown UserAgent Info: Morange-'+_version+'-'+_type
        pass

    if _version or _type:
        return 'Morange-'+_version+'-'+_type

    if has_ua==-1:
        #print 'No_UserAgentInfo: '+line
        return 'Morange-No_UserAgentInfo-'
    
    return 'Morange-Unknown-Unknown'



def extract_client_morange_version_client_type(line):

    if line.find('userAgent=')==-1:
        return 'Morange-NoMoagent-NoMoagent'

    _version=extract(line,r'userAgent=.*?Morange(?:%2F(?:25)?)?([^&\s\+%]+)[ignorecase]').lower() or 'NoVersion'
    _type=extract(line,r'userAgent=.*?(CS60|CJME|CAndroid|CiOS|CBB|CWM)[ignorecase]').lower() or 'NoType'
    
    if _type=='cs60':
        line=line.lower()
        if line.find('s60%2f3')>-1:
            _type+='-3'
        if line.find('s60%2f5')>-1:
            _type+='-5'

    return 'Morange-'+_version+'-'+_type



def extract_client_morange_version_type(line):

    _type=extract(line,r'userAgent=.*?(CS60|CJME|CAndroid|CiOS|CBB|CWM)[ignorecase]').lower()

    if _type=='cjme':
        return 'JME'

    if _type=='candroid':
        return 'Android'

    if _type=='cbb':
        return 'BlackBerry'

    if _type=='cios':
        return 'iOS'

    if _type=='cs60':
        return 'S60'

    if _type=='cwm':
        return 'WinCE'
    
    _type=extract(line,r'userAgent=.*?(JME|Android|RIM|BlackBerry|iOS|S60|MIDP|WinCE|MPP)[ignorecase]').lower()

    if _type=='jme' or _type=='midp':
        return 'JME'

    if _type=='android':
        return 'Android'

    if _type=='rim' or _type=='blackberry':
        return 'BlackBerry'

    if _type=='ios':
        return 'iOS'

    if _type=='s60':
        return 'S60'

    if _type=='wince':
        return 'WinCE'

    return 'Unknown'


def extract_client_morange_version_type_with_s3_s5(line):

    _type=extract(line,r'userAgent=.*?(CS60|CJME|CAndroid|CiOS|CBB|CWM)[ignorecase]').lower()

    if _type=='cjme':
        return 'JME'

    if _type=='candroid':
        return 'Android'

    if _type=='cbb':
        return 'BlackBerry'

    if _type=='cios':
        return 'iOS'

    if _type=='cs60':
        line=line.lower()
        if line.find('s60%2f3')>-1:
            return 'S60-3'
        if line.find('s60%2f5')>-1:
            return 'S60-5'
        return 'S60'

    if _type=='cwm':
        return 'WinCE'
    
    _type=extract(line,r'userAgent=.*?(JME|Android|RIM|BlackBerry|iOS|S60|MIDP|WinCE|MPP)[ignorecase]').lower()

    if _type=='jme' or _type=='midp':
        return 'JME'

    if _type=='android':
        return 'Android'

    if _type=='rim' or _type=='blackberry':
        return 'BlackBerry'

    if _type=='ios':
        return 'iOS'

    if _type=='s60':
        line=line.lower()
        if line.find('s60%2f3')>-1:
            return 'S60-3'
        if line.find('s60%2f5')>-1:
            return 'S60-5'
        return 'S60'

    if _type=='wince':
        return 'WinCE'

    return 'Unknown'



def extract_client_build_info(line=''):

    #line=r'''18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143'''

    _type=extract(line,r'userAgent=.*?((?:CS60|CJME|CWM|CAndroid|CiOS|CBB|CBBP|CWP)(?:%2F|[^%0-9])[0-9\.]+)').upper()    
    
    if not _type:
        _type=extract(line,r'userAgent=.*?(Morange(?:%2F|[^%0-9])[0-9\.]+)').upper()    

    return _type.replace('%2F','/')



def recognize_client_version_type_name(line):
    if extract(line,r'userAgent=[^&=]*?(CS60)[ignorecase]'):
        return 'CS60'
    if extract(line,r'userAgent=[^&=]*?(CJME)[ignorecase]'):
        return 'CJME'
    if extract(line,r'userAgent=[^&=]*?(CAndroid)[ignorecase]'):
        return 'CAndroid'
    if extract(line,r'userAgent=[^&=]*?(CiOS)[ignorecase]'):
        return 'CiOS'
    if extract(line,r'userAgent=[^&=]*?(CBB)[ignorecase]'):
        return 'CBB'
    if extract(line,r'userAgent=[^&=]*?(CWM)[ignorecase]'):
        return 'CWM'
    return 'unknown'


def ip_to_number(line):
    "convert decimal dotted quad string to long integer"

    ip=extract(line,r'cli_ip=([0-9\.]*)')
    if not ip:
        ip=extract(line,r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')
    if not ip:
        print line
        return 0
    hexn = ''.join(["%02X" % long(i) for i in ip.split('.')])
    return long(hexn, 16)


def extract_country_name(line):
    ip=extract(line,r'cli_ip=([0-9\.]*)')
    if not ip:
        ip=extract(line,r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')
    if not ip:
        return ""
    return helper_ip.get_country_name_from_ip(ip)


def number_to_ip(n):
    "convert long int to dotted quad string"

    d = 256 * 256 * 256
    q = []
    while d > 0:
        m,n = divmod(n,d)
        q.append(str(m))
        d = d/256

    return '.'.join(q)

def get_supported_platform_from_browser_user_agent(line):
    ua=extract(line,r'HTTP[^\s]+ ([^\s]+)').lower()
    if ua.find('symbianos')>-1:
        if ua.find('6.1')>-1 or ua.find('7.0')>-1 or ua.find('8.0')>-1 or ua.find('8.1')>-1:
            return 'SymbianOS-2'
        if ua.find('9.1')>-1 or ua.find('9.2')>-1 or ua.find('9.3')>-1:
            return 'SymbianOS-3'
        if ua.find('9.4')>-1:
            return 'SymbianOS-5'
        return 'SymbianOS-J2ME'
    elif ua.find('series60')>-1:
        if ua.find('0.9')>-1 or ua.find('1.2')>-1 or ua.find('2.0')>-1 or ua.find('2.1')>-1 or ua.find('2.6')>-1 or ua.find('2.8')>-1:
            return 'SymbianOS-2'
        if ua.find('3.0')>-1 or ua.find('3.1')>-1 or ua.find('3.2')>-1:
            return 'SymbianOS-3'
        if ua.find('5.0')>-1:
            return 'SymbianOS-5'
        return 'SymbianOS-J2ME'
    elif ua.find('nokia')>-1:
        return 'Nokia-J2ME'
    elif ua.find('sonyericsson')>-1:
        return 'SonyEricsson-J2ME'
    elif ua.find('blackberry')>-1 or ua.find('rim')>-1:
        if ua.find('/4.6.')>-1 or ua.find('/4.7.')>-1 or ua.find('/4.8.')>-1 or ua.find('/4.9.')>-1 or ua.find('/5.')>-1 or ua.find('/6.')>-1:
            return 'BlackBerry-BB'
        return 'BlackBerry-J2ME'
    elif ua.find('opera+mini')>-1:
        return 'OperaMini-J2ME'
    elif ua.find('midp-2')>-1:
        return 'J2ME-J2ME'
    elif ua.find('iphone')>-1:
        return 'IPhone-IPhone'
    

    return 'Unknown'
        

def get_weekday_from_date_str(date_str):
    time_stamp=time.mktime(time.strptime(date_str,'%Y-%m-%d'))
    return date.fromtimestamp(time_stamp).isoweekday()

def get_weekday_from_time_stamp(time_stamp):
    return date.fromtimestamp(time_stamp).isoweekday()

def get_day_diff_from_date_str(date_str_1,date_str_2):
    date_1=datetime.strptime(date_str_1,'%Y-%m-%d')
    date_2=datetime.strptime(date_str_2,'%Y-%m-%d')
    return (date_1-date_2).days


def get_sec_diff_from_time_str(time_str_1,time_str_2):
    time_1=time.mktime(time.strptime(time_str_1,'%Y-%m-%d %H:%M:%S'))
    time_2=time.mktime(time.strptime(time_str_2,'%Y-%m-%d %H:%M:%S'))
    return time_1-time_2

def base36encode(number):
    if not isinstance(number, (int, long)):
        raise TypeError('number must be an integer')
    if number < 0:
        raise ValueError('number must be positive')

    alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'

    base36 = ''
    while number:
        number, i = divmod(number, 36)
        base36 = alphabet[i] + base36

    return base36 or alphabet[0]

def base36decode(number):
    return int(number,36)


def regex_replace(needle,replacement,source):
    return re.sub(needle, replacement, source)


def translate_iis_website_hourly_log_path(sg_timestamp,url_pattern,timezone_offset_to_sg,date_format='%y%m%d'):
    #url_pattern example: \\192.168.1.52\W3SVC1602359321\ex%(date)s%(hour)s
    
    result_paths=[]
    #start_sg_timestamp=time_floor_timestamp(sg_timestamp)-3600*timezone_offset_to_sg
    start_sg_timestamp=time_floor_timestamp(sg_timestamp+3600*timezone_offset_to_sg)-3600*timezone_offset_to_sg

    for i in range(0,24):
        hour_str=datetime.fromtimestamp(start_sg_timestamp).strftime('%H')
        date_str=datetime.fromtimestamp(start_sg_timestamp).strftime(date_format)
        result_paths.append(url_pattern % {'date':date_str,'hour':hour_str})
        start_sg_timestamp+=3600

    return result_paths

def translate_date(sg_timestamp,timezone_offset_to_sg):
    return time_floor(sg_timestamp+3600*timezone_offset_to_sg).replace(' 00:00:00','')

def get_phone_os_type_from_native_useragent(line):
    native_useragent=extract(line,r' HTTP\/[\d\.]+ ([^\s]+)')
    return get_phone_os_type_from_raw_native_useragent(native_useragent)

def get_phone_os_type_from_raw_native_useragent(native_useragent):
    native_useragent=native_useragent.lower()
    if native_useragent.find('nokia')>-1 or native_useragent.find('symbian')>-1:
        return "Symbian"
    if native_useragent.find("iphone")>-1 or extract(native_useragent,r'\bios\b'):
        return "iOS"
    if extract(native_useragent,r'\brim\b') or native_useragent.find('blackberry')>-1 or native_useragent.find('mds_')>-1:
        return "BlackBerry"
    if native_useragent.find('android')>-1:
        return "Android"
    if native_useragent.find('j2me')>-1 or native_useragent.find('midp')>-1:
        return "J2ME"
    return "Unidentified-Browser"


def get_client_type_from_file_name(line):
    file_name=extract(line,r'(?:GET|POST) ([^\s]+)').lower()
    if file_name.find('.cod')>-1 or file_name.find('bb')>-1:
        return 'BlackBerry'
    if file_name.find('.jad')>-1 or file_name.find('.jar')>-1:
        return 'J2ME'
    if file_name.find('.sisx')>-1 or file_name.find('.sis')>-1:
        return 'Symbian'
    if file_name.find('.apk')>-1:
        return 'Android'
    if file_name.find('.ipa')>-1:
        return 'iOS'
    return 'Non-Client-Files'


def is_download_request(line):
    return 'Y' if get_client_type_from_file_name(line)=='Non Client Files' else 'N'

def get_date_from_timestamp(my_date):
    return datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    
def get_md5_key(oem_name,category,key):
    return '0x'+helper_math.md5(oem_name+'|'+category+'|'+key)

def get_md5_sub_key(sub_key):
    return '0x'+helper_math.md5(sub_key)
    
def get_stat_range_from_target_date(begin_date_str,end_date_str):
    today=get_date_str_now()
    begin_index=-get_day_diff_from_date_str(begin_date_str,today)
    end_index=-get_day_diff_from_date_str(end_date_str,today)
    return range(begin_index,end_index-1,-1)
        
    

def get_matched_date(target_date):
    ret={'1_day':1}
    if get_weekday_from_time_stamp(time_str_to_timestamp(target_date,format='%Y-%m-%d'))==7:
        ret['7_day']=7
    if get_day_diff_from_date_str(target_date,'2012-01-01') % 14 == 0:
        ret['14_day']=14
    if date_add(target_date,1).endswith('-01'):
        ret['1_month']=int(target_date[8:])
    return ret


def date_iterator(begin_date,end_date,step=1):
    if begin_date>end_date:
        print begin_date,end_date
        raise ValueError('date_iterator:begin_date>end_date')
    if step<1:
        raise ValueError('date_iterator:step<1')
    while True:
        if begin_date>end_date:
            break
        yield begin_date
        begin_date=date_add(begin_date,step)


def get_phone_model(line):
    line=urllib.unquote_plus(line.lower()).replace('/','-').replace('-sw_platform',' ').replace('-wap2.0',' ')
    phone_model_raw=helper_regex.extract(line,r'useragent=\s*(.*?)( |%20)[ignorecase]').replace('-unknown_fversion','')
    phone_model=helper_regex.regex_replace(r'([_-][\.\d]+)',r'',phone_model_raw).replace('nokia-nokia','nokia').replace('lge-lg','lg')
    if 'samsung' in phone_model:
        phone_model=helper_regex.regex_replace(r'(\-[^\-]{7,})',r'',phone_model)
    if 'sonyericsson' in phone_model:
        phone_model=helper_regex.regex_replace(r'(\-[^\-]{6,})',r'',phone_model)
    if 'nokia' == phone_model.strip():
        #print line
        print phone_model_raw
    return phone_model




def get_time_to_day_diff_dispersion(id_to_creation_date_dict,day_options=[0,7,30,60],target_date=''):
    # 0 for v<=0, 7 for v in (0,7], ... 

    day_options=[-99999999]+day_options+[99999999]
    
    target_date=target_date or get_date_str_now()
    result={}
    for i in range(1,len(day_options)):
        result[day_options[i]]=dict((k,v) for k,v in id_to_creation_date_dict.iteritems() \
                               if get_day_diff_from_date_str(target_date,v[0:10])<=day_options[i] \
                               and get_day_diff_from_date_str(target_date,v[0:10])>day_options[i-1])

    result['all']=id_to_creation_date_dict
    #print id_to_creation_date_dict
    #print result
    #print target_date
    return result



if __name__=='__main__':

    a={'+966535466591': '2012-06-18 23:34:40', '+966503905107': '2012-03-19 17:41:27', '+966500825545': '2012-03-19 03:02:48', '+966556076949': '2012-06-18 22:00:25', '+966533952238': '2011-05-09 08:50:01', '+966531151627': '2012-06-19 11:41:18', '+966507030641': '2012-06-19 08:27:13', '+966557220725': '2012-06-07 19:59:30', '+966537992212': '2012-06-19 11:29:05', '+966552005265': '2012-02-28 10:58:27', '+966534634757': '2012-04-01 17:56:53', '+966557770507': '2011-06-30 07:47:38', '+966537923951': '2012-06-19 12:04:08', '+966502038423': '2012-06-19 03:44:22', '+966502349920': '2012-06-19 18:39:47', '+966534092581': '2012-06-18 21:58:09', '+966501447705': '2012-06-19 17:39:22', '+966530567867': '2012-06-19 07:05:02', '+966508157725': '2012-06-14 11:42:24', '+966500168603': '2012-03-27 01:49:55', '+966554593876': '2012-06-18 20:16:38', '+966556156752': '2012-06-19 18:03:26', '+966501075411': '2012-06-17 20:28:28', '+966502545248': '2012-06-19 13:09:38', '+966536446302': '2011-05-28 11:25:53', '+966533283989': '2012-03-15 18:55:45', '+966531255773': '2012-03-11 15:44:23', '+966507996967': '2011-02-12 16:29:16', '+966532113843': '2012-03-24 12:10:19', '+966508372831': '2012-04-15 18:41:36', '+966534903536': '2012-06-19 18:09:35', '+966554267925': '2011-07-09 23:54:05', '+966559620424': '2012-03-21 15:30:26', '+966500844635': '2012-06-18 23:53:38', '+966552297899': '2011-04-18 04:04:20', '+966557597669': '2010-06-07 04:40:17', '+966538279475': '2012-04-01 16:42:25', '+966535959291': '2011-11-22 18:44:20', '+966558928053': '2010-07-18 02:50:42', '+966538414303': '2011-12-21 22:00:08', '+966534713042': '2012-06-14 17:14:46', '+966531418194': '2012-03-04 04:39:58', '+966536836414': '2012-03-09 03:20:14', '+966557127762': '2010-03-29 05:48:12', '+966505309225': '2012-06-13 06:57:00', '+966537104590': '2012-01-06 12:52:13', '+966530526003': '2012-01-27 20:07:19', '+966538613525': '2012-06-19 02:08:32', '+966536863725': '2012-05-30 16:52:05', '+966535668195': '2012-06-19 20:27:09', '+966559657552': '2012-03-17 08:26:58', '+966559922776': '2010-04-30 00:00:35', '+966503562429': '2012-06-19 15:36:43', '+966557942523': '2012-06-19 03:43:17', '+966537645507': '2012-02-07 22:26:05', '+966551867415': '2010-04-08 13:11:26', '+966501145333': '2011-03-10 22:58:39', '+966557507651': '2012-03-17 00:06:11', '+966502993696': '2012-06-19 08:18:28', '+966506880885': '2012-06-18 21:17:00', '+966532888846': '2010-04-04 22:46:53', '+966553831182': '2011-11-13 20:16:55', '+966503926189': '2012-06-19 14:59:10', '+966558172703': '2012-05-05 10:47:47', '+966500456084': '2010-09-08 20:10:42', '+966503321723': '2012-04-21 20:14:36', '+966500993665': '2012-06-09 00:43:15', '+966550995763': '2011-08-17 02:08:11', '+966559869663': '2012-04-09 13:59:34', '+966552421239': '2012-06-19 18:17:22', '+966556436692': '2012-06-19 15:22:17', '+966501450774': '2012-06-18 21:10:37', '+966556253310': '2012-06-19 09:14:19', '+966537215215': '2012-06-18 23:08:20', '+966534024112': '2012-06-18 23:55:43', '+966533783518': '2012-06-19 13:00:23', '+966538623874': '2011-11-15 06:59:34', '+966503223265': '2011-07-26 01:16:54', '+966558884537': '2012-06-18 21:14:08', '+966531214062': '2011-01-21 23:13:56', '+966533469459': '2012-06-18 22:53:35', '+966506226411': '2012-06-09 16:04:23', '+966501069885': '2012-06-19 16:04:48', '+966552800348': '2012-06-19 15:33:36', '+966531506604': '2012-06-16 19:36:20', '+966501929665': '2011-08-13 17:15:57', '+966502624508': '2012-06-16 07:23:22', '+966559222662': '2012-06-19 04:38:57', '+966556141835': '2012-06-18 21:10:24', '+966530422756': '2012-06-19 19:20:26', '+966557795400': '2012-06-19 13:38:18', '+966535684652': '2012-05-15 11:05:37', '+966508953850': '2012-03-29 20:01:59', '+966557838153': '2012-06-19 16:17:56', '+966530789728': '2011-11-25 14:44:35', '+966558110986': '2010-11-20 16:17:52', '+966556859454': '2012-02-27 19:53:34', '+966531699852': '2012-06-19 11:37:56', '+966552556171': '2011-03-16 00:41:15', '+966551112001': '2010-12-24 05:42:42', '+966530661313': '2012-06-18 22:10:13', '+966538689548': '2012-06-19 02:11:22', '+966503760802': '2012-03-20 17:30:22', '+966537675465': '2012-03-22 15:01:13', '+966558787963': '2012-06-18 23:49:53', '+966551342194': '2012-03-16 16:58:38', '+966532602014': '2012-03-18 16:08:55', '+966550668648': '2012-01-03 13:52:28', '+966533286977': '2012-06-19 05:32:36', '+966532419756': '2012-06-19 07:54:37', '+966554024867': '2012-06-19 07:04:42', '+966531497935': '2011-02-17 08:23:27', '+966537422285': '2012-01-21 15:27:26', '+966502758804': '2012-06-19 00:26:35', '+966550811035': '2012-06-19 12:01:07', '+966532006138': '2012-06-04 08:51:27', '+966533908713': '2012-06-19 08:27:49', '+966504423153': '2012-06-19 06:51:48', '+966502808174': '2012-03-31 14:19:48', '+966532685054': '2012-06-19 10:46:45', '+966503224802': '2011-06-13 02:42:32', '+966508598097': '2011-10-14 08:29:08', '+966552628618': '2011-11-03 22:24:47', '+966530533686': '2010-12-17 19:29:00', '+966551058080': '2011-06-08 06:28:02', '+966551253598': '2012-06-19 04:39:59', '+966530626946': '2012-06-19 05:06:11', '+966553725859': '2012-04-05 17:02:35', '+966559481713': '2012-06-18 21:20:31', '+966557731577': '2011-04-30 19:14:30', '+966537894974': '2012-02-11 18:58:07', '+966500545064': '2012-06-07 19:29:26', '+966557367637': '2010-05-05 02:47:18', '+966536392664': '2012-06-19 06:55:47', '+966501532367': '2012-03-31 13:50:35', '+966500584850': '2012-06-19 09:17:21', '+966550667967': '2012-04-27 19:39:55', '+966532050692': '2012-06-19 09:33:34', '+966534621117': '2012-06-19 09:40:14', '+966553664136': '2011-04-03 18:28:07', '+966553845465': '2012-05-29 17:37:26', '+966537722420': '2012-06-19 11:38:57', '+966551253324': '2012-06-19 09:42:52', '+966533101675': '2012-03-09 16:55:10', '+966555989370': '2012-06-19 05:24:33', '+966535858234': '2011-07-23 22:11:21', '+966530939074': '2012-06-19 19:09:58', '+966556559803': '2012-03-28 12:51:09', '+966550993463': '2012-03-17 11:09:28', '+966509772209': '2012-03-01 18:40:13', '+966538238384': '2012-01-10 23:25:11', '+966532555248': '2012-06-19 12:41:35', '+966502547391': '2012-02-29 06:12:40', '+966537939611': '2012-06-19 13:14:54', '+966558850523': '2011-03-25 13:51:02', '+966501676619': '2012-06-19 07:15:22', '+966551116169': '2012-06-18 21:38:20', '+966506735403': '2012-06-19 04:23:14', '+966535711631': '2012-03-23 04:33:17', '+966553763985': '2011-06-19 16:47:40', '+966556063613': '2012-06-18 22:59:03', '+966507816127': '2012-01-23 14:14:02', '+966507234055': '2012-03-25 20:22:27', '+966500087102': '2011-10-07 12:41:58', '+966557952772': '2012-03-18 13:07:16', '+966557438882': '2010-03-28 20:51:10', '+966535508856': '2012-06-19 11:12:16', '+966553904813': '2012-06-19 00:15:53', '+966553913908': '2012-04-11 10:43:28', '+966558134677': '2012-06-19 00:40:12', '+966552080685': '2011-02-17 09:10:32', '+966531144344': '2012-06-18 23:48:32', '+966506192834': '2012-01-20 20:12:36', '+966504481591': '2011-12-05 01:57:59', '+966535897003': '2012-03-15 16:08:26', '+966535097213': '2012-06-16 00:37:58', '+966551351068': '2012-03-24 21:47:54', '+966557112975': '2012-04-06 19:16:43', '+966535294065': '2011-06-15 17:33:53', '+966508883385': '2012-06-19 16:44:44', '+966500506261': '2011-07-07 22:07:14', '+966504377165': '2011-05-04 12:17:30', '+966550289702': '2011-09-25 21:38:02', '+966530534255': '2011-07-21 16:38:46', '+966534377615': '2012-03-15 15:22:36', '+966538645683': '2012-04-01 02:15:13', '+966559184635': '2012-01-02 15:13:51', '+966534167510': '2012-04-01 14:31:36', '+966534030754': '2012-06-19 18:35:36', '+966556099592': '2012-06-19 07:14:42', '+966505643532': '2012-06-19 19:07:08', '+966566003585': '2011-01-26 00:26:04', '+966530474413': '2012-02-02 20:22:19', '+966538637592': '2012-03-14 15:59:45', '+966558586942': '2011-12-07 03:37:44', '+966530234649': '2012-06-18 21:25:53', '+966552395354': '2012-06-11 09:31:03', '+966533759167': '2012-06-19 20:17:52', '+966556354897': '2011-11-05 22:28:25', '+966537587825': '2012-05-26 22:22:18', '+966537974668': '2012-06-04 15:26:35', '+966505461235': '2012-06-19 01:33:34', '+966550088604': '2012-06-19 19:41:51', '+966552454209': '2012-06-18 03:32:25', '+966538180611': '2012-06-19 15:55:42', '+966536267708': '2012-01-29 21:51:39', '+966559375653': '2012-06-19 08:03:40', '+966532617914': '2011-02-07 23:12:51', '+966533958611': '2012-06-19 18:36:09', '+966501040645': '2012-04-05 19:48:28', '+966557417353': '2012-06-19 12:59:06', '+966507932994': '2012-05-24 08:13:02', '+966559079836': '2012-06-18 22:16:38', '+966500252910': '2010-12-16 11:13:11', '+966557045621': '2012-06-19 19:38:39', '+966559510912': '2011-09-02 10:04:35', '+966831041489865': '2012-04-28 16:38:12', '+966551246562': '2012-06-19 16:55:06', '+966537255967': '2012-06-19 09:53:00', '+966550619954': '2011-05-30 07:30:03', '+966507060296': '2012-03-31 15:41:54', '+966559752723': '2012-06-06 18:11:12', '+966503286686': '2012-03-23 17:21:23', '+966537240949': '2012-06-19 07:42:31', '+966535518473': '2012-03-21 15:38:30', '+966500261405': '2012-06-19 12:31:17', '+966531485338': '2012-06-19 19:38:18', '+966500587990': '2011-08-27 19:58:27', '+966559029252': '2012-03-28 10:45:00', '+966558195534': '2012-02-02 19:34:15', '+966533403575': '2012-02-06 21:06:07', '+966553874984': '2012-06-19 13:58:18', '+966531206107': '2012-06-19 04:17:42', '+966509216404': '2012-06-19 17:50:02', '+966534643083': '2012-06-19 07:36:26', '+966502964425': '2012-03-21 09:52:52', '+966556577570': '2011-06-09 09:01:45', '+966550471028': '2011-08-05 20:32:23', '+966531378878': '2010-11-09 17:47:38', '+966505346509': '2012-06-18 22:37:19', '+966537228796': '2012-04-03 01:22:59', '+966532365927': '2012-06-19 19:29:08', '+966550426621': '2011-03-24 10:21:56', '+966551966424': '2012-06-16 20:42:24', '+966553903600': '2012-06-19 19:09:45', '+966533461053': '2012-03-09 00:28:22', '+966538021391': '2012-04-12 11:43:55', '+966557012245': '2012-06-12 12:38:21', '+966552606017': '2012-06-19 09:34:30', '+966532050116': '2012-06-19 18:13:40', '+966509808270': '2012-06-19 08:19:26', '+966530407498': '2012-04-14 20:32:44', '+966536154462': '2012-06-19 05:48:24', '+966509369360': '2012-06-19 17:45:01', '+966502079530': '2012-06-19 17:47:23', '+966550289741': '2012-06-18 23:35:17', '+966550595890': '2011-12-04 07:52:25', '+966556393338': '2011-09-29 20:37:19', '+966536054675': '2012-03-01 13:15:39', '+966538890923': '2012-03-07 21:39:15', '+966555722616': '2012-06-19 11:55:47', '+966558872135': '2012-04-06 18:38:24', '+966535501640': '2012-06-03 19:59:54', '+966556322979': '2012-06-19 19:16:11', '+966500298818': '2012-06-18 22:00:53', '+966506667702': '2012-06-19 18:38:45', '+966531205900': '2012-06-19 12:02:39', '+966508003236': '2012-06-19 10:16:12', '+966530326585': '2012-03-03 14:41:41', '+966502443923': '2012-06-19 14:47:20', '+966532503132': '2012-03-15 20:37:56', '+966551294583': '2012-06-12 13:37:15', '+966553177071': '2012-04-03 17:38:35', '+966507983893': '2012-04-08 05:19:16', '+966537258142': '2011-10-24 17:28:17', '+966559314808': '2012-05-25 20:36:53', '+966532461284': '2012-03-15 16:03:43', '+966537911564': '2012-06-08 22:40:22', '+966506659019': '2012-06-19 08:44:13', '+966536442460': '2012-03-03 09:57:15', '+966503955446': '2012-06-19 20:44:39', '+966550463690': '2012-06-19 00:23:32', '+966538218512': '2012-06-19 17:54:50', '+966536018613': '2012-04-17 11:35:51', '+966504316587': '2012-06-18 21:11:54', '+966559329130': '2012-05-12 08:13:33', '+966507164742': '2012-01-27 14:40:11', '+966551209362': '2012-04-15 05:01:49', '+966559194246': '2011-10-03 11:14:57', '+966556847895': '2012-03-27 23:30:55', '+966531849456': '2012-06-19 17:41:57', '+966556983412': '2012-03-17 03:04:59', '+966534306268': '2012-04-13 15:34:46', '+966502825083': '2012-05-10 10:22:03', '+966557794427': '2012-06-19 18:33:27', '+966554256570': '2012-06-18 22:31:05', '+966551265792': '2012-02-12 16:40:35', '+966550481210': '2012-06-18 09:50:26', '+966502495857': '2012-06-19 01:16:00', '+966556620418': '2011-09-23 18:40:59', '+966538033325': '2012-03-15 17:19:21', '+966503322756': '2012-03-27 16:41:19', '+966506915535': '2012-04-08 06:22:58', '+966559340403': '2010-01-28 12:09:42', '+966535387841': '2012-06-18 21:55:58', '+966534628896': '2012-06-19 18:05:22', '+966557890092': '2012-03-29 16:48:50', '+966558373832': '2012-03-23 15:37:16', '+966554917960': '2012-06-18 21:03:13', '+966556641509': '2012-06-19 03:26:28', '+966534582290': '2012-06-18 21:15:45', '+966507623410': '2012-06-19 00:18:50', '+966550356226': '2012-05-23 07:55:42', '+966531394654': '2012-01-26 05:45:49', '+966509564790': '2010-01-27 15:01:20', '+966536731185': '2012-03-14 18:58:44', '+966506705952': '2011-12-19 07:36:48', '+966506794347': '2012-06-19 13:12:59', '+966556701163': '2012-04-13 11:06:18', '+966554374872': '2012-03-31 22:57:06', '+966501493206': '2012-06-19 19:20:45', '+966505835513': '2012-06-19 05:11:44', '+966550619790': '2010-04-12 19:28:58', '+966556562499': '2012-03-22 14:33:04', '+966507226303': '2012-06-19 10:09:48', '+966507694147': '2012-06-12 20:17:58', '+966502225708': '2012-06-19 00:24:48', '+966552500658': '2011-07-31 05:40:21', '+966508189610': '2012-06-19 02:41:51', '+966501273680': '2011-02-17 23:02:03', '+966502436335': '2012-06-17 07:02:05', '+966550019908': '2011-07-23 06:28:04', '+966504403211': '2012-06-19 01:28:27', '+966534459962': '2012-01-31 16:02:46', '+966507591138': '2010-09-26 00:38:11', '+966506455998': '2012-04-07 14:38:27', '+966507652592': '2012-06-19 05:48:50', '+966538129836': '2012-03-03 05:57:47', '+966506791833': '2010-09-05 12:12:20', '+966550692336': '2012-06-19 01:41:39', '+966533035411': '2011-02-23 23:23:05', '+966552360621': '2012-06-19 16:52:27', '+966536777185': '2011-08-24 21:11:45', '+966559556445': '2010-09-19 15:07:07', '+966550754079': '2012-01-20 21:10:48', '+966536594817': '2012-05-28 20:02:51', '+966556279404': '2010-04-04 22:17:13', '+966558985393': '2012-06-18 21:51:21', '+966550665648': '2012-06-18 23:09:08', '+966533209332': '2012-06-19 11:54:13', '+966559753498': '2012-06-19 18:14:08', '+966509215224': '2012-06-18 23:06:34', '+966504854174': '2012-06-19 06:32:15', '+966553569730': '2012-04-05 17:34:05', '+966552935571': '2011-10-27 17:10:32', '+966503427192': '2012-04-28 10:48:07', '+966536147760': '2011-06-04 03:36:54', '+966532291060': '2012-05-02 16:58:29', '+966530992112': '2012-06-19 16:12:06', '+966554606823': '2012-06-19 07:22:12', '+966509344866': '2012-06-19 00:25:41', '+966500471658': '2012-06-19 04:18:43', '+966559392174': '2011-07-30 22:47:55', '+966506438079': '2011-04-29 15:59:03', '+966509572145': '2012-03-09 17:40:24', '+966505246485': '2012-06-19 06:27:19', '+966506787921': '2010-04-21 11:24:29', '+966500908354': '2012-04-21 14:51:51', '+966506060801': '2012-06-19 07:03:24', '+966535079298': '2011-06-02 19:26:06', '+966538689433': '2012-03-24 14:06:42', '+966534939091': '2012-04-18 20:09:46', '+966559046963': '2012-06-19 20:26:02', '+966503354912': '2012-04-01 10:22:31', '+966534376236': '2011-11-15 05:22:29', '+966532188635': '2012-06-19 14:01:31', '+966534500221': '2012-06-18 23:12:24', '+966537423609': '2012-03-19 18:29:52', '+966554687521': '2012-02-27 05:25:53', '+966551637841': '2012-06-07 16:26:33', '+966501716670': '2012-02-29 16:23:27', '+966501704118': '2012-06-19 18:49:59', '+966509948612': '2011-03-02 21:36:55', '+966509660112': '2011-10-13 18:47:05', '+966559925505': '2012-06-19 08:46:12', '+966557251638': '2012-06-18 17:18:05', '+966534442768': '2011-11-11 17:58:52', '+966557707754': '2011-07-26 20:06:08', '+966535587962': '2011-11-12 18:13:44', '+966502641855': '2012-03-29 14:52:43', '+966503486498': '2012-06-02 19:30:54', '+966501129458': '2012-01-22 13:40:35', '+966504347055': '2012-03-22 17:32:27', '+966550280895': '2012-03-31 04:47:57', '+966506838422': '2012-06-18 21:10:51', '+966501089040': '2012-06-19 16:40:24', '+966532417761': '2012-06-18 21:49:10', '+966508209636': '2012-05-01 07:51:06', '+966509717080': '2010-03-31 15:29:07', '+966553794563': '2012-06-18 21:11:55', '+966533255807': '2012-03-07 11:20:46', '+966534489704': '2012-06-19 16:25:51', '+966557180329': '2012-06-19 15:57:14', '+966532759994': '2012-03-18 21:37:28'}

    print get_time_to_day_diff_dispersion(a)
    exit()

    for d in date_iterator('2012-03-05','2012-03-20',1):
        print d
    exit()
        
    
    export_app_name()
    exit()

    print get_stat_range_from_target_date(begin_date_str='2011-12-15',end_date_str='2011-12-20') 
    exit()

    print recognize_app_from_moagent_log_line(r'http://i-shabik-smsapi-stc-invitation.morange.com/SendSMS.ashx?UserId=35516559&ToNumbers=0558706139&Content=%d8%b5%d8%af%d9%8a%d9%82%d9%83%20%d9%8a%d8%af%d8%b9%d9%88%d9%83%20%d9%84%d9%84%d8%a5%d8%b4%d8%aa%d8%b1%d8%a7%d9%83%20%d9%85%d8%b9%d9%87%20%d9%81%d9%8a%20%d8%b4%d8%a7%d8%a8%d9%83.%20%d8%a5%d8%b4%d8%aa%d8%b1%d9%83%20%d8%a8%d8%b')
    exit()


    print get_simplified_url_unique_key('http://i-shabik-smsapi-stc-invitation.morange.com/SendSMS.ashx?UserId=35516559&ToNumbers=0558706139&Content=%d8%b5%d8%af%d9%8a%d9%82%d9%83%20%d9%8a%d8%af%d8%b9%d9%88%d9%83%20%d9%84%d9%84%d8%a5%d8%b4%d8%aa%d8%b1%d8%a7%d9%83%20%d9%85%d8%b9%d9%87%20%d9%81%d9%8a%20%d8%b4%d8%a7%d8%a8%d9%83.%20%d8%a5%d8%b4%d8%aa%d8%b1%d9%83%20%d8%a8%d8%b')
    exit()

    
    print get_matched_date('2012-01-31')
    exit()

    #print get_simplified_url_unique_key('08 Apr 17:27:09,956	7678648	406	390	16	-1	http://mobileshabik.morange.com/mobile_homepage.aspx?isprefetch=1&isprefetch=0&evflg=test&monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147&action=fds&fhdjskafh')
    #exit()
    t=time.time()
    for i in range(1,100000):
        get_simplified_url_unique_key('08 Apr 17:27:09,956	7678648	406	390	16	-1	http://mobileshabik.morange.com/mobile_homepage.aspx?isprefetch=1&isprefetch=0&isprefetch=1&evflg=test&monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147&action=fds&fhdjskafh')
    print time.time()-t

    """
    print get_phone_os_type_from_native_useragent('')    
    print datetime.fromtimestamp(time_ceil_timestamp(time.time(),6)).strftime('%Y-%m-%d %H:%M:%S')
    exit()

    print get_time_clock_time()

    print translate_iis_website_hourly_log_path(time.time(),'\\192.168.1.52\W3SVC1602359321\ex%(date)s%(hour)s',-6,date_format='%y%m%d')
    print translate_date(time.time(),-6)
    exit()
    print number_to_ip(785735276)
    """
    #print base36decode('yxg34z')
    
    #print extract(' INFO 2010-08-22 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 21301488; iRoomId:53',r'INFO ([\s0-9\-]{10})')
    #print extract('fhd af',r'(?:hd) (a)')
    #print merge_keys_sorted({'fdsf':1,'a':3,'z':3})
    #print recognize_app_from_url('08 Apr 17:27:09,956	7678648	406	390	16	-1	http://mobileshabik.morange.com/mobile_hohmepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147')
    """
    t=time.time()
    for i in range(1,100000):
        get_url_unique_key('08 Apr 17:27:09,956	7678648	406	390	16	-1	http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147')
    print time.time()-t
    """
    #print extract_date_hour('data_2010-04-03 12_data')
    #print time_floor(time.time())
    #print time_ceil(time.time())

    #line='08 Apr 17:27:09,956	7678648	406	390	16	-1	http://mobileshabik.morange.com/mobile_hohmepage.aspx?&monetid=13032541&action=hfdalfh&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147'
    #line='2010-05-18 16:00:00,235 [ INFO] MoPerfmonService.ProcessBrowserRequest -  BReq	13790682	127418013785986	9	http://mobileshabik.morange.com/mobile_poll.aspx?action=PollItem&PollID=2467327&CreatorID=11897584&StatisticTag=Homepage_LatestEvent	1274180138093	84.235.75.18'
    #line='2010-05-18 16:00:00,250 [ INFO] MoPerfmonService.ProcessBrowserResponse -  BRep	12540177	12741695522471631	1274169553775	188.139.151.26'
    """
    c=time.time()    
    for i in range(1,100000):
        get_url_unique_key(str)
    print time.time()-c

    c=time.time()    
    for i in range(1,100000):
        get_url_unique_key_2(str)
    print time.time()-c
    """

    #print get_url_unique_key(line)
    #print extract_client_screen_size(str)
    #print extract_client_phone_model(str)
    #line=r'03 Jun 12:00:03,432 - 15730766	31	31	0	0	0	1035	http://voda-oa.i.mozat.com:8081/OceanAge/header.jsp?friendId=55461039&monetid=55414727&moclientwidth=240&userAgent=Samsung%2FGT-S5570+Encoding%2FUTF-8+Lang%2Far+Locale%2Far_AE+Morange%2F6.0.1.R1+Caps%2F661+Android%2F8+CAndroid%2F6.0.0.10355+PI%2F12F6119F76B59C997208E9EC1A3065A7+Domain%2F%40voda_egypt&moclientheight=282&devicewidth=240&deviceheight=320&cli_ip=41.206.155.119'
    #print extract_client_morange_version(line)
    #print extract_client_morange_version_type(line)
    #print get_online_time_mosession(str)
    #print set(apps.values())
    #print get_data_usage_mosession_refined('')
    #print get_online_time_mosession_refined('')

    #print recognize_app_from_url('lbtelk.morange.com/Home/LeaderBoard/2/?monetid=115496&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.144.207')
    #print extract_complete_time_key("fsdf_2010-12-10 1,938_fdsf")

    #print recognize_client_version('Morange0')
    """
    test_file_names=[r'change_password.aspx',r'events.aspx',r'events.aspx.cs',r'facebook_chat.ashx',r'facebook_info.aspx',r'facebook_info.aspx.cs',r'friends.aspx',r'friends.aspx.cs',r'helios_homepage.aspx',r'helios_homepage.aspx.cs',r'helios_homepage1.aspx',r'homepage_m_email_it.aspx',r'homepage_one_email_it.aspx',r'IDbanner.htm',r'me.ashx',r'mobile_4tab_msgBox.aspx',r'mobile_4tab_msgBox.aspx.cs',r'mobile_4tab_myphoto.aspx',r'mobile_4tab_myphoto.aspx.cs',r'mobile_4tab_mypoll.aspx',r'mobile_4tab_mypoll.aspx.cs',r'mobile_4tab_mysaying.aspx',r'mobile_4tab_mysaying.aspx.cs',r'mobile_accessory.aspx',r'mobile_accessory.aspx.cs',r'mobile_allApps.aspx',r'mobile_allApps.aspx.cs',r'mobile_app.aspx',r'mobile_app.aspx.cs',r'mobile_applist.aspx',r'mobile_applist.aspx.cs',r'mobile_app_setting.aspx',r'mobile_app_setting.aspx.cs',r'mobile_billing.aspx',r'mobile_billing.aspx.cs',r'mobile_bindphone_sent.aspx',r'mobile_bindphone_sent.aspx.cs',r'mobile_browser_hotlinks.aspx',r'mobile_browser_hotlinks.aspx.cs',r'mobile_browser_hotlinks_bak.aspx',r'mobile_changelang.aspx',r'mobile_changelang.aspx.cs',r'mobile_changeprivacy.aspx',r'mobile_changeprivacy.aspx.cs',r'mobile_chatroom.aspx',r'mobile_chatroom.aspx.cs',r'mobile_circle.aspx',r'mobile_circle.aspx.cs',r'mobile_circleforum.aspx',r'mobile_circleforum.aspx.cs',r'mobile_circlemember.aspx',r'mobile_circlemember.aspx.cs',r'mobile_circlephoto.aspx',r'mobile_circlephoto.aspx.cs',r'mobile_conversation.aspx',r'mobile_conversation.aspx.cs',r'mobile_dock_setting.aspx',r'mobile_dock_setting.aspx.cs',r'mobile_editprofile.aspx',r'mobile_editprofile.aspx.cs',r'mobile_email.aspx',r'mobile_email.aspx.cs',r'mobile_explorepeople.aspx',r'mobile_explorepeople.aspx.cs',r'mobile_forum.aspx',r'mobile_forum.aspx.cs',r'mobile_help.aspx',r'mobile_help.aspx.cs',r'mobile_homepage.aspx',r'mobile_homepage.aspx.cs',r'mobile_homepage.html',r'mobile_homepage_setting.aspx',r'mobile_homepage_setting.aspx.cs',r'mobile_im.aspx',r'mobile_im.aspx.cs',r'mobile_message.aspx',r'mobile_message.aspx.cs',r'mobile_moblog.aspx',r'mobile_moblog.aspx.cs',r'mobile_mochat.aspx',r'mobile_mochat.aspx.cs',r'mobile_mophoto.aspx',r'mobile_mophoto.aspx.cs',r'mobile_msg.aspx',r'mobile_msg.aspx.cs',r'mobile_myEvents.aspx',r'mobile_myEvents.aspx.cs',r'mobile_newsfeeds.aspx',r'mobile_newsfeeds.aspx.cs',r'mobile_notification.aspx',r'mobile_notification.aspx.cs',r'mobile_personalEventPrivacy.aspx',r'mobile_personalEventPrivacy.aspx.cs',r'mobile_phonebook.aspx',r'mobile_phonebook.aspx.cs',r'mobile_poke.aspx',r'mobile_poke.aspx.cs',r'mobile_poll.aspx',r'mobile_poll.aspx.cs',r'mobile_privacysetting.aspx',r'mobile_privacysetting.aspx.cs',r'mobile_rank.aspx',r'mobile_rank.aspx.cs',r'mobile_recommand.aspx',r'mobile_recommand.aspx.cs',r'mobile_requests.aspx',r'mobile_requests.aspx.cs',r'mobile_select_timezone.aspx',r'mobile_select_timezone.aspx.cs',r'mobile_setting.aspx',r'mobile_setting.aspx.cs',r'mobile_sone.aspx',r'mobile_sone.aspx.cs',r'mobile_sonesetting.aspx',r'mobile_sonesetting.aspx.cs',r'mobile_store.aspx',r'mobile_store.aspx.cs',r'mophoto_album.aspx',r'mophoto_album.aspx.cs',r'mophoto_albumlist.aspx',r'mophoto_albumlist.aspx.cs',r'mophoto_comments.aspx',r'mophoto_comments.aspx.cs',r'mophoto_create_album.aspx',r'mophoto_create_album.aspx.cs',r'mophoto_delete_album.aspx',r'mophoto_delete_album.aspx.cs',r'mophoto_delete_photo.aspx',r'mophoto_delete_photo.aspx.cs',r'mophoto_edit_album.aspx',r'mophoto_edit_album.aspx.cs',r'mophoto_edit_photo.aspx',r'mophoto_edit_photo.aspx.cs',r'mophoto_module.aspx',r'mophoto_module.aspx.cs',r'mophoto_more.aspx',r'mophoto_more.aspx.cs',r'mophoto_multiphotos.aspx',r'mophoto_multiphotos.aspx.cs',r'mophoto_photo.aspx',r'mophoto_photo.aspx.cs',r'mophoto_public_photo.aspx',r'mophoto_public_photo.aspx.cs',r'mophoto_submit.aspx',r'mophoto_submit.aspx.cs',r'mophoto_upload_photo.aspx',r'mophoto_upload_photo.aspx.cs',r'mosone_multiuser.aspx',r'mosone_multiuser.aspx.cs',r'moweb_profile.aspx',r'moweb_profile.aspx.cs',r'new_user_welcome.aspx',r'new_user_welcome.aspx.cs',r'shabik_redirect.htm',r'StyleSheet.css',r't.ashx',r'tab_apps.aspx',r'tab_apps.aspx.cs',r'tab_events.aspx',r'tab_events.aspx.cs',r'tab_home.aspx',r'tab_home.aspx.cs',r'tab_me.aspx',r'tab_me.aspx.cs',r'tab_notification.aspx',r'tab_notification.aspx.cs',r'Testing.aspx',r'Testing.aspx.cs',r'view_ads.aspx',r'view_ads.aspx.cs']
    for i in test_file_names:
        if recognize_app_from_url(i)=='unrecognized':
            print i+','+recognize_app_from_url(i)
        
    print get_time_str_now()
    print get_script_file_name()

    print time_add('2010-08-08 01:01:01',-3)
    print extract_client_morange_version(r'24 Jan 00:00:01,382 - 129579820263785	563	563	0	0	0	524	http://mobile.morange.com/mobile_forum.aspx?action=Get_PostDetail&BoardId=21810&ThreadId=409063&PostID=5163087&userid=12653423&monetid=13952346&moclientwidth=240&userAgent=NokiaN73-1-4.0736.3.2.1+JConf%2FCLDC-1.1+JProf%2FMIDP-2.0+Encoding%2FISO-8859-1+Locale%2Far+Lang%2Fen+Caps%2F92+Morange%2F6.0.0.101115+CJME%2F101115&moclientheight=280&devicewidth=240&deviceheight=320&cli_ip=91.213.191.158')
    print get_day_diff_from_date_str('2011-03-21','2011-02-21')
    print date_add('2010-08-08',-3)
    apps_list=list(set(apps.values()))
    apps_list.sort()
    counter=70
    for i in apps_list:
        #print "    {'src':'fig%s','oem_name':'mozat'.lower(),'des':'login-app_%s_last_time'}," % (counter,i)
        print "'%s':'`sub_key`=\"%s\"'," % (i,i)
        counter+=1
    print extract_useragent_key(r'03 Jun 12:00:03,432 - 15730766	31	31	0	0	0	1035	http://voda-oa.i.mozat.com:8081/OceanAge/header.jsp?friendId=55461039&monetid=55414727&moclientwidth=240&userAgent=Samsung%2FGT-S5570+Encoding%2FUTF-8+Lang%2Far+Locale%2Far_AE+Morange%2F6.0.1.R1+Caps%2F661+Android%2F8+CAndroid%2F6.0.0.10355+PI%2F12F6119F76B59C997208E9EC1A3065A7+Domain%2F%40voda_egypt&moclientheight=282&devicewidth=240&deviceheight=320&cli_ip=41.206.155.119')

    print extract_date('')

    print extract_useragent_key(r'11 Jul 06:00:00,086 - 131033520545411   16      16      -1      -1      0677     ttp://matrix-interface-mozat.i.mozat.com/get_invitee_list.ashx?lang=ar&caps=348&domain=%40voda_egypt&isretry=0&userAgent=Nokia2710c-2-06.13%20JConf%2fCLDC-1.1%20JProf%2fMIDP-2.1%20Encoding%2fISO-8859-1%20Locale%2far%20Lang%2far%20Caps%2f348%20Morange%2f6.0.2.110630%20Domain%2f%40voda_egypt%20CJME%2f110630&hash=97&monetid=55556744&cli_ip=62.68.241.230')
    """

"""

apps = {

    ".gif":"photo_server",
    ".jpeg":"photo_server",
    ".jpg":"photo_server",
    ".png":"photo_server",
    "/oceanage/":"ocean_age",
    "/texasholdem":"texas_holdem",
    "_msg.aspx":"message",
    "_myEvents.aspx":"event",
    "_requests.aspx":"notification",
    "accessory.aspx":"app_center",
    "aftersmsinvite":"invite",
    "album.aspx":"photo",
    "billing.aspx":"billing",
    "bindphone_sent.aspx":"friend",
    "browser_hotlinks.aspx":"browser",
    "change_password":"setting",
    "conversation.aspx":"message",
    "events":"event",
    "explorepeople.aspx":"friend",
    "facebook":"facebook",
    "flickr":"flickr",
    "footballwar.i.":"football_war",
    "friend.morange.com":"friend",
    "friends.aspx":"friend",
    "gettingstartfindingfrienddetails":"friend",
    "gettingstartsmsinviteredirect":"invite",
    "gomoku":"gomoku",
    "happybarn.":"happy_barn",
    "invite_outer_user":"invite",
    "jit.mozat.net/mobilecards":"greeting_cards",
    "lbtelk":"leader_board",
    "linkedin":"linkedin",
    "mobile_4tab_msgBox":"message",
    "mobile_allapps":"app_center",
    "mobile_app":"app_center",
    "mobile_app_setting":"setting",
    "mobile_changelang":"setting",
    "mobile_changeprivacy":"setting",
    "mobile_chatroom":"chatroom",
    "mobile_chatroom_settings":"chatroom",
    "mobile_circle":"circle",
    "mobile_circlemember":"circle",
    "mobile_circlephoto":"circle",
    "mobile_dock_setting":"setting",
    "mobile_editprofile":"profile",
    "mobile_email":"email",
    "mobile_forum":"circle",
    "mobile_help":"help",
    "mobile_homepage":"homepage_old_version",
    "mobile_homepage_setting":"setting",
    "mobile_im":"im",
    "mobile_invitation":"invite",
    "mobile_invitation_phonecontact":"invite",
    "mobile_message":"message",
    "mobile_moblog":"saying",
    "mobile_mochat":"mochat",
    "mobile_mophoto":"photo",
    "mobile_newsfeeds":"event",
    "mobile_notification":"notification",
    "mobile_personaleventprivacy":"setting",
    "mobile_phonebook":"phone_backup",
    "mobile_poke":"poke",
    "mobile_poll":"poll",
    "mobile_privacysetting":"setting",
    "mobile_rank":"profile",
    "mobile_recent_visitors":"recent_visitor",
    "mobile_recommand":"friend",
    "mobile_select_timezone":"setting",
    "mobile_setting":"setting",
    "mobile_sone":"friend",
    "mobile_sonesetting":"setting",
    "mobile_star":"star_user",
    "mobile_store":"app_center",
    "momail":"email",
    "mophoto_album":"photo",
    "mophoto_albumlist":"photo",
    "mophoto_comments":"photo",
    "mophoto_create_album":"photo",
    "mophoto_edit_album":"photo",
    "mophoto_edit_photo":"photo",
    "mophoto_module":"photo",
    "mophoto_more":"public_photo",
    "mophoto_multiphotos":"photo",
    "mophoto_photo":"photo",
    "mophoto_popular_photos":"hot_photo",
    "mophoto_public_photo":"public_photo",
    "mophoto_submit":"photo",
    "mophoto_upload_photo":"photo",
    "motwitter.":"twitter",
    "msgBox.aspx":"message",
    "multiuser.aspx":"friend",
    "myphoto.aspx":"photo",
    "mypoll.aspx":"poll",
    "mysaying.aspx":"saying",
    "netlog":"netlog",
    "new_user_welcome":"help",
    "oa.i.":"ocean_age",
    "oaw.i.":"ocean_age_world",
    "photo.aspx":"photo",
    "profile.aspx":"profile",
    "rss":"rss",
    "smsinvite":"invite",
    "status.i":"status",
    "tab_apps":"app_center",
    "tab_events":"event",
    "tab_home":"homepage",
    "tab_me":"profile",
    "tab_notification":"notification",
    "telk-twitter.":"twitter",
    "youtube":"youtube",
}
"""
