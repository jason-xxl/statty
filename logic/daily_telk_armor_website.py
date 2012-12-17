import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date

iphone=0
opera_mini=0

def IsSupportedBlackBerry(UserAgent):
    if len(UserAgent) < 1:
        return False

    try:
        i = UserAgent.index("BlackBerry")
    except:
        return False

    i = UserAgent.index("/", i) + 1
    j = UserAgent.index("+", i)
    if j == -1:
        j = len(UserAgent)

    value = UserAgent[i:][0:j - i]

    if CompareVersion("4.6.0", value) >= 0:
        return True
    
    return False

def CompareVersion(BaseVersion, Version):
    if BaseVersion == Version:
        return 0

    basePart = BaseVersion.split(".")
    verrsionPart = Version.split(".")

    for i in range(0, len(basePart)):
        if len(verrsionPart) > i:
            base = int(basePart[i])
            ver = int(verrsionPart[i])
            if base > ver:
                return -1
            elif base < ver:
                return 1
        else:
            return -1

    return 1

def is_not_supported(ua):
    global iphone, opera_mini
    if ua.count("Nokia") > 0:
        return False
    if ua.count("Symbian") >0:
        return False
    if ua.count("iPhone") >0:
        iphone += 1
        return True
    if ua.count("Opera+Mini") >0:
        opera_mini += 1
        return False
    if ua.count("SonyEricsson") > 0:
        return False
    try:
        if IsSupportedBlackBerry(ua):
            return False
    except:
        pass
    return True

def get_phone_brand(line):
    if line.count("Nokia") > 0 or line.count("Symbian") >0:
        return "symbian"
    if line.count("iPhone") >0:
        return "iphone"
    if line.count("Opera+Mini") >0 or line.count("Opera Mini") >0:
        return "opera_mini"
    if line.count("SonyEricsson") > 0:
        return "sony_ericsson"
    if line.count("RIM") > 0 or line.count("BlackBerry") > 0:
        return "blackberry"
    return "unknown"




def stat_website(my_date):

    global current_date
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    #2010-06-12 16:00:00 GET /rpc/avatar.ashx user_id=12896151&size=48 94.96.36.135 HTTP/1.1 Mozilla/4.0+(compatible;+MSIE+8.0;+Windows+NT+6.1;+Trident/4.0;+SLCC2;+.NET+CLR+2.0.50727;+.NET+CLR+3.5.30729;+.NET+CLR+3.0.30729;+Media+Center+PC+6.0;+InfoPath.2) http://shabik.net.sa/chatroom2/main.aspx?roomId=54 302 466 296
    #2010-06-12 16:00:00 POST /rpc/photos/RatingProxy.ashx - 188.117.114.228 HTTP/1.1 Mozilla/5.0+(Windows;+U;+Windows+NT+5.1;+en-US)+AppleWebKit/533.4+(KHTML,+like+Gecko)+Chrome/5.0.375.70+Safari/533.4 http://shabik.net.sa/photos/albums.aspx?user_id=3319753&photo_id=13017261&album_id=998364 200 178 312

    oem_name='Telk_Armor'
    stat_category='website'

    stat_plan=Stat_plan()
    
    stat_sql_client_download_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'j2me':r'(/)j2me/armorlife.*?\.jad', \
                                                       'j2me_jar':r'(/)j2me/armorlife.*?\.jar', \
                                                       'symbian_2':r'(/)symbian/armorlife.*?\-2\.sis', \
                                                       'symbian_3':r'(/)symbian/armorlife.*?\-3\.sisx', \
                                                       'symbian_5':r'(/)symbian/armorlife.*?\-5\.sisx', \
                                                       'android':r'(/)android/armorlife.*?\.apk', \
                                                       'winmobile_6':r'(/)wm/armorlife.*?wm6', \
                                                       'winmobile_5':r'(/)wm/armorlife.*?wm5', \
                                                       'blackberry_start':r'(/)blackberry/armorlifebb.*?\.jad', \
                                                       'blackberry_finish':r'(/)blackberry/armorlifebb\-5\.cod'}, \
                                   where={'client_download':r'(GET|POST) /'}, \
                                   group_by={'daily':get_current_date})
    
    stat_plan.add_stat_sql(stat_sql_client_download_daily)

    '''
    stat_sql_client_download_phone_brand_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'phone':r'(.)'}, \
                                   where={'client_download':r'(GET|POST) /'}, \
                                   group_by={'daily':get_current_date,'client_brand':get_phone_brand})
    
    stat_plan.add_stat_sql(stat_sql_client_download_phone_brand_daily)
        
    stat_sql_client_download_public_phone_brand_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'phone':r'(.)'}, \
                                   where={'client_download':r'(GET|POST) /'}, \
                                   group_by={'daily':get_current_date,'client_brand':get_phone_brand})
    
    stat_plan.add_stat_sql(stat_sql_client_download_public_phone_brand_daily)
    '''

    # download device

    stat_sql_client_download_os_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'download':r'(.)'}, \
                                   where={'client_download_page':r'(GET|POST) /download.aspx'}, \
                                   group_by={'daily':get_current_date,'by_os_type':helper_regex.get_supported_platform_from_browser_user_agent})
    
    stat_plan.add_stat_sql(stat_sql_client_download_os_daily)

    stat_plan.add_log_source(r'\\192.168.100.21\W3SVC1_download_log\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'.log')

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_website(my_date)
