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

def stat_website(my_date):

    global current_date
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    #2010-06-12 16:00:00 GET /rpc/avatar.ashx user_id=12896151&size=48 94.96.36.135 HTTP/1.1 Mozilla/4.0+(compatible;+MSIE+8.0;+Windows+NT+6.1;+Trident/4.0;+SLCC2;+.NET+CLR+2.0.50727;+.NET+CLR+3.5.30729;+.NET+CLR+3.0.30729;+Media+Center+PC+6.0;+InfoPath.2) http://shabik.net.sa/chatroom2/main.aspx?roomId=54 302 466 296
    #2010-06-12 16:00:00 POST /rpc/photos/RatingProxy.ashx - 188.117.114.228 HTTP/1.1 Mozilla/5.0+(Windows;+U;+Windows+NT+5.1;+en-US)+AppleWebKit/533.4+(KHTML,+like+Gecko)+Chrome/5.0.375.70+Safari/533.4 http://shabik.net.sa/photos/albums.aspx?user_id=3319753&photo_id=13017261&album_id=998364 200 178 312

    oem_name='Telk_Armor'
    stat_category='website'

    stat_plan=Stat_plan()
    
    stat_sql_client_download_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'homepage':r'(GET) \/wap\/(?:ID\/)?download\.aspx[ignorecase]'}, \
                                   where={'client_download':r'(GET|POST)'}, \
                                   group_by={'daily':get_current_date})
    
    stat_plan.add_stat_sql(stat_sql_client_download_daily)

    stat_plan.add_log_source(r'\\192.168.100.21\w3svc1451704712\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'.log')

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_website(my_date)
