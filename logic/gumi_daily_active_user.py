import os, sys

ENGINE_ROOT = os.path.join(os.path.dirname(__file__),'../engine')
sys.path.insert(0, os.path.join(ENGINE_ROOT, "."))

from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
from celery.task import task
from datetime import datetime
import time
import helper_regex
import helper_ip
import gumi_helper_user
import config

def get_country_name(line):
    ip=helper_regex.extract(line,r'(\d+\.\d+\.\d+\.\d+)')
    if not ip:
        return 'ip_empty'
    return helper_ip.get_country_code_from_ip(ip)

@task
def stat_im(my_date, flat):
    oem_name='Gumi_puzzle'
    stat_category='user'

    if helper_check_mount.check_mount():
    #test = True
    #if test:
        stat_plan = Stat_plan()

        # Log Sample
        #2012-12-26T16:31:34+08:00, 49.128.46.50, 
        #COOKIES=csrftoken=7TvbWAd7FWMAea1Fk5qBBSPHWgoyL3t7; sessionid=def162d1937932d07ec8455b92feb276; uid=7; 
        #messages=\x2258a5f254b41f41a605113683f5fe0ebbcee3c0b1$[[\x5C\x22__json_message\x5C\x22\x5C05425\x5C054\x5C\x22Successfully signed in as _fb_1035382759.\x5C\x22]]\x22; token=2c14146c723afe1986a309e421be85aa; ak=8667733931654496; dn=\x22iPhone Simulator\x22; dp=\x22iPhone Simulator\x22; v=6.0; vid=B9F75917-A920-4026-ABC4-EC47017CD40D, 
        #"GET /static/message/img/0_avatar.png HTTP/1.1" 200 22119 "http://live.gumi.sg/messaging/" 
        #"Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Mobile/10A403"

        # daily active user
        daily_active_user = Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                         select_count_distinct_collection={'uid':r'uid=(\d+)'}, \
                         where={'live_log':r'(HTTP\/\d\.\d\" +(2\d\d|3\d\d))'}, \
                         group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})'})

        # daily active user by country
        daily_active_user_by_country = Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                         select_count_distinct_collection={'uid':r'uid=(\d+)'}, \
                         where={'live_log':r'(HTTP\/\d\.\d\" +(2\d\d|3\d\d))'}, \
                         group_by={'daily':r'(\d{4}\-\d{2}\-\d{2})', \
                                   'by_country':get_country_name})

        stat_plan.add_stat_sql(daily_active_user)
        stat_plan.add_stat_sql(daily_active_user_by_country)

        server_ip = helper_ip.get_current_server_ip()
        directory = '/mnt/%s/access.log-%s'%(server_ip,datetime.fromtimestamp(my_date).strftime('%Y%m%d'))
        stat_plan.add_log_source(directory)

        #stat_plan.add_log_source(r'/var/log/secure.log')
        stat_plan.run()
        
    else:
        'MOUNT ERROR'

if __name__=='__main__':
    for i in range(30,0,-1):
        my_date=time.time()-3600*24*i
        stat_im(my_date, False)
    #for i in range(1,0,-1):
    #    my_date=time.time()-3600*24*i
    #    stat_im(my_date, False)




