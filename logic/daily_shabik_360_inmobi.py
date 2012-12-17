from stat_plan import Stat_plan
from stat_sql import Stat_sql
import time
import helper_regex
import config
from datetime import date 
import tool_qlx

'''
Timezone:
  Singapore: UTC+8
  log File Name Time: UTC+3
  Log File Content Time: UTC+0

Format Examples:
1 2011-08-13 16:02:12 GET /360/stc.jar - 84.235.75.80 HTTP/1.1 Nokia7230/5.0+(06.90)+Profile/MIDP-2.1+Configuration/CLDC-1.1 - 200 517172 34003
2 2012-05-23 20:04:02 GET /360.aspx frm=inmobi 188.51.59.205 HTTP/1.1 Mozilla/5.0+(BlackBerry;+U;+BlackBerry+9300;+ar)+AppleWebKit/534.8++(KHTML,+like+Gecko)+Version/6.0.0.546+Mobile+Safari/534.8+ frm=inmobi;+rand=1763247966;+ASP.NET_SessionId=ac45lxt1b51amivjgdfgahwq http://tubidy.mobi/watch/cgIcfylsjcZKBZMS_2F5alIA_3D_3D/3gp-mobile/fs 200 5683 203

Fields:
  ASP.NET_SessionId: session id
  
Inmobi:  
  "frm=inmobi" appears in both the cookie (expire within one day) and the link.  

Note: 
1 NO session ID for 2012-06-29 - 2012-07-05
2 Incomplete session ID: 2012-06-28 0-12pm, 2012-07-06 0-12pm
'''
def check_time_zone(my_time):
    '''
    make sure the file name time is UTC3, and the content time is UTC0
    '''
    pass
     
def stat_website(my_time):
    print(tool_qlx.get_utc_datetime(my_time, tool_qlx.UTC8))
    # make sure the date of the three places are the same. 
    """
    assert(tool_qlx.get_utc_datetime(my_time, tool_qlx.UTC8).date() \
           == tool_qlx.get_utc_datetime(my_time, tool_qlx.UTC3).date())
#        \== tool_qlx.get_utc_datetime(my_time, tool_qlx.UTC0).date())
    """
    # the date in UTC3
    current_date_object = tool_qlx.get_utc_datetime(my_time, tool_qlx.UTC3).date()
    current_date = current_date_object.strftime('%Y-%m-%d')
    print(current_date_object,current_date)
    
    oem_name='Shabik_360'
    stat_category='website'
    db_name='raw_data_device_shabik_360'


    stat_plan = Stat_plan()
    
    # overall 
    stat_sql_overall_daily =Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'action':r'(.)'},
                                   select_count_distinct_collection={'session':r'NET_SessionId=([0-9a-z]+)'},
                                   where={'from_inmobi':r'(f)rm=inmobi'}, \
                                   group_by={'daily':lambda line:current_date},
                                   db_name=db_name)

    # download file
    stat_sql_download_file_daily = Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'session':r'NET_SessionId=([0-9a-z]+)'},
                                   where={'from_inmobi':r'(f)rm=inmobi', \
                                          'is_client_file':helper_regex.client_file_pattern}, \
                                   group_by={'daily':lambda line:current_date},
                                   db_name=db_name)

    stat_plan.add_stat_sql(stat_sql_overall_daily)
    stat_plan.add_stat_sql(stat_sql_download_file_daily)


    if current_date_object <= date(2012,6,27):
        stat_plan.add_url_sources(tool_qlx.fill_time_pattern_string(\
                    time_string='http://212.100.219.210/i.shabik.net.sa/W3SVC1160380090/ex{%y%m%d%H}.log',\
                    src_timestamp=my_time, dst_utc=tool_qlx.UTC3))
    else:
        stat_plan.add_url_sources(tool_qlx.fill_time_pattern_string(\
                    time_string='http://212.100.219.210/z.i.shabik.sa/W3SVC1438558973/ex{%y%m%d%H}.log',\
                    src_timestamp=my_time, dst_utc=tool_qlx.UTC3))

    stat_plan.dump_sources()

    stat_plan.run()
 

if __name__=='__main__':
    for i in range(config.day_to_update_stat,0,-1):
        my_time=time.time()-3600*24*i
        stat_website(my_time)