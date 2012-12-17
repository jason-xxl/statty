import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mysql

helper_mysql.quick_insert=True

def stat_moagent(my_date):

    oem_name='Viva'
    stat_category='moagent'
    db_name='data_int_user_info_viva'

    # 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    stat_plan=Stat_plan(plan_name='daily-moagent-umniah')
    
    # app uv
    
    stat_sql_user_agent_str=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_text_value={'user_agent':r'userAgent=(.*?)(?:&|\s*$)'}, \
                                   #select_first_int_value={'client_width':r'moclientwidth=(\d+)', \
                                   #                        'client_height':r'moclientheight=(\d+)', \
                                   #                        'ip':helper_regex.ip_to_number}, \
                                   where={'from_app_request':r'(u)serAgent=[^&$]'}, \
                                   group_by={'by_monet_id':r'monetid=(\d+)'}, \
                                   db_name=db_name, \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)
    
    stat_plan.add_stat_sql(stat_sql_user_agent_str)


    stat_plan.add_log_source(r'\\192.168.0.122\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))
    stat_plan.add_log_source(r'\\192.168.0.123\logs_moagent\internal_perf.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    stat_plan.run()



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)


