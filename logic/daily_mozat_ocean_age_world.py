import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config

current_date=''

def get_current_date(line):
    global current_date
    return current_date

def stat_ocean_age_world(my_date): # run on 5:00 a.m. , calculate yesterday's data

    global current_date
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    oem_name='Mozat'
    stat_category='ocean_age_world'

    # daily stat
    
    stat_plan=Stat_plan()

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_int_value={'retain_rate':r'"retain_rate":(\d+)', \
                                                    'fishman_count':r'"fishman_count":(\d+)', \
                                                    'owned_fishery_count':r'"owned_fishery_count":(\d+)', \
                                                    'virgin_fishery_count':r'"virgin_fishery_count":(\d+)', \
                                                    'warship_count':r'"warship_count":(\d+)', \
                                                    'ship_count':r'"ship_count":(\d+)', \
                                                    'family_count':r'"family_count":(\d+)', \
                                                     },
                                    where={'oaw':r'^({)'}, \
                                    group_by={'daily':get_current_date}))

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_int_value={
                                                    'level0':r'"level0":(\d+)', \
                                                    'level1':r'"level1":(\d+)', \
                                                    'level10':r'"level10":(\d+)', \
                                                    'level11':r'"level11":(\d+)', \
                                                    'level12':r'"level12":(\d+)', \
                                                    'level13':r'"level13":(\d+)', \
                                                    'level14':r'"level14":(\d+)', \
                                                    'level2':r'"level2":(\d+)', \
                                                    'level3':r'"level3":(\d+)', \
                                                    'level4':r'"level4":(\d+)', \
                                                    'level5':r'"level5":(\d+)', \
                                                    'level6':r'"level6":(\d+)', \
                                                    'level7':r'"level7":(\d+)', \
                                                    'level8':r'"level8":(\d+)', \
                                                    'level9':r'"level9":(\d+)'},
                                    where={'level':r'({)'}, \
                                    group_by={'daily':get_current_date}))

    stat_plan.add_url_sources(r'http://mozat-oaw.i.mozat.com:8003/get_stat_1?'
                             +'year='+datetime.fromtimestamp(my_date).strftime('%Y')
                             +'&month='+datetime.fromtimestamp(my_date).strftime('%m')
                             +'&day='+datetime.fromtimestamp(my_date).strftime('%d'))
    
    stat_plan.run()

    #daily online curve
    
    stat_plan=Stat_plan()

    stat_plan.add_stat_sql(Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_int_value={'0':r'"0":(\d+)', \
                                                     '1':r'"1":(\d+)', \
                                                     '2':r'"2":(\d+)', \
                                                     '3':r'"3":(\d+)', \
                                                     '4':r'"4":(\d+)', \
                                                     '5':r'"5":(\d+)', \
                                                     '6':r'"6":(\d+)', \
                                                     '7':r'"7":(\d+)', \
                                                     '8':r'"8":(\d+)', \
                                                     '9':r'"9":(\d+)', \
                                                     '10':r'"10":(\d+)', \
                                                     '11':r'"11":(\d+)', \
                                                     '12':r'"12":(\d+)', \
                                                     '13':r'"13":(\d+)', \
                                                     '14':r'"14":(\d+)', \
                                                     '15':r'"15":(\d+)', \
                                                     '16':r'"16":(\d+)', \
                                                     '17':r'"17":(\d+)', \
                                                     '18':r'"18":(\d+)', \
                                                     '19':r'"19":(\d+)', \
                                                     '20':r'"20":(\d+)', \
                                                     '21':r'"21":(\d+)', \
                                                     '22':r'"22":(\d+)', \
                                                     '23':r'"23":(\d+)', \
                                                     },
                                    where={'oaw_online_user':r'({)"0"'}, \
                                    group_by={'daily':get_current_date}))

    stat_plan.add_url_sources(r'http://mozat-oaw.i.mozat.com:8003/get_stat_n?'
                             +'year='+datetime.fromtimestamp(my_date).strftime('%Y')
                             +'&month='+datetime.fromtimestamp(my_date).strftime('%m')
                             +'&day='+datetime.fromtimestamp(my_date).strftime('%d'))
    
    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_ocean_age_world(my_date)

