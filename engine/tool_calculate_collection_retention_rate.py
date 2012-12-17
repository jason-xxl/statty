
import config
import glob
import re
import helper_regex
import helper_mysql
import helper_math
from helper_mysql import db
import _mysql

'''
'''

config.collection_cache_enabled=True
config.conn_stat_portal=config._conn_stat_portal_158_2


def calculate_collection(oem_name,category,db_name='raw_data',date_units_nday_unique=['weekly','monthly'],date_units_retain_rate=[1,2,3]):
    
    #fetch target keys

    config.collection_cache_enabled=True
    
    target_collection_name_sql=r'''

    select 

    distinct `oem_name`
    ,`category`
    ,replace(`key`,'_average','') as `key`
    ,`sub_key`

    from `%s`

    where `oem_name`='%s'
    and `category`='%s'
    
    and `key`='app_page_by_app_daily_visitor_unique'
    and `sub_key`='friend'

    /*and `key`='app_page_by_app_daily_visitor_unique_average'*/
    /*and `sub_key`=''*/
    
    /*
    and `key` like '%%_unique_average'
    and `key` not like '%%weekly_unique%%'
    and `key` not like '%%monthly_unique%%'
    and `key` not like '%%_hourly_%%'
    and `key` not like '%%days_unique%%'
    and `key` like '%%_daily_%%'
    */

    ''' % (db_name,oem_name,category)

    keys=helper_mysql.fetch_rows(target_collection_name_sql)

    print keys
    #exit()


    
    #do action

    for key_space in keys:
        #calculate_ndays_unique(key_space,db_name,date_units_nday_unique)
        calculate_retention_rate(key_space,db_name,date_units_retain_rate)

    exit()





def calculate_retention_rate(key_space,db_name,date_units=[1,2,3]):




    # calculate n days' unique

    min_date='2011-11-01'
    max_date='2011-12-10'

    if not min_date or not max_date:
        print 'date error.'
        return

    print min_date,max_date

    date_temp=min_date

    while True:

        if date_temp>=max_date:
            break
        print date_temp

        #select_retain_rate_by_date
        date_units=[1,2,3]
        with_average_life_cycle=False

        for date_unit in date_units:
            print date_unit
            #retain rate
            base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size \
                                =helper_math.calculate_date_range_retain_rate(\
                                       date_unit=date_unit, \
                                       oem_name=key_space['oem_name'],\
                                       category=key_space['category'],\
                                       key=key_space['key']+'_collection_id',\
                                       sub_key=key_space['sub_key'],\
                                       date=date_temp,\
                                       table_name=db_name)      
                                       
            print base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size

            helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'], \
                                      key=key_space['key']+'_'+str(date_unit)+'_day_base_size',
                                      sub_key=key_space['sub_key'],
                                      date=date_temp,
                                      value=base_size, \
                                      table_name=db_name)                

            helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'], \
                                      key=key_space['key']+'_'+str(date_unit)+'_day_retained_base_size',
                                      sub_key=key_space['sub_key'],
                                      value=retained_base_size, \
                                      table_name=db_name)                

            helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'], \
                                      key=key_space['key']+'_'+str(date_unit)+'_day_lost_base_size',
                                      sub_key=key_space['sub_key'],
                                      date=date_temp,
                                      value=lost_base_size, \
                                      table_name=db_name)                

            helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'], \
                                      key=key_space['key']+'_'+str(date_unit)+'_day_fresh_base_size',
                                      sub_key=key_space['sub_key'],
                                      date=date_temp,
                                      value=fresh_base_size, \
                                      table_name=db_name)                

            helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'], \
                                      key=key_space['key']+'_'+str(date_unit)+'_day_retain_rate',
                                      sub_key=key_space['sub_key'],
                                      date=date_temp,
                                      value=retain_rate, \
                                      table_name=db_name)                

            helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'], \
                                      key=key_space['key']+'_'+str(date_unit)+'_day_fresh_rate',
                                      sub_key=key_space['sub_key'],
                                      date=date_temp,
                                      value=fresh_rate, \
                                      table_name=db_name)                

            helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'], \
                                      key=key_space['key']+'_'+str(date_unit)+'_day_lost_rate',
                                      sub_key=key_space['sub_key'],
                                      date=date_temp,
                                      value=lost_rate, \
                                      table_name=db_name)                
            """
            #avg life cycle
            lost_col_average_life_cycle,retained_col_average_life_cycle,lost_col_dict,retained_col_dict=helper_math.calculate_date_range_average_life_cycle(\
                                       date_unit=date_unit, \
                                       oem_name=key_space['oem_name'],\
                                       category=key_space['category'],\
                                       key=key_space['key']+'_collection_id',\
                                       sub_key=sub_key_temp,\
                                       date=date_temp,\
                                       table_name=db_name)     
                                       
            helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'], \
                                      key=key_space['key']+'_'+str(date_unit)+'_day_lost_col_avg_life',
                                      sub_key=key_space['sub_key'],
                                      date=date_temp,
                                      value=lost_col_average_life_cycle, \
                                      table_name=db_name)                
                                       
            helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'], \
                                      key=key_space['key']+'_'+str(date_unit)+'_day_retained_col_avg_life',
                                      sub_key=key_space['sub_key'],
                                      date=date_temp,
                                      value=retained_col_average_life_cycle, \
                                      table_name=db_name) 
            """

        date_temp=helper_regex.date_add(date_temp,1)
        #exit()


if __name__=='__main__':

    #calculate_collection('Shabik_360','moagent',db_name='raw_data_shabik_360')
    calculate_retention_rate(key_space={'oem_name':'Shabik_360','category':'moagent','key':'app_page_by_url_pattern_daily_visitor_unique', \
    'sub_key':'mobileshabik.morange.com/mophoto_popular_photos.aspx?src_evflg_1'},db_name='data_url_pattern_shabik_360',date_units=[1,2,3])

