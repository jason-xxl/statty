
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
#config.conn_stat_portal=config._conn_stat_portal_158_2


def calculate_ndays_unique(key_space,db_name,date_units):

    # calculate n days' unique

    min_date=helper_mysql.get_one_value_string(r'''
    
    select 
    
    min(`date`)

    from `%s`
    where `oem_name`='%s'
    and `category`='%s'
    and `key`='%s_collection_id'
    and `sub_key`='%s'
    and `date`>='2011-12-16'

    ''' % (db_name,key_space['oem_name'],key_space['category'],key_space['key'],key_space['sub_key']))


    max_date=helper_mysql.get_one_value_string(r'''
    
    select 
    
    max(`date`)

    from `%s`
    where `oem_name`='%s'
    and `category`='%s'
    and `key`='%s_collection_id'
    and `sub_key`='%s'
    and `date`>='2011-12-16'

    ''' % (db_name,key_space['oem_name'],key_space['category'],key_space['key'],key_space['sub_key']))


    if not min_date or not max_date:
        print 'date error.'
        return

    date_temp=min_date

    #print date_temp
    #exit()
    
    while True:

        if date_temp>=max_date:
            break

        for date_unit in date_units:

            unique,total,average=helper_math.calculate_count_distinct(date_unit=date_unit,oem_name=key_space['oem_name'],category=key_space['category'],key=key_space['key'],sub_key=key_space['sub_key'],date=date_temp,table_name=db_name,allow_collection_empty=True)

            print 'distinct collection calc '+date_temp+': date_unit '+str(date_unit)+' unique '+str(unique)+' total '+str(total)+' average '+str(average)
            #exit()

            key_prefix=helper_regex.regex_replace('_unique$','',key_space['key'])

            if unique>0:
                suffix=str(date_unit)
                if isinstance(date_unit, (int, long)):
                    suffix+='_days'

                helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'],key=key_prefix+'_'+suffix+'_unique',sub_key=key_space['sub_key'],value=unique,date=date_temp,table_name=db_name)
                helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'],key=key_prefix+'_'+suffix+'_unique_base',sub_key=key_space['sub_key'],value=total,date=date_temp,table_name=db_name)
                helper_mysql.put_raw_data(oem_name=key_space['oem_name'],category=key_space['category'],key=key_prefix+'_'+suffix+'_unique_average',sub_key=key_space['sub_key'],value=average,date=date_temp,table_name=db_name)
        
        date_temp=helper_regex.date_add(date_temp,1)


def calculate_collection(oem_name,category,db_name='raw_data',date_units_nday_unique=[600],date_units_retain_rate=[1,2,3]):
    


    #fetch target keys

    config.collection_cache_enabled=True
    



    target_collection_name_sql=r'''

    select 

    distinct `oem_name`
    ,`category`
    ,replace(`key`,'_average','') as `key`
    ,`sub_key`

    from `raw_data_shabik_360`

    where `oem_name`="Shabik_360" and `category`="moagent" and `key`="app_page_daily_visitor_unique"

    '''


    keys=helper_mysql.fetch_rows(target_collection_name_sql)

    print keys
    #exit()


    
    #do action

    for key_space in keys:
        print key_space
        calculate_ndays_unique(key_space,db_name,date_units_nday_unique)




if __name__=='__main__':
    """
    helper_math.calculate_count_distinct(date_unit='monthly',oem_name='Shabik_360',category='moagent',key='app_page_daily_visitor_unique',sub_key='',date='2012-03-31',table_name='raw_data_shabik_360',allow_collection_empty=True)

    exit()

    r=[]
    for i in helper_regex.date_iterator('2012-02-01','2012-05-01'):
        r.append(( i,len(helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent',key='app_page_daily_visitor_unique',sub_key='',date=i,table_name='raw_data_shabik_360'))))

    for i in r:
        print i
        
    exit()
    """


    calculate_collection(oem_name='Shabik_360',category='moagent',db_name='raw_data_shabik_360',date_units_nday_unique=['monthly','weekly'],date_units_retain_rate=[])
    #calculate_collection('Shabik_360','moagent',db_name='raw_data')
    #calculate_retention_rate(key_space={'oem_name':'Shabik_360','category':'moagent','key':'app_page_by_url_pattern_daily_visitor_unique', \
    #'sub_key':'mobileshabik.morange.com/mophoto_popular_photos.aspx?src_evflg_1'},db_name='data_url_pattern_shabik_360',date_units=[1,2,3])

