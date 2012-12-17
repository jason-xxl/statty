
import glob
import re
import helper_regex
from helper_mysql import db
import _mysql

from datetime import datetime
import time

values=[
3441,
3229,
3189,
2778,
2918,
2847,
2420,
]
values.reverse()
do_execute=True

start_date='2011-06-30' #also the biggest date
oem_name='Vodafone'
category='sub'
key='daily_fresh_subscriber_include_unsub'


ts=time.mktime(time.strptime(start_date,'%Y-%m-%d'))


today_date=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')

for i in values:
    date=datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
    print date + ':' +str(i)

    sql=(r'''
    update 
    raw_data
    set `key`=`key`+'_expired_'+'%s'
    where `oem_name`='%s'
    and `category`='%s'
    and `key`='%s'
    and `date`='%s'
    /*limit 1*/
    ''' % (today_date,oem_name,category,key,date))

    print sql
    if do_execute:
        db.query(sql)

    """    
    sql=(r'''
    delete 
    FROM raw_data
    where `oem_name`='%s'
    and `category`='%s'
    and `key`='%s'
    and `date`='%s'
    limit 1
    ''' % (oem_name,category,key,date))

    print sql
    if do_execute:
        db.query(sql)
    """

    sql=(r'''
    insert 
    into raw_data
    (oem_name,category,`key`,`date`,`value`)
    values ('%s','%s','%s','%s','%s')
    ''' % (oem_name,category,key,date,str(i)))

    print sql
    if do_execute:
        db.query(sql)
        pass

    ts=ts-3600*24    


#db.close()






