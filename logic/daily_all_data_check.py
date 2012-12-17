
import glob
import re
import helper_regex
from helper_mysql import db
import _mysql
from datetime import datetime
import time
import config
import helper_mail

##### daily routine #####

ts_last_1_day=time.time()-3600*24
ts_last_7_day=time.time()-3600*24*7

last_1_day=datetime.fromtimestamp(ts_last_1_day).strftime('%Y-%m-%d')
last_7_day=datetime.fromtimestamp(ts_last_7_day).strftime('%Y-%m-%d')


db.query(r'''

select

    base.oem_name
    ,base.category
    ,base.`key`
    ,base.`sub_key`
    ,current.`value` as `value of last day`
    ,base.`value` as `value of 7 day`
    ,format(current.`value`/base.`value`,2) as rate

from raw_data as base

left join (
    select *
    from raw_data
    where date='%s'
    and `key` not like '%%top_key'
    and `key` not like '%%top_value'
    and `key` not like '%%by_url%%'
    and `key` not like '%%by_ip_range%%'
    and `key` not like '%%by_phone_model%%'
    and `key` not like '%%by_client_screen_size%%'
    and `key` not like '%%by_morange_version%%'
    and `key` not like '%%online_time%%'
    and `key` not like '%%data_usage%%'
    and `key` not like '%%expired'
) as current

on

current.oem_name=base.oem_name
and current.category=base.category
and current.`key`=base.`key`
and current.sub_key=base.sub_key

where

base.date='%s'
and base.`key` not like '%%top_key'
and base.`key` not like '%%top_value'
and base.`key` not like '%%by_url%%'
and base.`key` not like '%%by_ip_range%%'
and base.`key` not like '%%by_phone_model%%'
and base.`key` not like '%%by_client_screen_size%%'
and base.`key` not like '%%by_morange_version%%'
and base.`key` not like '%%online_time%%'
and base.`key` not like '%%data_usage%%'
and base.`key` not like '%%expired'
and base.`value` > 100

and (current.`value` is null or base.`value` is null
or current.`value`<=0 or base.`value`<=0)

order by

base.oem_name asc,
base.category asc,
base.`key` asc,
base.sub_key asc,
format(current.`value`/base.`value`,2) desc,
format(current.`value`/base.`value`,2) desc;


''' % (last_1_day,last_7_day))

result_table = db.store_result()

mail_title='stat portal: data check report'
counter=0

while 1:
    row_error_data = result_table.fetch_row(how=1)
    if not row_error_data:
        break
    counter+=1

if counter>0:
    mail_content='found data missing:'+str(counter) \
                  +'\n'+'<a href="https://statportal.morange.com/xstat/htdocs/view.php?id=458">https://statportal.morange.com/xstat/htdocs/view.php?id=458</a>'
    helper_mail.send_mail(title=mail_title,content_html=mail_content)

    

