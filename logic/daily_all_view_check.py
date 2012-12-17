
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

select * from view

where
(
    `name` like '%STC%'
    and
    (
        `sql` rlike 'viva([[:>:]]|[^_]|_[[:>:]]|_[^b]|_b[[:>:]]|_b[^h])' /*mysql has no regex predict syntax..*/
        or `sql` like '%viva_bh%'
        or `sql` like '%umniah%'
        or `sql` like '%telk_armor%'
        or `sql` like '%vodafone%'
        or `sql` rlike 'mozat[^\\.]'
    )
)
or
(
    `name` rlike 'viva([[:>:]]|[^_]|_[[:>:]]|_[^b]|_b[[:>:]]|_b[^h])'
    and
    (
    `sql` like '%stc%'
    or `sql` like '%viva_bh%'
    or `sql` like '%umniah%'
    or `sql` like '%telk_armor%'
    or `sql` like '%vodafone%'
    or `sql` rlike 'mozat[^\\.]'
    )
)
or
(
    `name` like '%Viva_BH%'
    and
    (
    `sql` like '%stc%'
    or `sql` rlike 'viva([[:>:]]|[^_]|_[[:>:]]|_[^b]|_b[[:>:]]|_b[^h])'
    or `sql` like '%umniah%'
    or `sql` like '%telk_armor%'
    or `sql` like '%vodafone%'
    or `sql` rlike 'mozat[^\\.]'
    )
)
or
(
    `name` like '%Umniah%'
    and
    (
    `sql` like '%stc%'
    or `sql` rlike 'viva([[:>:]]|[^_]|_[[:>:]]|_[^b]|_b[[:>:]]|_b[^h])'
    or `sql` like '%viva_bh%'
    or `sql` like '%telk_armor%'
    or `sql` like '%vodafone%'
    or `sql` rlike 'mozat[^\\.]'
    )
)
or
(
    `name` like '%Telk_Armor%'
    and
    (
    `sql` like '%stc%'
    or `sql` rlike 'viva([[:>:]]|[^_]|_[[:>:]]|_[^b]|_b[[:>:]]|_b[^h])'
    or `sql` like '%viva_bh%'
    or `sql` like '%umniah%'
    or `sql` like '%vodafone%'
    or `sql` rlike 'mozat[^\\.]'
    )
)
or
(
    `name` like '%Vodafone%'
    and
    (
    `sql` like '%stc%'
    or `sql` rlike 'viva([[:>:]]|[^_]|_[[:>:]]|_[^b]|_b[[:>:]]|_b[^h])'
    or `sql` like '%viva_bh%'
    or `sql` like '%umniah%'
    or `sql` like '%telk_armor%'
    or `sql` rlike 'mozat[^\\.]'
    )
)
or
(
    `name` rlike 'mozat[^\\.]'
    and
    (
    `sql` like '%stc%'
    or `sql` rlike 'viva([[:>:]]|[^_]|_[[:>:]]|_[^b]|_b[[:>:]]|_b[^h])'
    or `sql` like '%viva_bh%'
    or `sql` like '%umniah%'
    or `sql` like '%vodafone%'
    or `sql` like '%telk_armor%'
    )
)''')

result_table = db.store_result()

mail_title='stat portal: view check report'
counter=0

while 1:
    row_error_data = result_table.fetch_row(how=1)
    if not row_error_data:
        break
    print row_error_data
    counter+=1

if counter>0:
    mail_content='found view error:'+str(counter) \
                  +'\n'+'<a href="https://statportal.morange.com/xstat/htdocs/view.php?id=458">https://statportal.morange.com/xstat/htdocs/view.php?id=496</a>'
    helper_mail.send_mail(title=mail_title,content_html=mail_content)
    


