
import glob
import re
import helper_regex
import helper_mysql
from helper_mysql import db
import _mysql

collections=[]

db.query(r'''

    SELECT
    
    date AS `Time`
    ,`created_on`
    ,sub_key
    ,value as `collection_id`

    FROM `raw_data`

    WHERE (

    `oem_name`='Vodafone' and category='moagent' and `key` = 'app_page_by_morange_version_daily_user_unique_collection_id'
    and `sub_key` like '%MIDP%'

    ) AND date ='2011-05-28'
    ORDER BY sub_key DESC 

''')

result_view = db.store_result()

while 1:
    row_view = result_view.fetch_row(how=2)
    if not row_view:
        break
    
    row_view=row_view[0]
    col_id=row_view['raw_data.collection_id']
    collections.append(helper_mysql.get_raw_collection_by_id(col_id))

col_total=set([])
for i in collections:
    col_total |= i
    print len(i)

print '='+str(len(col_total))


#print collections
db.close()






