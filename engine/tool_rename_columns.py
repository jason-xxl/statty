# coding=utf8


import glob
import re
import helper_regex
from helper_mysql import db
import _mysql

view_ids=[]

db.query(r'''
    SELECT *
    FROM (
    SELECT *
    FROM VIEW WHERE name like '%Telk_Armor Daily Paid User%'
    ) view
    ORDER BY id ASC 
''')

result_view = db.store_result()

columns_to_rename={
    '总有效包日用户数':'Effective Daily Subscribers',
    '总有效包周用户数':'Effective Weekly Subscribers',
    '总有效包月用户数':'Effective Monthly Subscribers',
    '新增包日用户':'New Daily Package Subscriptions',
    '退订包日用户':'Daily Package Unsubscription',
    '新增包周用户':'New Weekly Package Subscriptions',
    '退订包周用户':'Weekly Package Unsubscription',
    '新增包月用户':'New Monthly Package Subscriptions',
    '退订包月用户':'Monthly Package Unsubscription',
    '在线用户峰值':'Peak Online User',
    '平均数据流量 kB (基于总有效用户)':'GPRS Data per User per Day',
    '历史总用户（含退订）':'Total User Including Unsub User',

    }

while 1:
    row_view = result_view.fetch_row(how=2)
    if not row_view:
        break
    
    row_view=row_view[0]
    print row_view['view.name']
    view_ids.append(row_view['view.id'])

print view_ids
#exit()


for id in view_ids:
    db.query(r'select * from view where id='+str(id))
    result_set = db.store_result()

    row = result_set.fetch_row(how=2)
    #({'view.name': 'Operators_Reports STC Overall 5 Average Data Per User', 'view.description': '', 'view.day_range_default': '0', 'view.sql': "...", 'view.default_tab': '0', 'view.chart': '', 'view.id': '71'},)

    if row:
        row=row[0]
        view_id=row['view.id']

        for column_name_old,column_name_new in  columns_to_rename.iteritems():
            sql= r'update view set `sql`=replace(`sql`,"%s","%s") where id=%s' % \
                     ('`'+column_name_old+'`','`'+column_name_new+'`',view_id)
            print sql
            db.query(sql)
            
            sql= r'update chart set columns=replace(columns,"%s","%s") where view_id=%s' % \
                     (column_name_old,column_name_new,view_id)
            print sql
            db.query(sql)

db.close()






