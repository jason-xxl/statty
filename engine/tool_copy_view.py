
import glob
import re
import helper_regex
from helper_mysql import db
import _mysql
import helper_mysql

view_ids=[]

db.query(r'''
    SELECT *
    FROM (
    SELECT *
    FROM VIEW WHERE /*name like 'STC%' or name like 'Technical STC%'*/  id in (137,1024,1025)
    ) view
    ORDER BY id ASC 
''')

result_view = db.store_result()

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

        name=row['view.name'].replace('Umniah','Globe')
        sql=row['view.sql'].replace('Umniah','Globe').replace('umniah','globe')

        max_view_id=helper_mysql.get_one_value_int(r'''
        select max(`id`) from `view`
        ''')

        description=row['view.description'].replace(str(row['view.id']),str(max_view_id+1))
        
        print 'view_name:'+name
        print 'sql:'+sql
        print '\n'
        
        db.query(r'insert into view (`name`,`description`,`day_range_default`,`sql`,`conn_string`) values (%s,%s,%s,%s,%s)' \
                              %(db.string_literal(name),db.string_literal(description), \
                                db.string_literal(row['view.day_range_default']),db.string_literal(sql),db.string_literal(row['view.conn_string'])))
        view_id=row['view.id']
        new_id=db.insert_id()
        print 'inserted view:'+str(new_id)

        """
        if new_id:
            db.query(r'select * from chart where view_id='+str(view_id)+' order by id asc')
            result_set_chart = db.store_result()
            chart_ids=[]
            while 1:
                row_chart = result_set_chart.fetch_row(how=2)
                if not row_chart:
                    break
                #print str(row_chart)
                row_chart=row_chart[0]
                chart_ids.append(row_chart['chart.id'])

            for j in chart_ids:
                db.query(r'insert into chart (`view_id`,`name`,`description`,`columns`,`options`) '+ \
                         'select ' + str(new_id) + ' as view_id, `name`,`description`,`columns`,`options` from chart where id='+str(j)) 
                
        """



db.close()






