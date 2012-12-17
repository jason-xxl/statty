
import glob
import re
import helper_regex
from helper_mysql import db
import _mysql

view_ids=[]

db.query(r'''

    SELECT *
    FROM `virtual_view` WHERE id in (106,104,102,97,96,95,94)
    /*name like '%STC User Online Time Average & Top 100'*/
    ORDER BY id ASC 
''')

result_view = db.store_result()

while 1:
    row_view = result_view.fetch_row(how=2)
    if not row_view:
        break
    
    row_view=row_view[0]
    print row_view['virtual_view.name']
    view_ids.append(row_view['virtual_view.id'])


print view_ids


for view_id in view_ids:
    db.query(r'select * from `virtual_view` where id='+str(view_id))
    result_set = db.store_result()

    row = result_set.fetch_row(how=2)
    # {'virtual_view.id': '102', 'virtual_view.created_on': '2010-10-29 11:53:31','virtual_view.description': '', 'virtual_view.day_range_default': '0', 'virtual_view.subscribed': '0', 'virtual_view.options': '', 'virtual_view.enabled': '1', 'virtual_view.user_id': '1', 'virtual_view.name': 'STC User Growth'}
    
    if row:
        row=row[0]
        print row
        
        db.query(r'''
        
        insert into `virtual_view`
        (`user_id`,`name`,`day_range_default`,`description`,`enabled`,`subscribed`,`options`)
        select `user_id`,%s as `name`,`day_range_default`,%s as `description`,`enabled`,`subscribed`,`options`
        from `virtual_view`
        where `id`='%s'
        
        ''' %(db.string_literal(row['virtual_view.name'].replace('STC','Viva_BH'))
              ,db.string_literal(row['virtual_view.description'].replace('STC','Viva_BH'))
              ,str(view_id)))
                 
        new_id=db.insert_id()
        print new_id

        if new_id>0:
            
            db.query(r'''

            insert into `virtual_view_chart`                
            (`view_id`,`name`,`description`,`tab_order`,`columns`,`column_value_tune`,`options`)
            
            select '%s' as `view_id`

            ,replace(`name`,'STC','Viva_BH')
            ,replace(`description`,'STC','Viva_BH'),`tab_order`
            ,replace(`columns`,'STC','Viva_BH'),`column_value_tune`,`options`
            
            from `virtual_view_chart`
            where `view_id`='%s'
            
            ''' % (str(new_id),str(view_id)))

            db.query(r'''

            insert into `virtual_view_item`                
            (`virtual_view_id`,`col_name`,`alias`,`view_id`,`seq`,`options`)
            
            select '%s' as `virtual_view_id`
            ,replace(`col_name`,'STC','Viva_BH')
            ,replace(`alias`,'STC','Viva_BH')
            ,`view_id`
            ,`seq`
            ,`options`
            
            from `virtual_view_item`
            where `virtual_view_id`='%s'
            
            ''' % (str(new_id),str(view_id)))






#db.close()






