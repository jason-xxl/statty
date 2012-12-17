import _mysql
import helper_regex
import helper_mysql
import helper_file
import helper_math
import config
import sys

import time
from datetime import date

def update_view(view_name,func_to_replace_description=None,func_to_replace_sql=None):

    print view_name

    sql='''
    select *
    from `view`
    where `name`='%s'
    ''' % (view_name,)

    print sql
    view_row=helper_mysql.fetch_row(sql=sql)

    if not view_row:
        print view_name,'not found'
        return -1
    else:
        print 'view found'
    
    description=view_row['description']
    sql=view_row['sql']

    if func_to_replace_description:
        description=func_to_replace_description(description)
    
    if func_to_replace_sql:
        sql=func_to_replace_sql(sql)

    sql=r'''
    
    update `view`
    set `sql`='%s',`description`='%s'
    where `name`='%s'

    ''' %(helper_mysql.escape_string(sql),helper_mysql.escape_string(description),view_name)
    
    print sql

    helper_mysql.execute(sql)

    return 1


def replace_view(view_name,view_sql='',view_description='',charting_javascript=''):
    
    view_id=helper_mysql.fetch_row(sql=r'''
    select id from `view` where `name`='%s'
    ''' % (view_name,))

    view_id=view_id['id'] if view_id.has_key('id') else ''

    sql=r'''
    
    replace into `view` (`id`,`name`,`sql`,`description`,`charting_javascript`)
    values ('%s','%s','%s','%s','%s')

    ''' % (view_id,view_name,helper_mysql.escape_string(view_sql),helper_mysql.escape_string(view_description), \
            helper_mysql.escape_string(charting_javascript))
    print sql
    return helper_mysql.execute(sql)

def grant_view(view_name,group_id):
    
    view_id=helper_mysql.fetch_row(sql=r'''
    select id from `view` where `name`='%s'
    ''' % (view_name,))

    view_id=view_id['id'] if view_id.has_key('id') else ''

    if view_id:
        helper_mysql.execute(sql=r'''
        replace INTO `mozat_stat`.`group_to_view` (
        `group_id` ,
        `view_id`
        )
        VALUES (
        '%s', '%s'
        );
        ''' % (group_id,view_id))
        return 1
    return 0

if __name__ =='__main__':
    update_view(view_name='Test Report Overall New User Login Rate Daily', \
                func_to_replace_description=lambda line:line.replace('a','A'), \
                func_to_replace_sql=lambda line:line.replace('b','B'))
    pass
