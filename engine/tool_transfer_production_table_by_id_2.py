
import config
import glob
import re
import helper_regex
import helper_mysql
from helper_mysql import db
import _mysql
import time

helper_mysql.db_throw_all_exception=True

source_conn=config.conn_stc_1
target_conn=config._conn_stat_portal_142

for i in range(0,100000000,50000):
    
    sql=r'''
    replace INTO `production_copy_friendship_stc_2`(`user_id`, `friend_id`, `following`, `followed`, `blocking`, `blocked`, `flags`, `created_on`, `modified_on`) 
    select `user_id`,`friend_id`,`following`,`followed`,`blocking`,`blocked`,`flags`,`created_on`,`modified_on`
    from production_copy_friendship_stc where id>=%s and id<%s
    ''' % (i,i+50000)
    
    helper_mysql.execute(sql)
    print i
    
    #break




