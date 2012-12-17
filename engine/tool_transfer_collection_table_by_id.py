
import config
import glob
import re
import helper_regex
import helper_mysql
from helper_mysql import db
import _mysql

helper_mysql.db_throw_all_exception=True

source_conn=config._conn_stat_portal_158
target_conn=config._conn_stat_portal_142
config.conn_stat_portal=config._conn_stat_portal_158


target_tables=[
	'collection',
]


print helper_regex.get_time_str_now()

for table_name in target_tables:
    
    #current_max_id=helper_mysql.get_raw_data(oem_name='Stat_Portal',category='data_migrate',key='max_transfered_id',sub_key=table_name,default_value=0,table_name='raw_data_debug',date='',db_conn=None)
    
    current_start_id=helper_mysql.get_raw_data(oem_name='Stat_Portal',category='data_migrate',key='max_transfered_id',sub_key=table_name,default_value=0,table_name='raw_data_debug',date='',db_conn=None)

    source_step=10000
    target_step=10000

    for i in range(0,100000000):
        current_start_id+=1
        print '===now at:',table_name,current_start_id

        source_rows=helper_mysql.fetch_rows(sql=r'''
        select * from %s where id>=%s order by id limit %s
        ''' % (table_name,current_start_id,source_step),db_conn=source_conn)
        if not source_rows:
            print '===finished at:',table_name,current_start_id
            print helper_regex.get_time_str_now()
            break        

        print 'length of source:',len(source_rows)
        print helper_regex.get_time_str_now()

        sql_temp=[]

        for row in source_rows:
            sql_temp.append("('%s','%s','%s','%s','%s')" % \
            (row['id'],row['created_on'],row['element_count'],row['element_string_md5'],helper_mysql.escape_string(row['element_string']),))
            current_start_id=max(current_start_id,int(row['id']))

        for i in range(0,100000001,target_step):
            if i>len(sql_temp) or not sql_temp[i:min(i+target_step,len(sql_temp)+1)]:
                break        
            sql='replace into '+table_name+' (id,created_on,element_count,element_string_md5,element_string) values '+(','.join(sql_temp[i:min(i+target_step,len(sql_temp)+1)]))

            print helper_regex.get_time_str_now()+' slice: ',i,', affeted:',helper_mysql.execute(sql,db_conn=target_conn)

        helper_mysql.put_raw_data(oem_name='Stat_Portal',category='data_migrate',key='max_transfered_id',sub_key=table_name,value=current_start_id,table_name='raw_data_debug',date='',db_conn=None)
        
        print '===saved max_id:',current_start_id


print helper_regex.get_time_str_now()

exit()



