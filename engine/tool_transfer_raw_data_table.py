
import config
import glob
import re
import helper_regex
import helper_mysql
from helper_mysql import db
import _mysql



source_conn=config._conn_stat_portal_158
target_conn=config._conn_stat_portal_142

target_tables=[
    'raw_data_url_pattern'
]

print helper_regex.get_time_str_now()

for table_name in target_tables:
    
    #current_max_id=helper_mysql.get_raw_data(oem_name='Stat_Portal',category='data_migrate',key='max_transfered_id',sub_key=table_name,default_value=0,table_name='raw_data_debug',date='',db_conn=None)
    
    begin_date='2011-07-11'
    step=1000

    for i in range(0,1000):
        current_date=helper_regex.date_add(begin_date,-i)
        print table_name,current_date

        r'''
        select * 
        from %s 
        where date='%s' and `oem_name`="Vodafone" and `category`="website"
        ''' % (table_name,current_date)

        source_rows=helper_mysql.fetch_rows(sql=r'''
        select * 
        from %s 
        where date='%s' and `oem_name`="Vodafone" and `category`="website"
        ''' % (table_name,current_date),db_conn=source_conn)
        

        #exit()
        print 'length of source:',len(source_rows)
        print helper_regex.get_time_str_now()

        sql_temp=[]
        for row in source_rows:
            sql_temp.append("('%s','%s','%s','%s','%s','%s','%s')" % \
            (row['oem_name'],row['category'],row['key'],helper_mysql.escape_string(row['sub_key']),row['date'],row['value'],row['created_on'],))

        for i in range(0,100000001,step):
            print helper_regex.get_time_str_now()
            print 'slice:',i
            if i>len(sql_temp):
                break        
            sql='replace into '+table_name+' (oem_name,category,`key`,sub_key,date,`value`,created_on) values '+(','.join(sql_temp[i:min(i+step,len(sql_temp)+1)]))
            #print sql   
            helper_mysql.execute(sql,db_conn=target_conn)


print helper_regex.get_time_str_now()

exit()



