
import config
import helper_regex
import helper_mysql
import time
import helper_redis

helper_mysql.db_throw_all_exception=True

source_conn=config.conn_stc_1

target_tables={
	'friendship.friendship':'production_copy_friendship_stc',
}


print helper_regex.get_time_str_now()

for table_name,target_table_name in target_tables.iteritems():
    
    current_start_id=helper_mysql.get_raw_data(oem_name='Stat_Portal',category='data_migrate_friend_relation',key='max_transfered_id',sub_key=table_name,default_value=0,table_name='raw_data_debug',date='',db_conn=None)
    source_step=50000
    #target_step=50000
    #current_start_id=20000000

    for i in xrange(0,100000001):
        time.sleep(3)
        current_start_id+=1
        print '===now at:',table_name,current_start_id

        sql=r'''
        select id,user_id,friend_id,0+following as following,0+followed as followed,0+blocking as blocking,0+blocked as blocked,0+flags as flags,created_on,modified_on
        from %s 
        where id>=%s and id<%s+%s
        ''' % (table_name,current_start_id,current_start_id,source_step)

        source_rows=helper_mysql.fetch_rows(sql,db_conn=source_conn)
        print sql
        if not source_rows:
            print '===finished at:',table_name,current_start_id
            print helper_regex.get_time_str_now()
            break

        print 'length of source:',len(source_rows)
        print helper_regex.get_time_str_now()

        sql_temp=[]

        for row in source_rows:
            sql_temp.append("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
                (row['id'],row['user_id'],row['friend_id'],row['following'],row['followed'],row['blocking'], \
                row['blocked'],row['flags'],row['created_on'],row['modified_on'],))
            current_start_id=max(current_start_id,int(row['id']))

        for i in range(0,100000001,target_step):
            if i>len(sql_temp) or not sql_temp[i:min(i+target_step,len(sql_temp)+1)]:
                print 'break in replace loop'
                break  

            sql='replace into '+target_table_name+' (id,user_id,friend_id,following,followed,blocking,blocked,flags,created_on,modified_on) values '+ \
                                (','.join(sql_temp[i:min(i+target_step,len(sql_temp)+1)]))
            #print sql
            affected=helper_mysql.execute(sql,db_conn=target_conn)
            print helper_regex.get_time_str_now()+' slice: ',i,', affeted:',affected

        helper_mysql.put_raw_data(oem_name='Stat_Portal',category='data_migrate_friend_relation',key='max_transfered_id',sub_key=table_name,value=current_start_id,table_name='raw_data_debug',date='',db_conn=None)
        
        print '===saved max_id:',current_start_id

print helper_regex.get_time_str_now()

exit()



