import helper_sql_server
import glob
import os
import helper_regex
import helper_mysql
import helper_file
import config
import time
import codecs

def export_db_to_log(my_date):

    oem_name='Shabik_360'
    stat_category='friend_relation'

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)

    start_time=helper_regex.time_add_by_hour(current_date+' 00:00:00',-3)
    end_time=helper_regex.time_add_by_hour(current_date+' 00:00:00',-3+24)

    source_db=config.conn_stc_1
    source_table='friendship.friendship'
    source_condition_name='adding_friend'

    target_log_name='E:\\RoutineScripts\\log\\%s_%s_%s_record_from_%s.log.%s' \
                    % (oem_name,stat_category,source_condition_name,source_table,current_date)

    start_id=helper_mysql.guess_pk_id_by_time(table_name=source_table, \
                                          target_time=start_time,pk_column_name='id', \
                                          sequence_time_column_name='created_on',db_conn=config.conn_stc_1)

    end_id=helper_mysql.guess_pk_id_by_time(table_name=source_table, \
                                          target_time=end_time,pk_column_name='id', \
                                          sequence_time_column_name='created_on',db_conn=config.conn_stc_1)

    sql=r'''
    
    select id,created_on,user_id,friend_id
    from friendship.friendship
    where id>=%s and id<%s
    and following=1
    order by id asc

    ''' % (start_id,end_id)

    existing_adding=set([])

    target_log=codecs.open(target_log_name,'w','utf-8',None,1024*1024*4)

    rows=helper_mysql.fetch_rows(sql=sql,db_conn=config.conn_stc_1)

    for r in rows:
        if (r['friend_id'],r['user_id']) in existing_adding:
            existing_adding.remove((r['friend_id'],r['user_id']))
            continue
        
        t=helper_regex.time_add_by_hour(r['created_on'],8)
        
        target_log.write('%s [[friend add]] monetid=%s to=%s\n' % \
                            (t,r['user_id'],r['friend_id']))

        existing_adding.add((r['friend_id'],r['user_id']))

    target_log.close()

    print "start_id,end_id:",start_id,end_id

if __name__=='__main__':

    for i in range(config.day_to_update_stat+80,0+80,-1):
        my_date=time.time()-3600*24*i
        export_db_to_log(my_date)
