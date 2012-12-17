import helper_sql_server
import helper_mysql
import helper_math
import config


def get_cached_sql_result_as_dict(sql,db_conn):
    if db_conn['db_type'] not in ('sql_server','mysql'):
        raise Error('db_type error')
    unique_key=helper_math.md5(','.join(sorted(db_conn.values()))+sql)

    cached_result=helper_mysql.get_dict_of_raw_collection_from_key(oem_name='',category='', key='',\
                                sub_key=unique_key,date='',table_name='raw_data_cache_sql_result',db_conn=None)

    if cached_result:
        print 'get from cache:',unique_key
        return cached_result
    
    if db_conn['db_type']=='sql_server':
        cached_result=helper_sql_server.fetch_dict(conn_config=db_conn,sql=sql)
    else:
        cached_result=helper_mysql.fetch_dict(db_conn=db_conn,sql=sql)

    helper_mysql.put_collection_with_value(collection=cached_result,oem_name='',category='',key='',sub_key=unique_key, \
                                table_name='raw_data_cache_sql_result',date='',created_on=None,db_conn=None)
    
    print 'push into cache:',unique_key
    return cached_result

if __name__=='__main__':

    print get_cached_sql_result_as_dict(sql='select 0,1',db_conn=config.conn_mt)

    pass
