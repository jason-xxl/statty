import helper_sql_server
import helper_mysql
import helper_regex
import config

current_date=helper_regex.date_add(helper_regex.get_date_str_now(),-1)

def c(oem_name='',category='',key='',sub_key='',date='',table_name='raw_data',db_conn=None):
    if isinstance(date,str) or not date:
        return helper_mysql.get_raw_collection_from_key(oem_name=oem_name,category=category,key=key,sub_key=sub_key,date=date,table_name=table_name,db_conn=db_conn)
    return helper_mysql.get_raw_collection_from_key_date_range(oem_name=oem_name,category=category,key=key,sub_key=sub_key,begin_date=min(date),end_date=max(date),table_name=table_name,db_conn=db_conn)





if __name__ =='__main__':
    pass