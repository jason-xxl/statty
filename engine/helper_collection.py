import _mysql
import helper_regex
import helper_mysql
import helper_file
import helper_math
import config
import sys

import time
from datetime import date


def put_named_collection(collection={},table_name='',oem_name='',category='',key='',sub_key='',date=''):
    if not date:
        sub_key,date=helper_regex.extract_complete_time_key(sub_key)
    if not collection:
        return 0
    root_dir=config.conn_stat_portal['named_collection_root_dir']
    file_name=helper_math.sha256(table_name+'|'+oem_name+'|'+category+'|'+key+'|'+str(sub_key)+'|'+str(date))
    element_str=repr(collection)
    helper_file.put_zipped_collection(file_name,element_str,step=8,root_dir=root_dir)

    return 1

def get_named_collection(table_name='',oem_name='',category='',key='',sub_key='',date=''):
    if not date:
        sub_key,date=helper_regex.extract_complete_time_key(sub_key)
    root_dir=config.conn_stat_portal['named_collection_root_dir']
    file_name=helper_math.sha256(table_name+'|'+oem_name+'|'+category+'|'+key+'|'+str(sub_key)+'|'+str(date))
    element_str=helper_file.get_zipped_collection(file_name,step=8,root_dir=root_dir)
    try:
        collection=eval(element_str)
        return collection
    except:
        print 'read named collection error: ',file_name
        return {}

if __name__ =='__main__':
    collection={}
    for i in range(1,500):
        collection[str(i)]=i
        if i % 10000==0:
            print i
        
    put_named_collection(collection=collection,table_name='raw_data_test',oem_name='test',category='test',key='test',sub_key='test',date='2012-03-20')
    print len(get_named_collection(table_name='raw_data_test',oem_name='test',category='test',key='test',sub_key='test',date='2012-03-20'))
    
    pass
