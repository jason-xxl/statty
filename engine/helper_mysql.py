import _mysql
import helper_regex
import helper_math
import functools
import config
import sys
import helper_file
import collections
import time
from datetime import date
import math

db_force_new_connection=False
db_throw_all_exception=False

quick_insert=False
quick_insert_auto_cleared_data_space={}

print_log=True
low_priority_insert=True

key_text_dict_cache=set([])
sub_key_text_dict_cache=set([])

#db_host=config.stat_db_host
#db_user=config.stat_db_user
#db_pwd=config.stat_db_pwd
#db_name=config.stat_db_name
#db_name_crawl=config.crawl_db_name

db=_mysql.connect(host=config.conn_stat_portal['host'],user=config.conn_stat_portal['account'], \
                  passwd=config.conn_stat_portal['pwd'],db=config.conn_stat_portal['db'], \
                  port=(3306 if not config.conn_stat_portal.has_key('port') else int(config.conn_stat_portal['port'])))

db_pool={}

def escape_string(content):
    try:
        content=str(content)
        content=db.escape_string(content)
        return content
    except:
        print 'helper_mysql.py failed to escape: ',content
        content=repr(content).strip('ru').strip('\'')
        content=db.escape_string(content)
        return content

def get_db(conn_param,force_new=False):
    global db_force_new_connection
    if db_force_new_connection:
        force_new=True

    if not conn_param:
        conn_param=config.conn_stat_portal
        
    key='_'.join(conn_param.values())
    if db_pool.has_key(key) and not force_new:
        #debug=db_pool[key].dump_debug_info()
        #print 'ping '+key+': '+str(debug)
        return db_pool[key]

    if db_pool.has_key(key):
        try:
            db_pool[key].close()
        except:
            print 'close failed'
            pass

    db=_mysql.connect(host=conn_param['host'],user=conn_param['account'],
                      passwd=conn_param['pwd'],db=conn_param['db'],
                      port=(3306 if not conn_param.has_key('port') else int(conn_param['port'])))
    db.autocommit(True)

    # force all mysql sessions to be read uncommited for statistics
    db.query(r'SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
    db_pool[key]=db
    #debug=db_pool[key].dump_debug_info()
    #print 'ping '+key+': '+str(debug)
    return db


def _update_key_text(oem_name,category,key):
    global key_text_dict_cache
    full_key=oem_name+'|'+category+'|'+key
    if not helper_math.md5(key) in key_text_dict_cache:
        execute(r'''
            replace 
            into mozat_stat.key_text_dict
            (md5,text)
            values
            (unhex(md5('%s')),'%s')
        ''' % (full_key,full_key))
        key_text_dict_cache.add(helper_math.md5(full_key))

def _update_sub_key_text(sub_key):
    global sub_key_text_dict_cache
    if not helper_math.md5(sub_key) in sub_key_text_dict_cache:
        execute(r'''
            replace 
            into mozat_stat.sub_key_text_dict
            (md5,text)
            values
            (unhex(md5('%s')),'%s')
        ''' % (sub_key,sub_key))
        sub_key_text_dict_cache.add(helper_math.md5(sub_key))

    
def execute(sql,db_conn=None):
    db=get_db(db_conn)
    try:
        #print 'sql:' + sql
        db.query(sql)
        return db.affected_rows()
    except:
        print "helper_mysql.execute failed:", sys.exc_info()[0]
        print "error sql:",sql
        if db_throw_all_exception:
            exit()


def put_raw_data(oem_name='',category='',key='',sub_key='',value=0,table_name='raw_data',date='',created_on=None,db_conn=None):
    if table_name.startswith('raw_data'):
        _put_raw_data(oem_name=oem_name,category=category,key=key,sub_key=sub_key,value=value,table_name=table_name,date=date,created_on=created_on,db_conn=db_conn)
    elif table_name.startswith('data'):
        _put_compact_raw_data(oem_name=oem_name,category=category,key=key,sub_key=sub_key,value=value,table_name=table_name,date=date,created_on=created_on,db_conn=db_conn)
    

def _put_raw_data(oem_name='',category='',key='',sub_key='',value=0,table_name='raw_data',date='',created_on=None,db_conn=None):
    
    if not created_on:
        created_on=helper_regex.get_time_str_now()

    if not date:
        sub_key,date=helper_regex.extract_date_hour_key(sub_key)

    global quick_insert,quick_insert_auto_cleared_data_space,low_priority_insert

    if_apply_quick_insert=quick_insert and table_name and date and oem_name and category and key

    if if_apply_quick_insert:
        #auto clear data space, pass if any condition not provided
        combined_key='|'.join([table_name,oem_name,category,key,date])
        if not quick_insert_auto_cleared_data_space.has_key(combined_key):
            clear_raw_data_space(oem_name=oem_name,category=category,key=key,sub_key=None,date=date,table_name=table_name,db_conn=db_conn)
            quick_insert_auto_cleared_data_space[combined_key]=True

    db=get_db(db_conn)
    
    sub_key=escape_string(sub_key) if sub_key!='' else ''
    
    """
    sql1='delete from '+table_name+' where `oem_name`="'+oem_name+'" and `category`="'+category+'" and `key`="'+key+ \
                 '" and `sub_key`="'+sub_key+'" and `date`="'+date+'" limit 1'
    
    if not if_apply_quick_insert :
        print 'SQL1:'+sql1

    sql2='insert '+('low_priority ' if low_priority_insert else '')+'into '+table_name+'(`oem_name`,`category`,`key`,`sub_key`,`date`,`value`,`created_on`) values("' \
             +oem_name+'","'+category+'","'+key+'","'+sub_key+'","'+date+'","' \
             +str(value)+'",'+('null' if not created_on else '"'+created_on+'"')+')'
    
    if print_log:
        print 'SQL2:'+sql2
    """

    sql3='replace '+('low_priority ' if low_priority_insert else '')+'into '+table_name+'(`oem_name`,`category`,`key`,`sub_key`,`date`,`value`,`created_on`) values("' \
             +oem_name+'","'+category+'","'+key+'","'+sub_key+'","'+date+'","' \
             +str(value)+'",'+('null' if not created_on else '"'+created_on+'"')+')'
    
    if print_log:
        print 'SQL2:'+sql3


    try:
        """
        if not if_apply_quick_insert:
            db.query(sql1)
        db.query(sql2)
        """
        db.query(sql3)

    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql3
        
    
    #db.close()


def _put_compact_raw_data(oem_name='',category='',key='',sub_key='',value=0,table_name='data',date='',created_on=None,db_conn=None):
    
    if not created_on:
        created_on=helper_regex.get_time_str_now()

    if not table_name.startswith('data_int_') and not date:
        sub_key,date=helper_regex.extract_date_hour_key(sub_key)
    
    date=date.replace('-','').replace(' ','')

    global quick_insert,quick_insert_auto_cleared_data_space,low_priority_insert

    sub_key=escape_string(sub_key) if sub_key else ''

    _update_key_text(oem_name,category,key)

    if not table_name.startswith('data_int_'):
        _update_sub_key_text(sub_key)

    if_apply_quick_insert=quick_insert and table_name and date and oem_name and category and key

    if if_apply_quick_insert:
        #auto clear data space, pass if any condition not provided
        combined_key='|'.join([table_name,str(oem_name),str(category),str(key),str(date)])

        if not quick_insert_auto_cleared_data_space.has_key(combined_key):
            clear_raw_data_space(oem_name=oem_name,category=category,key=key,sub_key=None,date=date,table_name=table_name,db_conn=db_conn)
            quick_insert_auto_cleared_data_space[combined_key]=True

    db=get_db(db_conn)
    
    #sub_key=escape_string(sub_key)
    if not date:
        date='0'
    
    sql=r"replace %s into `%s` (`key`,`sub_key`,`date`,`value`,`created_on`) values (unhex(md5(concat('%s','|','%s','|','%s'))),%s,%s,%s,%s)" \
        % ('low_priority ' if low_priority_insert else '',table_name,oem_name,category,key \
            ,(sub_key if table_name.startswith('data_int_') else ("unhex(md5('%s'))" % (sub_key))) \
            ,date,str(value), 'null' if not created_on else "'"+created_on+"'")

    if print_log:
        print 'SQL:'+sql

    try:
        db.query(sql)
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql
        
    #db.close()


def clear_raw_data_space(oem_name='',category='',key='',sub_key='',date='',table_name='raw_data',db_conn=None):
    if table_name.startswith('raw_data'):
        _clear_raw_data_space(oem_name=oem_name,category=category,key=key,sub_key=sub_key,date=date,table_name=table_name,db_conn=db_conn)
    elif table_name.startswith('data'):
        _clear_compact_raw_data_space(oem_name=oem_name,category=category,key=key,sub_key=sub_key,date=date,table_name=table_name,db_conn=db_conn)

def _clear_raw_data_space(oem_name='',category='',key='',sub_key='',date='',table_name='raw_data',db_conn=None):

    ### shoulf be VERY CAREFUL to use this delete feature
    ### should force replace the condition to None if want to remove the condition

    db=get_db(db_conn)

    if sub_key:
        sub_key=escape_string(sub_key)

    if not any((oem_name,category,key,date)):
        print 'cancel _clear_raw_data_space'
        return -1
    
    sql1='delete from '+table_name+' where 1'
    
    if oem_name is not None:
        sql1+=' and `oem_name`="'+oem_name+'"'
    
    if category is not None:
        sql1+=' and `category`="'+category+'"'
    
    if key is not None:
        if len(key.strip('%'))<len(key):
            sql1+=' and `key` like "'+key+'"'
        else:
            sql1+=' and `key`="'+key+'"'
    
    if sub_key is not None:
        sql1+=' and `sub_key`="'+sub_key+'"'
    
    if date is not None:
        sql1+=' and `date`="'+date+'"'

    print 'SQL1:'+sql1

    try:
        db.query(sql1)
        pass
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql1


def _clear_compact_raw_data_space(oem_name='',category='',key='',sub_key='',date='',table_name='',db_conn=None):

    ### shoulf be VERY CAREFUL to use this delete feature
    ### should force replace the condition to None if want to remove the condition

    db=get_db(db_conn)

    if not(any((oem_name,category,key,date))):
        print 'cancel _clear_compact_raw_data_space'
        return -1
    
    date=date.replace('-','').replace(' ','')

    if sub_key:
        sub_key=escape_string(sub_key)

    sql=r"delete from `%s` where `key`=unhex(md5(concat('%s','|','%s','|','%s'))) and `date`=%s" \
            % (table_name,oem_name,category,key,date)

    if sub_key is not None:
        if table_name.startswith('data_int_'):
            sql+=r" and `sub_key`=unhex(md5('%s'))" % (sub_key,)
        elif table_name.startswith('data_'):
            sql+=r" and `sub_key`=%s" % (sub_key,)

    print 'SQL:'+sql

    try:
        db.query(sql)
        pass
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql


def put_raw_data_text(oem_name='',category='',key='',sub_key='',value='',table_name='raw_data',date='',db_conn=None,value_table='value_text_dict'):

    db=get_db(db_conn)
    db.query(r'select id from '+value_table+' where text= "'+escape_string(value)+'"')
    result_set = db.store_result()
    row = result_set.fetch_row(how=2)
    if row:
        row=row[0]
        if int(row[value_table+'.id'])>0:
            put_raw_data(oem_name=oem_name,category=category,key=key,sub_key=sub_key, \
                         value=row[value_table+'.id'], \
                         table_name=table_name,date=date)
            return
    try:
        db.query(r'insert into '+value_table+'(`text`) values("'+escape_string(value)+'")')
        last_insert_id=db.insert_id()
        put_raw_data(oem_name=oem_name,category=category,key=key,sub_key=sub_key,value=last_insert_id,table_name=table_name,date=date)
    except:
        db.query(r'select id from '+value_table+' where text= "'+escape_string(value)+'"')
        result_set = db.store_result()
        row = result_set.fetch_row(how=2)
        if row:
            row=row[0]
            if int(row[value_table+'.id'])>0:
                put_raw_data(oem_name=oem_name,category=category,key=key,sub_key=sub_key, \
                             value=row[value_table+'.id'], \
                             table_name=table_name,date=date)


def get_one_value_int(sql,db_conn=None):

    db=get_db(db_conn)
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()
    
    try:    
        db.query(sql)
        result_set = db.store_result()
        row = result_set.fetch_row(how=0)
        
        log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
        if not row:
            return -1
        return int(row[0][0])
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql
       
        log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1

def get_one_value_string(sql,db_conn=None):

    db=get_db(db_conn)
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()
    
    try:    
        db.query(sql)
        result_set = db.store_result()
        row = result_set.fetch_row(how=0)
        
        log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
        if not row:
            return -1
        return str(row[0][0])
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql
        
        log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1
        
    

def get_raw_data(oem_name='',category='',key='',sub_key='',default_value=-1,table_name='raw_data',date='',db_conn=None):

    value=default_value
    
    db=get_db(db_conn)
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    if table_name.startswith('raw_data'):
        sql=r"select `value` from `%s` where `oem_name`='%s' and `category`='%s' and `key`='%s' and `sub_key`='%s' and `date`='%s' /*order by `id` desc*/ limit 1" \
             %(table_name,oem_name,category,key,sub_key,date)

    elif table_name.startswith('data_int_'):
        date=date.replace('-','').replace(' ','')
        sql=r"select `value` from `%s` where `key`=unhex(md5(concat('%s','|','%s','|','%s'))) and `sub_key`=%s%s" \
             %(table_name,oem_name,category,key,sub_key," and date="+date if date != '' else '')

    else:
        date=date.replace('-','').replace(' ','')
        sql=r"select `value` from `%s` where `key`=unhex(md5(concat('%s','|','%s','|','%s'))) and `sub_key`=unhex(md5('%s'))%s" \
             %(table_name,oem_name,category,key,sub_key," and date="+date if date != '' else '')
        
    print 'sql_read:',sql
    db.query(sql)
    result_set = db.store_result()
    row = result_set.fetch_row(how=0)
    log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
    if row:
        value=int(row[0][0])

    return value
    try:    
        pass
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql
       
        log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1



def fetch_dict(sql,key_name=0,value_name=1,db_conn=None):

    db=get_db(db_conn)
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    db.query(sql)
    result_set = db.store_result()
    rows = result_set.fetch_row(how=0,maxrows=0)
    log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
    if not rows:
        return {}

    #print str(row)
    ret={}
    for i in rows:
        ret[str(i[key_name])]=str(i[value_name])
    
    try:
        return ret
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql

        log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1


def fetch_dict_map_to_collection(collection_set,sql_template,db_conn=None,step=1000,use_string_id=False):
    result={}
    ids=[i for i in collection_set]
    for i in range(0,100000):
        if use_string_id:
            sql_ids="'"+"','".join(ids[i*step:(i+1)*step])+"'"
        else:
            sql_ids=','.join(ids[i*step:(i+1)*step])
        if not sql_ids or sql_ids=="''":
            break
        sql=sql_template % (sql_ids,)
        print sql
        temp_dict=fetch_dict(sql,db_conn=db_conn)
        result.update(temp_dict)

    print 'fetch_dict_map_to_collection: ',len(result),len(result)
    return result



def fetch_set(sql,db_conn=None):

    db=get_db(db_conn)
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    db.query(sql)
    result_set = db.store_result()
    rows = result_set.fetch_row(how=0,maxrows=0)
    log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
    if not rows:
        return set([])
    #print str(row)
    ret=set([])
    for i in rows:
        ret.add(i[0])

    
    try:
        return ret
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql

        log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1


def fetch_row(sql,db_conn=None):
    
    db=get_db(db_conn)
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()
    try:
        db.query(sql)
        result_set = db.store_result()
        row = result_set.fetch_row(how=1,maxrows=1)
        log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)

        if not row:
            return {}

        for i in row:
            return i

    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql

        
        log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1


def fetch_rows(sql,db_conn=None):
    
    db=get_db(db_conn)
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()
    db.query(sql)
    result_set = db.store_result()
    log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)

    ret=[]
    while True:
        row = result_set.fetch_row(how=1,maxrows=1)
        if not row:
            break
        ret.append(row[0])
    
    return ret
    try:
        pass
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql

        
        log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1



def get_raw_collection_by_id(collection_id):
    if not collection_id:
        return set([])

    if config.collection_cache_enabled and config.collection_cache.has_key(collection_id):
        print 'fetch from collection cache:',collection_id
        return config.collection_cache[collection_id]
    
    db=get_db(None)
    
    sql='select `element_string` from `collection` where `id`="%s"' \
        %(str(collection_id),)

    print 'get collection: collecton id '+str(collection_id)

    db.query(sql)
    result_set = db.store_result()
    rows = result_set.fetch_row(how=1,maxrows=0)
    if not rows:
        print 'no collection record:',collection_id
        return set([])
    
    for i in rows:
        #print i['element_string']
        elements_string=(str(i['element_string'])).strip()
        #print '1'+elements_string
        if helper_regex.extract(elements_string,config.collection_filename_pattern):
            elements_string=helper_file.get_zipped_collection(collection_id=collection_id)
            #print '2'+elements_string
            if not elements_string:
                print 'empty file collection: ',collection_id
                #exit()

        if elements_string.startswith('{') and elements_string.endswith('}'):
            elements=set([])
            exec('elements='+elements_string)
            elements_set=set(elements.keys())
        else:
            elements_set=set(elements_string.split(','))

        if config.collection_cache_enabled and elements_set:
            print 'put to collection cache:',collection_id
            config.collection_cache[collection_id]=elements_set

        break

    print 'elements_set size: ',len(elements_set),' id:',collection_id
    if not len(elements_set):
        #exit()
        pass

    return elements_set

def get_dict_of_raw_collection_by_id(collection_id):

    if not collection_id:
        return {}

    # todo:cache
    """
    if config.collection_cache_enabled and config.collection_cache.has_key(collection_id):
        print 'fetch from collection cache:',collection_id
        return config.collection_cache[collection_id]
    """

    db=get_db(None)
    
    sql='select `element_string` from `collection` where `id`="%s"' \
        %(str(collection_id),)

    print 'get collection: collecton id '+str(collection_id)

    db.query(sql)
    result_set = db.store_result()
    rows = result_set.fetch_row(how=1,maxrows=0)
    if not rows:
        print 'no collection record:',collection_id
        return {}
    
    for i in rows:
        #print i['element_string']
        elements_string=(str(i['element_string'])).strip()
        #print '1'+elements_string
        if helper_regex.extract(elements_string,config.collection_filename_pattern):
            elements_string=helper_file.get_zipped_collection(collection_id=collection_id)
            #print '2'+elements_string
            if not elements_string:
                print 'empty file collection: ',collection_id
                return {}
                #exit()

        if elements_string.startswith('{') and elements_string.endswith('}'):
            elements={}
            exec('elements='+elements_string)
            #print 'elements='+elements_string
            #print elements
            #exit()
        else:
            elements={}
            for k in elements_string.split(','):
                elements[k]=None

        """
        if config.collection_cache_enabled and elements_set:
            print 'put to collection cache:',collection_id
            config.collection_cache[collection_id]=elements_set
        """
        break

    print 'elements_dict size: ',len(elements),' id:',collection_id
    if not len(elements):
        #exit()
        pass

    return elements


def get_raw_collection_from_file(full_path,separator=','):
    if not full_path:
        return set([])
    
    elements_string=helper_file.get_content_from_file(full_path)
    elements_set=set(elements_string.split(separator))

    return elements_set
    try:
        pass
    except:
        return set([])


def remove_raw_collection_by_id(collection_id):
    if not collection_id:
        return 0

    if config.collection_cache_enabled and config.collection_cache.has_key(collection_id):
        print 'remove from collection cache:',collection_id
        del config.collection_cache[collection_id]

    db=get_db(None)
    
    sql='select `element_string` from `collection` where `id`="%s"' \
        %(str(collection_id),)

    print 'remove collection: collecton id '+str(collection_id)

    db.query(sql)
    result_set = db.store_result()
    rows = result_set.fetch_row(how=1,maxrows=0)
    if not rows:
        return 0
    
    for i in rows:
        #print i['element_string']
        elements_string=str(i['element_string'])
        if helper_regex.extract(elements_string,config.collection_filename_pattern):
            helper_file.delete_zipped_collection(collection_id=collection_id)

        sql='delete from `collection` where `id`="%s" limit 1' \
            %(str(collection_id),)
        db.query(sql)
        break

    return collection_id

def get_raw_collection_size_by_id(collection_id):
    if not collection_id:
        return 0

    if config.collection_cache_enabled and config.collection_cache.has_key(collection_id):
        print 'fetch size from collection cache:',collection_id
        return len(config.collection_cache[collection_id])

    db=get_db(None)
    
    sql='select `element_count` from `collection` where `id`="%s"' \
        %(str(collection_id),)

    print 'get collection size: collecton id '+str(collection_id)

    db.query(sql)
    result_set = db.store_result()
    rows = result_set.fetch_row(how=1,maxrows=0)
    if not rows:
        return 0

    ret=0
    
    for i in rows:
        ret=int(i['element_count'])
        break

    return ret



def get_raw_collection_from_key_date_range(oem_name='',category='',key='',sub_key='',begin_date='',end_date='',table_name='raw_data',db_conn=None):

    result=set([])
    
    for i in range(0,10000):
        date_temp=helper_regex.date_add(begin_date,i)
        if date_temp>end_date:
            break
        result|=get_raw_collection_from_key(oem_name=oem_name,category=category,key=key,sub_key=sub_key,date=date_temp,table_name=table_name,db_conn=db_conn)
    return result   

def get_dict_of_raw_collection_from_key_date_range(oem_name='',category='',key='',sub_key='',begin_date='',end_date='',table_name='raw_data',db_conn=None):

    result={}
    
    for i in range(0,10000):
        date_temp=helper_regex.date_add(begin_date,i)
        if date_temp>end_date:
            break
        temp=get_dict_of_raw_collection_from_key(oem_name=oem_name,category=category,key=key,sub_key=sub_key,date=date_temp,table_name=table_name,db_conn=db_conn)
        for k,v in temp.iteritems():
            result.setdefault(k,0)
            result[k]+=v
    return result   

def get_dict_of_raw_collection_from_key(oem_name='',category='',key='',sub_key='',date='',table_name='raw_data',db_conn=None):

    print 'get_raw_collection_from_key:',oem_name,category,key,sub_key,date,table_name,db_conn
    key=key.replace('_collection_id','')+'_collection_id'

    collection_id=get_raw_data(oem_name=oem_name,category=category,key=key,sub_key=sub_key,default_value=-1, \
                                table_name=table_name,date=date,db_conn=db_conn)
    
    return get_dict_of_raw_collection_by_id(collection_id)

def get_raw_collection_from_key(oem_name='',category='',key='',sub_key='',date='',table_name='raw_data',db_conn=None):

    print 'get_raw_collection_from_key:',oem_name,category,key,sub_key,date,table_name,db_conn
    key=key.replace('_collection_id','')+'_collection_id'

    collection_id=get_raw_data(oem_name=oem_name,category=category,key=key,sub_key=sub_key,default_value=-1, \
                                table_name=table_name,date=date,db_conn=db_conn)
    
    return get_raw_collection_by_id(collection_id)


def get_merged_dict_collection_from_compact_raw_data_with_sub_key_pattern(oem_name='',category='',key='',sub_key_pattern='',date='',table_name='', db_conn=None):
    '''
    For COMPACT raw data. not as powerful as get_merged_dict_collection_from_raw_data_with_sub_key_pattern
    table sub_key_text_dict must exists
    
    todo: make it more powerful, support customized sub_key filter
    '''
    key=key.replace('_collection_id','')+'_collection_id'

    # step 1: get collection ids
    modified_date = date.replace('-','') # remove dash -
    sql = r'''
    SELECT a.value FROM %s as a, sub_key_text_dict as b 
    where `key`=unhex(md5('%s|%s|%s')) 
    and date=%s 
    and a.sub_key = b.md5 
    and b.text like '%s'
    ''' % (table_name, oem_name, category, key, modified_date, sub_key_pattern)
    collection_ids = fetch_set(sql,db_conn=db_conn)

    # step2: collection ids -> get monete ids and visits 
    #monet_id_visit_dict = collections.defaultdict(int) # default value is 0 # could cause some error
    monet_id_visit_dict = {}

    for collection_id in collection_ids:
        collection_dict = get_dict_of_raw_collection_by_id(collection_id)
        for k,v in collection_dict.iteritems():
            monet_id_visit_dict.setdefault(k,0)
            monet_id_visit_dict[k] += v # added to it
#    print len(monet_id_visit_dict) # uv
#    print sum(monet_id_visit_dict.values()) # pv
    return monet_id_visit_dict


def get_merged_dict_collection_from_raw_data_with_sub_key_pattern(oem_name='',category='',key='',sub_key_pattern='',date='',begin_date='',end_date='',table_name='raw_data',db_conn=None):
    
    if not callable(sub_key_pattern): # filter function
        sub_key_pattern=sub_key_pattern.replace('[ignorecase]','')+'[ignorecase]'

    if date:
        begin_date=date
        end_date=date
    
    result=dict()
    
    for i in range(0,10000):
        date_temp=helper_regex.date_add(begin_date,i)
        if date_temp>end_date:
            break

        sql=r'''

        select sub_key,`value`
        from %s
        where oem_name='%s'
        and category='%s'
        and `key`='%s'
        and date='%s'

        ''' % (table_name,oem_name,category,key,date_temp)

        #print sql

        collection_ids=fetch_dict(sql,db_conn=db_conn)
        #print collection_ids
        for sub_key,id in collection_ids.iteritems():
            if callable(sub_key_pattern): # filter function
                if sub_key_pattern(sub_key):
                    print 'match sub_key:',sub_key
                    sub_key_result = get_dict_of_raw_collection_by_id(id)
                    for k, v in sub_key_result.iteritems():
                        if k in result:
                            result[k] += v # must be numeric vlaue
                        else:
                            result[k] = v    
            else: # string 
                if helper_regex.extract(sub_key,sub_key_pattern):
                    print 'match sub_key:',sub_key
                    sub_key_result = get_dict_of_raw_collection_by_id(id)
                    for k, v in sub_key_result.iteritems():
                        if k in result:
                            result[k] += v
                        else:
                            result[k] = v
        if not result:
            print 'no matched:',sub_key_pattern
    return result   


def get_merged_collection_from_raw_data_with_sub_key_pattern(oem_name='',category='',key='',sub_key_pattern='',date='',begin_date='',end_date='',table_name='raw_data',db_conn=None):
    
    if not callable(sub_key_pattern): # filter function
        sub_key_pattern=sub_key_pattern.replace('[ignorecase]','')+'[ignorecase]'

    if date:
        begin_date=date
        end_date=date
    
    result=set([])
    
    for i in range(0,10000):
        date_temp=helper_regex.date_add(begin_date,i)
        if date_temp>end_date:
            break

        sql=r'''

        select sub_key,`value`
        from %s
        where oem_name='%s'
        and category='%s'
        and `key`='%s'
        and date='%s'

        ''' % (table_name,oem_name,category,key,date_temp)

        #print sql

        collection_ids=fetch_dict(sql,db_conn=db_conn)
        #print collection_ids
        for sub_key,id in collection_ids.iteritems():
            if callable(sub_key_pattern): # filter function
                if sub_key_pattern(sub_key):
                    print 'match sub_key:',sub_key
                    result|=get_raw_collection_by_id(id)    
            else: # string
                if helper_regex.extract(sub_key,sub_key_pattern):
                    print 'match sub_key:',sub_key
                    result|=get_raw_collection_by_id(id)
        if not result:
            print 'no matched:',sub_key_pattern
    return result   


def get_raw_collection(collection):
    if isinstance(collection,dict) and collection.has_key('oem_name'):
        return get_raw_collection_from_key(
            oem_name=collection['oem_name'] if collection.has_key('oem_name') else '',
            category=collection['category'] if collection.has_key('category') else '',
            key=collection['key'] if collection.has_key('key') else '',
            sub_key=collection['sub_key'] if collection.has_key('sub_key') else '',
            date=collection['date'] if collection.has_key('date') else '',
            table_name=collection['table_name'] if collection.has_key('table_name') else 'raw_data')
    if isinstance(collection,dict):
        return set(collection.keys())
    if isinstance(collection,list):
        return set(collection)
    if isinstance(collection,set):
        return collection
    if isinstance(collection, str):
        elements=collection.split(',')
        return set(elements)

    return None


def log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=''):
    
    script_file_name=script_file_name.replace("\\",r"\\\\").replace(r"'",r"\'")
    sql=sql.replace("\\",r"\\\\").replace(r"'",r"\'")
    error_msg=str(error_msg).replace("\\",r"\\\\").replace(r"'",r"\'")
    current_time=helper_regex.get_time_clock_time()
    
    if 'raw_data' in sql:
        return

    sql=r"""
    
    INSERT INTO `mozat_stat`.`log_sql_execution_time` (
        `script_file_name`,
        `sql`,
        `started_on`,
        `execution_time_in_sec`,
        `error_msg`
    ) VALUES ('%s','%s','%s','%s','%s')
    
    """ % (script_file_name,sql,start_time_str,current_time-start_time,error_msg)

    #print sql

    try:
        db=get_db(None)
        db.query(sql)
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql
        return -1

    return 0    

def put_raw_collection_with_value(collection,force_create=False):
    if not collection:
        return 0
    
    if isinstance(collection, dict):
        for i,j in collection.iteritems():
            if j is None:
                element=collection.keys()
                element.sort()
                element_str=','.join([escape_string(i).strip() for i in element])
                element_count=len(collection)
            else:
                collection=dict((escape_string(i).strip(),int(j)) for i,j in collection.iteritems())
                #element=collection.keys()
                #element.sort()
                element_str=repr(collection)
                #print element_str
                element_count=len(collection)
            break
        

    elif isinstance(collection, list):
        collection.sort()
        collection=[escape_string(i).strip() for i in collection]
        element_str=','.join(collection)
        element_count=len(collection)

    elif isinstance(collection, set):
        element=list([escape_string(i).strip() for i in collection])
        element.sort()
        element_str=','.join(element)
        element_count=len(collection)

    elif isinstance(collection, str):
        element=collection.split(',')
        element.sort()
        element_str=','.join(element)
        element_count=len(element)
        element_str=element_str.replace(r'\'','').replace(r'"','').strip(',')

    else:
        import helper_mail

        title='stat python exception: '+helper_regex.get_script_file_name()
        content='collection str tye error:',str(type(collection))

        #print content

        helper_mail.send_mail(title=title,content_html=content)

        #print 'collection file priginal size: ',len(element_str)

    element_md5=helper_math.md5(element_str)

    """
    if not helper_regex.extract(element_str,r'(^$|^[^,]+$|^(?:[^,]+,)*?(?:[^,]+)$)'):
        print 'element_str format error:'+element_str
        return -1
    """

    db=get_db(None)

    existing_id=0

    if not force_create:
        existing_id=get_one_value_int(r'''
            select id from `collection` 
            where `element_string_md5`='%s'
            and `element_count`='%s'
            order by id desc
            limit 1
        ''' % (element_md5,element_count))
    else:
        print "force create collection"

    if not force_create and existing_id>0:
        print 'insert collection: element count '+str(element_count)+' (reused collection '+str(existing_id)+')'
        return existing_id
    
    sql='insert into `collection`(`element_count`,`element_string_md5`,`element_string`) values("%s","%s","")' \
        %(str(element_count),element_md5)

    last_inserted_id=0

    #print sql
    db.query(sql)
    last_inserted_id=db.insert_id()

    if config.collection_cache_enabled:
        print 'put to collection cache:',last_inserted_id
        config.collection_cache[last_inserted_id]=set(','.split(element_str))

    file_path=helper_file.put_zipped_collection(last_inserted_id,element_str)
    sql='update `collection` set `element_string`="%s" where `id`="%s" limit 1' \
        %(escape_string(file_path),last_inserted_id)
    db.query(sql)
    print 'insert collection: element count '+str(element_count)+' '+str(last_inserted_id)

    try:
        pass
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql
        return -1

    return last_inserted_id

def put_raw_collection(collection,force_create=False):
    if not collection:
        return 0
    
    if isinstance(collection, dict):
        for i,j in collection.iteritems():
            if j is None:
                element=collection.keys()
                element.sort()
                element_str=','.join([escape_string(i).strip() for i in element])
                element_count=len(collection)
            else:
                collection=dict((escape_string(i).strip(),int(j)) for i,j in collection.iteritems())
                #element=collection.keys()
                #element.sort()
                element_str=repr(collection)
                #print element_str
                element_count=len(collection)
            break
        

    elif isinstance(collection, list):
        collection.sort()
        collection=[escape_string(i).strip() for i in collection]
        element_str=','.join(collection)
        element_count=len(collection)

    elif isinstance(collection, set):
        element=list([escape_string(i).strip() for i in collection])
        element.sort()
        element_str=','.join(element)
        element_count=len(collection)

    elif isinstance(collection, str):
        element=collection.split(',')
        element.sort()
        element_str=','.join(element)
        element_count=len(element)
        element_str=element_str.replace(r'\'','').replace(r'"','').strip(',')

    else:
        import helper_mail

        title='stat python exception: '+helper_regex.get_script_file_name()
        content='collection str tye error:',str(type(collection))

        #print content

        helper_mail.send_mail(title=title,content_html=content)

        #print 'collection file priginal size: ',len(element_str)

    element_md5=helper_math.md5(element_str)

    """
    if not helper_regex.extract(element_str,r'(^$|^[^,]+$|^(?:[^,]+,)*?(?:[^,]+)$)'):
        print 'element_str format error:'+element_str
        return -1
    """

    db=get_db(None)

    existing_id=0

    if not force_create:
        existing_id=get_one_value_int(r'''
            select id from `collection` 
            where `element_string_md5`='%s'
            and `element_count`='%s'
            order by id desc
            limit 1
        ''' % (element_md5,element_count))
    else:
        print "force create collection"

    if not force_create and existing_id>0:
        print 'insert collection: element count '+str(element_count)+' (reused collection '+str(existing_id)+')'
        return existing_id
    
    sql='insert into `collection`(`element_count`,`element_string_md5`) values("%s","%s")' \
        %(str(element_count),element_md5)

    last_inserted_id=0

    try:
        #print sql
        db.query(sql)
        last_inserted_id=db.insert_id()

        if config.collection_cache_enabled:
            print 'put to collection cache:',last_inserted_id
            config.collection_cache[last_inserted_id]=set(','.split(element_str))

        file_path=helper_file.put_zipped_collection(last_inserted_id,element_str)
        sql='update `collection` set `element_string`="%s" where `id`="%s" limit 1' \
            %(escape_string(file_path),last_inserted_id)
        db.query(sql)
        print 'insert collection: element count '+str(element_count)+' '+str(last_inserted_id)
    except:
        print "Unexpected error:", sys.exc_info()[0]
        print 'Mysql error happened !!!'
        print 'Error SQL:'+sql
        return -1

    return last_inserted_id



def put_collection(collection='',oem_name='',category='',key='',sub_key='',table_name='raw_data',date='',created_on=None,db_conn=None):
    #print "put_collection:"
    #print collection
    #print len(collection)-len(collection.replace(',',''))

    collection_id=put_raw_collection(collection)
    print collection_id

    if collection_id==0:
        print 'put_collection: put_raw_collection error'
        return 0

    put_raw_data(oem_name=oem_name,category=category,key=key+'_collection_id',sub_key=sub_key, \
    value=collection_id, \
    table_name=table_name,date=date,created_on=created_on,db_conn=db_conn)

    #print len(get_raw_collection(collection))

    put_raw_data(oem_name=oem_name,category=category,key=key+'_element_count',sub_key=sub_key, \
    value=len(get_raw_collection(collection)), \
    table_name=table_name,date=date,created_on=created_on,db_conn=db_conn)

    return collection_id


def put_collection_with_value(collection='',oem_name='',category='',key='',sub_key='',table_name='raw_data',date='',created_on=None,db_conn=None):
    #print "put_collection:"
    #print collection
    #print len(collection)-len(collection.replace(',',''))

    collection_id=put_raw_collection_with_value(collection)
    print collection_id

    if collection_id==0:
        print 'put_collection: put_raw_collection error'
        return 0

    put_raw_data(oem_name=oem_name,category=category,key=key+'_collection_id',sub_key=sub_key, \
    value=collection_id, \
    table_name=table_name,date=date,created_on=created_on,db_conn=db_conn)

    #print len(get_raw_collection(collection))

    put_raw_data(oem_name=oem_name,category=category,key=key+'_element_count',sub_key=sub_key, \
    value=len(get_raw_collection(collection)), \
    table_name=table_name,date=date,created_on=created_on,db_conn=db_conn)

    return collection_id


def replace_collection(collection='',oem_name='',category='',key='',sub_key='',table_name='raw_data',date='',created_on=None,db_conn=None):
    
    #this means the raw collection will be delete first if exists

    db=get_db(None)

    old_collection_id=get_raw_data(oem_name=oem_name,category=category,key=key+'_collection_id',sub_key=sub_key, \
                                   table_name=table_name,date=date,db_conn=db_conn)
    if old_collection_id:
        print 'delete old collection: '+str(old_collection_id)
        remove_raw_collection_by_id(old_collection_id)

    collection_id=put_raw_collection(collection)
    print collection_id

    if collection_id==0:
        print 'put_collection1: put_raw_collection error'
        return 0

    put_raw_data(oem_name=oem_name,category=category,key=key+'_collection_id',sub_key=sub_key, \
    value=collection_id, \
    table_name=table_name,date=date,created_on=created_on,db_conn=db_conn)

    #print len(get_raw_collection(collection))

    put_raw_data(oem_name=oem_name,category=category,key=key+'_element_count',sub_key=sub_key, \
    value=len(get_raw_collection(collection)), \
    table_name=table_name,date=date,created_on=created_on,db_conn=db_conn)

    return collection_id


def get_dict_value(line,field_def='',the_dict={},exception_function=None):
    if callable(field_def):
        key=field_def(line)
    else:
        key=helper_regex.extract(line,field_def)
    ret=the_dict[key] if the_dict.has_key(key) else ""

    if not ret and callable(exception_function):
        ret=exception_function(line)
    return ret


def get_translater_for_fetch_dict(sql,field_def,key_name=0,value_name=1,db_conn=None,exception_function=None):

    the_dict=fetch_dict(sql,key_name=0,value_name=1,db_conn=db_conn)
    if not the_dict:
        the_dict={}

    return functools.partial(_get_dict_value, field_def=field_def,the_dict=the_dict,exception_function=exception_function)


def _max_text_column_size_test(size=1024):

    s='a'*size

    db=get_db(None)

    sql1='delete from collection where `id`=-1'
    db.query(sql1)

    sql1='insert into collection (`id`,`element_string`) values("-1","%s")' % (s,)
    db.query(sql1)


    sql2='select `element_string` from collection where `id`=-1'
    #print 'SQL2:'+sql2

    return len(get_one_value_string(sql2))

    
    #db.close()



def log_current_timestamp(script_name,time_name):
    import time,helper_ip,os
    put_raw_data(oem_name=script_name,category=helper_ip.get_current_server_ip(),key=str(os.getpid()),sub_key=time_name,value=int(time.time()),table_name='raw_data_monitor')




def guess_pk_id_by_time(table_name,target_time,pk_column_name='id', \
                        sequence_time_column_name='creation_time',db_conn=None):
    ###
    ### this function "guess" the minimum pk id which >= target_time
    ### mostly for a very common use of "where creation_time>=t1 and creation_time<t2" 
    ### id there's positive corelation between creation_time and pk id
    ###

    max_id=get_one_value_int(r'''
    select max(%s) from %s
    ''' % (pk_column_name,table_name),db_conn)
    
    min_id=get_one_value_int(r'''
    select min(%s) from %s
    ''' % (pk_column_name,table_name),db_conn)
    
    max_time=get_one_value_string(r'''
    select %s from %s order by %s desc limit 1
    ''' % (sequence_time_column_name,table_name,pk_column_name),db_conn)

    min_time=get_one_value_string(r'''
    select %s from %s order by %s asc limit 1
    ''' % (sequence_time_column_name,table_name,pk_column_name),db_conn)

    #print max_id,min_id,max_time,min_time

    if target_time>max_time:
        raise Exception('guess_pk_id_by_time: target_time>max_time')

    if target_time<min_time:
        raise Exception('guess_pk_id_by_time: target_time<min_time')

    found_target_time=False
    final_id=None

    last_max_id=-1
    last_min_id=-2

    #while True:

    import math

    for i in range(0,int(math.ceil(math.log(max_id,2)))*2): 

        imagined_id=int(math.floor(min_id+(max_id-min_id)*1.0/2))
        
        row=fetch_row(r'''
        
        select %s,%s
        from %s
        where %s<=%s
        order by %s desc 
        limit 1

        ''' % (pk_column_name,sequence_time_column_name,table_name,pk_column_name,imagined_id,pk_column_name),db_conn)

        #print target_time,row['created_on'],max_id,min_id,imagined_id,row['id']

        if row['created_on']>target_time:
            max_id=int(row['id'])
        elif row['created_on']<target_time:
            if not found_target_time:
                min_id=int(row['id'])
            else:
                final_id=get_one_value_int(r'''

                select min(%s)
                from %s
                where %s>=%s and %s<=%s
                and %s='%s'

                ''' % (pk_column_name,table_name,pk_column_name,row['id'],pk_column_name,max_id, \
                sequence_time_column_name,target_time),db_conn)
                break
        elif row['created_on']==target_time:
            found_target_time=True
            max_id=int(row['id'])
        
        if last_max_id==max_id and last_min_id==min_id:
            final_id=get_one_value_int(r'''

            select min(%s)
            from %s
            where %s>=%s and %s<=%s
            and %s>='%s'

            ''' % (pk_column_name,table_name,pk_column_name,min_id,pk_column_name,max_id, \
            sequence_time_column_name,target_time),db_conn)
            break
        else:
            last_max_id=max_id
            last_min_id=min_id
        
    if final_id is None:
        raise Exception('guess_pk_id_by_time: final_id is None')

    ### test

    final_time=get_one_value_string(r'''

    select %s as time from %s where %s=%s

    ''' % (sequence_time_column_name,table_name,pk_column_name,final_id),db_conn)

    print 'guess_pk_id_by_time result:',table_name,final_id,final_time,target_time

    return final_id


if __name__ =='__main__':
    print guess_pk_id_by_time(table_name='mozat_stat.production_copy_friendship_stc', \
                              target_time='2012-05-05 07:19:56', \
                              pk_column_name='id', \
                              sequence_time_column_name='created_on',db_conn=None)

    exit()
    print get_raw_collection_by_id(put_raw_collection({'dd':1,'p':2,'a':3,444:3}))
    #print get_dict_of_raw_collection_by_id(put_raw_collection({'dd':1,'p':2,'a':3}))
    #print get_raw_collection_by_id(put_raw_collection(set(['dd',2,3])))
    exit()
    print get_raw_collection_by_id(put_raw_collection({'dd':None,'p':None,'a':None}))
    put_raw_data(oem_name='test',category='test',key='test',sub_key=1123,value=1,table_name='data_int_user_info_globe',date='2011-10-21',created_on=None,db_conn=config._conn_stat_portal_158_2)
    exit()
    
    print get_raw_data(oem_name='test',category='test',key='test',sub_key=1123,default_value=-1,table_name='data_int_user_info_globe',date='2011-10-21',db_conn=None)
    exit()
    t={}
    for i in range(0,1000):
        t['testtest'+str(i)]=200
        if i%1000000==0:
            print i
    print put_raw_collection(t)
    get_raw_collection_by_id(put_raw_collection({'dd':1,'p':1,'a':1}))
    exit()

    quick_insert=True
    put_raw_data(oem_name='test',category='test',key='test',sub_key=1123,value=1,table_name='data_int_user_info_stc',date='2011-10-20',created_on=None,db_conn=None)
    

    exit()
    print get_raw_collection_by_id(put_raw_collection({'d':None,'p':None,'a':None}))


    print repr(u'fdsfr').strip('ru').strip('\'')
    exit()

    exit()

    start_time=helper_regex.get_time_clock_time()
    for i in range(200):
        for j in range(200):
            put_raw_data(oem_name=i,category=j,key=i,sub_key=j,value=0,table_name='data',date='2011-10-20',created_on=None,db_conn=None)
    end_time=helper_regex.get_time_clock_time()
    print end_time-start_time
        

    exit()
    quick_insert=True
    low_priority_insert=False
    put_raw_data(oem_name='mozat_stat',category='test',key='test',sub_key='test_2010-12-20_test',created_on=None,value=100,table_name='raw_data_test')
    put_raw_data(oem_name='mozat_stat',category='test',key='test',sub_key='test2_2010-12-20_test',created_on=None,value=100,table_name='raw_data_test')
    put_raw_data(oem_name='mozat_stat',category='test',key='test2',sub_key='test3_2010-12-20_test',created_on=None,value=100,table_name='raw_data_test')
    
    exit()
    print get_db(None).escape_string('fdsf"fdsf')

    print get_raw_collection_from_file(r'\\192.168.0.158\WebStatShare\BUZZDifference_mozat.txt')
    
    put_raw_data_text(oem_name='mozat_stat',category='test',key='test_txt',sub_key='test_2010-12-20_test',value="good morning")
    exit()
    replace_collection(collection=get_raw_collection_by_id(970705),oem_name='Vodafone',category='login',key='user_last_login_1_day_unique' \
                        ,sub_key='',table_name='raw_data',date='2011-04-20')

    #
    """
    print len(get_raw_collection_from_key(oem_name='Vodafone',category='sub',key='daily_in_sub_subscriber_collection_id', \
            sub_key='',date='2011-04-20',table_name='raw_data'))
    for i in range(1,10):
        id=replace_collection(collection=range(0,i),oem_name='Test',category='Test',key='Test_replace_collection',sub_key='Test',table_name='raw_data',date='2011-04-21')
        print get_raw_collection_by_id(id)
    """
    #print _max_text_column_size_test(1000000)
    print get_one_value_int('select 1')

    print fetch_row('select @@tx_isolation')
    print fetch_row(r'''
            select
            distinct sub_key,null as a
            from raw_data_user_info
            where
            oem_name='%s'
            and category='%s'
            limit 100
        ''' % ('STC','user_generated_content')
    )
    """

    print get_raw_collection_size_by_id(put_raw_collection({'d':None,'p':None}))


    print get_raw_data(oem_name='STC',category='sub',key='sub_user_total',date='2010-12-20')

    print log_execution_info("test.py'","test_sql'","2010-12-20 12:00:01",error_msg='Test Msg\'',start_time_str=start_time_str)
    print helper_regex.get_script_file_name()
    print 'raw_data' in 'dRaw_datad'

    print get_raw_collection_by_id(put_raw_collection({'d':None,'p':None,'k':None,'t':None}))
    print get_raw_collection_by_id(put_raw_collection(set(['d','p','k','t'])))
    print get_raw_collection_by_id(put_raw_collection(['d','p','k','t']))

    """

"""

CREATE TABLE IF NOT EXISTS `mozat_stat`.`raw_data` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `oem_name` varchar(45) NOT NULL DEFAULT '',
  `category` varchar(45) NOT NULL DEFAULT '',
  `key` varchar(45) NOT NULL DEFAULT '',
  `sub_key` varchar(45) NOT NULL DEFAULT '',
  `value` int(10) unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `name` (`oem_name`,`category`,`key`,`sub_key`) USING BTREE,
  KEY `created_on` (`created_on`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `data` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


CREATE TABLE IF NOT EXISTS `data_int_user_info_stc` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


CREATE TABLE IF NOT EXISTS `sub_key_text_dict` (
  `md5` binary(16) NOT NULL,
  `text` varchar(500) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


CREATE TABLE IF NOT EXISTS `key_text_dict` (
  `md5` binary(16) NOT NULL,
  `text` varchar(500) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


"""