# -*- coding: utf-8 -*- 
import _mssql
import config
import helper_mysql
import helper_regex
import sys
import functools


conn_stc=config.conn_stc
conn_stc_mt=config.conn_stc_mt
conn_umniah=config.conn_umniah
conn_viva_bh=config.conn_viva_bh
conn_viva_bh_mt=config.conn_viva_bh_mt
conn_viva=config.conn_viva
conn_viva_mt=config.conn_viva_mt
conn_viva_invitation=config.conn_viva_invitation
conn_telk_armor=config.conn_telk_armor
conn_mozat=config.conn_mozat
conn_viva_billing=config.conn_viva_billing
conn_stc_billing=config.conn_viva_billing
conn_vodafone=config.conn_vodafone
conn_vodafone_mt=config.conn_vodafone_mt
conn_mt=config.conn_mt

conn_helper_db=config.conn_helper_db

db_pool={}


def get_db(conn_param):
    if not conn_param:
        conn_param=config.conn_stc
        
    key='_'.join(conn_param.values())
    if db_pool.has_key(key):
        return db_pool[key]

    db = _mssql.connect(server=conn_param['host'], user=conn_param['account'], password=conn_param['pwd'], \
        database=conn_param['db'],charset='utf8')

    db_pool[key]=db
    return db

 


def execute(conn_config,sql,log_sql=True):
    conn = get_db(conn_config)
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    #print dir(conn)
    conn.execute_non_query(sql)
    if log_sql:
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
    return conn.identity or conn.rows_affected
    try:
        pass
    except Exception,e:
        print 'failed:'+str(e)
        print "Unexpected error:", sys.exc_info()[0]
        print sql
        print 'SQL Server error happened !!!'
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1

    finally:
        #conn.close()
        pass


def fetch_scalar(conn_config,sql):
    conn = get_db(conn_config)
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    try:
        ret=str(conn.execute_scalar(sql))
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
        return ret
    except Exception,e:
        print 'failed:'+str(e)
        print "Unexpected error:", sys.exc_info()[0]
        print sql
        print 'SQL Server error happened !!!'
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1
    finally:
        #conn.close()
        pass


def fetch_scalar_int(conn_config,sql):
    ret=-1
    
    try:
        ret=fetch_scalar(conn_config,sql)
        if ret:
            ret=int(ret)
        else:
            ret=0
    except Exception,e:
        print 'failed:'+str(e)

    return ret


def fetch_scalar_float(conn_config,sql):
    ret=-1
    try:
        ret=fetch_scalar(conn_config,sql)
        if ret:
            ret=float(ret)
        else:
            ret=0
    except Exception,e:
        print 'failed:'+str(e)

    return ret


def fetch_row(conn_config,sql):
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    try:
        conn = get_db(conn_config)
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
        return conn.execute_row(sql)
    except Exception,e:
        print 'failed:'+str(e)
        print "Unexpected error:", sys.exc_info()[0]
        print sql
        print 'SQL Server error happened !!!'
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1
    finally:
        #conn.close()
        pass


def fetch_dict(conn_config,sql,key_name=0,value_name=1,use_interger_key=False):
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    sql=str(sql)

    conn = get_db(conn_config)
    rows=conn.execute_query(sql)
    helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
    dict={}
    counter=0
    if use_interger_key:
        for row in conn:
            dict[row[key_name]]=row[value_name]
            #counter+=1
            #if counter % 100000 ==0:
            #    print counter
            
    else:
        for row in conn:
            dict[str(row[key_name])]=row[value_name]
            #counter+=1
            #if counter % 100000 ==0:
            #    print counter
            
    return dict
    try:
        pass
    except Exception,e:
        print 'failed:'+str(e)
        print "Unexpected error:", sys.exc_info()[0]
        print sql
        print 'SQL Server error happened !!!'
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1
    finally:
        #conn.close()
        pass


def fetch_set(conn_config,sql):
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    conn = get_db(conn_config)
    rows=conn.execute_query(sql)
    helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
    ret=set([])
    for row in conn:
        ret.add(row[0])
    return ret

    try:
        pass
    except Exception,e:
        print 'failed:'+str(e)
        print "Unexpected error:", sys.exc_info()[0]
        print sql
        print 'SQL Server error happened !!!'
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1
    finally:
        #conn.close()
        pass


def fetch_rows_dict(conn_config,sql,key_name=0):
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    try:
        conn = get_db(conn_config)
        rows=conn.execute_query(sql)
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
        dict={}
        for row in conn:
            dict[str(row[key_name])]=row
                
        return dict

    except Exception,e:
        print 'failed:'+str(e)
        print "Unexpected error:", sys.exc_info()[0]
        print sql
        print 'SQL Server error happened !!!'
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1
    finally:
        #conn.close()
        pass


def fetch_rows(conn_config,sql):
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    try:
        conn = get_db(conn_config)
        rows=conn.execute_query(sql)
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)
        ret=[]
        for row in conn:
            ret.append(row)
                
        return ret

    except Exception,e:
        print 'failed:'+str(e)
        print "Unexpected error:", sys.exc_info()[0]
        print sql
        print 'SQL Server error happened !!!'
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1
    finally:
        #conn.close()
        pass

def _is_in_dict_keys(line,field_def='',the_dict={}):
    if callable(field_def):
        return the_dict.has_key(field_def(line))
    else:
        return the_dict.has_key(helper_regex.extract(line,field_def))


def get_filter_for_fetch_dict(conn_config,sql,field_def,key_name=0,value_name=1):
    
    the_dict=fetch_dict(conn_config,sql,key_name,value_name)
    if not the_dict:
        the_dict={}
    print 'get_filter_for_fetch_dict: dict length: ',len(the_dict)
    return functools.partial(_is_in_dict_keys, field_def=field_def,the_dict=the_dict)


def get_set_for_fetch_dict(conn_config,sql,key_name=0,value_name=1):
    
    the_dict=fetch_dict(conn_config,sql,key_name,value_name)
    if not the_dict:
        the_dict={}
    return set(the_dict.keys())


def fetch_dict_into_collection(conn_config,sql,key_name=0,oem_name='',category='',key='',sub_key='', \
                               table_name='raw_data',date='',db_conn=None):

    result_set=get_set_for_fetch_dict(conn_config,sql,key_name=key_name)
    helper_mysql.put_collection(collection=result_set,oem_name=oem_name,category=category,key=key,sub_key=sub_key, \
                                       table_name=table_name,date=date,db_conn=db_conn)
    return len(result_set)


def fetch_dict_map_to_collection(collection_set,sql_template,conn_config,step=1000,use_string_id=False):
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
        temp_dict=fetch_dict(conn_config,sql)
        result.update(temp_dict)

    print 'fetch_dict_map_to_collection: ',len(result),len(result)
    return result

def fetch_joined_array(conn_config,sql,seperator=''):
    
    start_time=helper_regex.get_time_clock_time()
    start_time_str=helper_regex.get_time_str_now()
    script_file_name=helper_regex.get_script_file_name()

    try:
        conn = get_db(conn_config)
        rows=conn.execute_query(sql)
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg='',start_time_str=start_time_str)

        array=[]
        for row in conn:
            array.append(row[0])
                
        return seperator.join(array)
    except Exception,e:
        print 'failed:'+str(e)
        print "Unexpected error:", sys.exc_info()[0]
        print sql
        print 'SQL Server error happened !!!'
        helper_mysql.log_execution_info(script_file_name,sql,start_time,error_msg=sys.exc_info()[0],start_time_str=start_time_str)
        return -1
    finally:
        #conn.close()
        pass


"""

def fetch_rows_dict_keys_into_collection(conn_config,sql,key_name=0):
    result=fetch_rows_dict(conn_config,sql,key_name=key_name)
    if not result:
        return -1
    return helper_mysql.put_raw_collection(result)
"""


if __name__=='__main__':

    #print execute(conn_config=config.conn_mt,sql=r'''SET CHARACTER SET utf8;''')
    #exit()
    import helper_mysql
    
    sql=u'''
    insert into stc_broadcast.dbo.list_id56(msisdn,p0,p1,p2) values('+966505599149',N'شفتي توت وخصري موت',N'أروع أحساس','');
    '''
    print sql
    
    #sql=u'''
    #insert into stc_broadcast.dbo.list_id56(msisdn,p0,p1,p2) values('+966505599149',N'%s',N'%s','');
    #''' % (helper_mysql.escape_string(u'شفتي توت وخصري موت'),helper_mysql.escape_string(u'أروع أحساس'))
    #print sql
    print execute(conn_config=config.conn_mt,sql=sql)

    exit()
    print fetch_scalar_float(conn_viva_bh,'SELECT count([id]) as c FROM [bahrain_users].[dbo].[admin_logs]')
    print fetch_row(conn_viva_bh,'SELECT count([id]) as c,1 as d FROM [bahrain_users].[dbo].[admin_logs]')
    print fetch_scalar_int(conn_umniah,'SELECT 1; --count(distinct [db_name]) FROM [DB82].[mozone_util].[dbo].[db_to_backup]')
    print fetch_dict(conn_umniah,'SELECT 1 as a,2 as b,3 as c; --[db_name] as a, null as b FROM [DB82].[mozone_util].[dbo].[db_to_backup]','a','b')
    """
    print fetch_scalar(conn_viva_bh,r'''
    SELECT msg
    from [DB86].[bahrain_mt].dbo.survey_reply with(nolock)
    where [CreatedOn]>='2010-09-13 00:00:00' and [CreatedOn]<'2010-09-14 00:00:00'
    ''')
    """
    
    """
    print execute(conn_stc_mt,r'''

    INSERT INTO [invitation_shabik].[dbo].[on_sub_friend_suggest_log]
           ([date]
           ,[monet_id]
           ,[suggested_friends])
    VALUES
           ('2010-01-01'
           ,10000
           ,2)
    ''')
    """

    """    
    print fetch_dict(conn_helper_db,r'''

    select
    top 1 1 as [d],4 as [d3]    
    from MYSQL82...UserAddFriendLogs
    where modified_on<='2010-12-21 05:00:00'
    and count_after>count_before
    ''')
    """
    a=get_filter_for_fetch_dict(conn_mozat,r'''

        select 
        distinct user_id,null
        from mozone_user.dbo.profile with(nolock) 
        where [creationDate]>='2011-01-01 00:00:00' and [creationDate]<'2011-01-11 00:00:00'


    '''
    ,r'(.*)',key_name=0,value_name=1) 

    print a(line='1d00')
