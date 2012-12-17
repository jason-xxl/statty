import config
import redis
import helper_regex

redis_conn = redis.StrictRedis(host=config.conn_stat_portal_redis['host'], \
                               port=config.conn_stat_portal_redis['port'], \
                               db=config.conn_stat_portal_redis['db'])
"""
def set(key,value):
    global redis_conn
    return redis_conn.set(key,value)

def get(key):
    global redis_conn
    return redis_conn.get(key)

def remove(key):
    global redis_conn
    return redis_conn.delete(key)

def pipelined_append_list(dict_data):
    global redis_conn
    pipeline=redis_conn.pipe()
    #pipeline.
    
def set_hash_values(key,values):
    global redis_conn
    redis_conn.hmset(key,values)
    
def get_hash_value(key):
    global redis_conn
    return redis_conn.hmget(key)
"""

# time series log data, only append, auto ignore sequential duplicate, [time-stamp, value] as element, list based

def append_log_data(oem_name,monet_id,feature_name,time_string,value,auto_remove_duplicate=True):
    global redis_conn
    time_stamp=helper_regex.time_str_to_timestamp(time_string)
    key=':'.join([oem_name,str(monet_id),feature_name])
    
    last_values=get_log_data(oem_name,monet_id,feature_name,start_index=-2,end_index=-1)
    #print last_values
    #print [time_stamp,value]
    if last_values==[str(time_stamp),str(value)]:
        print 'redis ignored append:',key,time_stamp,value
        return False
    redis_conn.rpush(key,time_stamp)
    redis_conn.rpush(key,value)
    print 'redis append:',key,time_stamp,value
    return True

def get_log_data(oem_name,monet_id,feature_name,start_index=0,end_index=-1):
    global redis_conn
    key=':'.join([oem_name,str(monet_id),feature_name])
    return redis_conn.lrange(key,start_index,end_index)

def get_log_data_as_timestamp_to_value_dict(oem_name,monet_id,feature_name):
    global redis_conn
    key=':'.join([oem_name,str(monet_id),feature_name])
    temp=redis_conn.lrange(key,0,-1)
    result={}
    key=None
    for i in temp:
        if key is None:
            key=i
        else:
            result[key]=i
            key=None
    return result


def get_log_data_as_timestamp_value_tuple_list(oem_name,monet_id,feature_name):
    global redis_conn
    key=':'.join([oem_name,str(monet_id),feature_name])
    temp=redis_conn.lrange(key,0,-1)
    result=[]
    key=None
    for i in temp:
        if key is None:
            key=i
        else:
            result.append((key,i))
            key=None
    result=sorted(result)
    return result

def remove_key(oem_name,monet_id,feature_name):
    global redis_conn
    key=':'.join([oem_name,str(monet_id),feature_name])
    print 'redis remove:',key
    return redis_conn.delete(key)


if __name__=='__main__':

    append_log_data(oem_name='test',monet_id=123,feature_name='test_feature',time_string='2012-06-06 06:06:06',value=123456)
    append_log_data(oem_name='test',monet_id=123,feature_name='test_feature',time_string='2012-06-06 06:06:07',value=123456)
    print get_log_data(oem_name='test',monet_id=123,feature_name='test_feature')
    print get_log_data_as_timestamp_to_value_dict(oem_name='test',monet_id=123,feature_name='test_feature')
    print get_log_data_as_timestamp_value_tuple_list(oem_name='test',monet_id=123,feature_name='test_feature')
    remove_key(oem_name='test',monet_id=123,feature_name='test_feature')
    """
    set_hash_values('stc:01234:friend',1)
    print get_hash_value('stc:01234:friend')
    exit()
    
    for i in xrange(1000):      
        set('test:key1','test:key1')
        get('test:key1')
        remove('test:key1')

        set(-1,-1)
        get(-1)
        remove(-1)
    """

    pass

    

