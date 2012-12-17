import hashlib
import config
import _mysql
import helper_regex
import helper_mysql
import helper_collection
import config
import sys

import time
from datetime import date


def sha256(s):
    return hashlib.sha256(s).hexdigest()
    

def _get_sql_select_collection_id_by_date(oem_name,category,key,sub_key,table_name):
    sub_key=helper_mysql.escape_string(sub_key)

    if table_name.startswith('raw_data'):
        sql_date=helper_regex.generate_date_range_for_raw_data_query(day_range=1000)
        sql=r"""
        select `date`,`value` from `%s` where `oem_name`='%s' and `category`='%s' and `key`='%s' and `sub_key`='%s' %s
        """ %(table_name,oem_name,category,key,sub_key,sql_date)
        return sql


    if table_name.startswith('data_int'):
        sql_date=helper_regex.generate_date_range_for_data_query(day_range=1000)
        sql=r"""
        select concat(substring(`date`,1,4),'-',substring(`date`,5,2),'-',substring(`date`,7,2))
        ,`value` 
        from `%s` 
        where `key`=%s
        and `sub_key`=%s
        %s
        """ %(table_name,helper_regex.get_md5_key(oem_name,category,key),sub_key,sql_date)
        return sql

    if table_name.startswith('data'):
        sql_date=helper_regex.generate_date_range_for_data_query(day_range=1000)
        sql=r"""
        select concat(substring(`date`,1,4),'-',substring(`date`,5,2),'-',substring(`date`,7,2))
        ,`value` 
        from `%s` 
        where `key`=%s
        and `sub_key`=%s
        %s
        """ %(table_name,helper_regex.get_md5_key(oem_name,category,key),helper_regex.get_md5_sub_key(sub_key),sql_date)
        return sql

    sql=''
    return sql


def calculate_date_range_retain_rate(date_unit,oem_name,category,key,sub_key,date,table_name='raw_data'):

    if date_unit<1:
        date_unit=1        
        
    base_size,retain_rate,fresh_rate,lost_rate=0,0,1,1
    retained_base_size,lost_base_size,fresh_base_size=0,0,0
    
    if not date:
        return base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size
    
    #sql=r"select `date`,`value` from `%s` where `oem_name`='%s' and `category`='%s' and `key`='%s' and `sub_key`='%s'" \
    # %(table_name,oem_name,category,key,sub_key)

    key=key.replace('_collection_id','')+'_collection_id'
    sql=_get_sql_select_collection_id_by_date(oem_name,category,key,sub_key,table_name)

    collection_id_dict=helper_mysql.fetch_dict(sql)
    
    key_temp=collection_id_dict.keys()
    key_temp.sort(reverse=True)

    """
    print 'existing collection list:'    
    for i in key_temp[0:65]:
        print i+': '+str(collection_id_dict[i])
    """

    col_1=set([])
    for i in range(0,date_unit):
        date_temp=helper_regex.date_add(date,-i)
        col_id_temp=collection_id_dict[date_temp] if collection_id_dict.has_key(date_temp) else 0
        col_temp=helper_mysql.get_raw_collection_by_id(col_id_temp)
        col_1 |= col_temp

        if col_id_temp==0: #force return null when data not complete
            return base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size

    base_size=len(col_1)
    
    col_2=set([])
    for i in range(0+date_unit,date_unit+date_unit):
        date_temp=helper_regex.date_add(date,-i)
        col_id_temp=collection_id_dict[date_temp] if collection_id_dict.has_key(date_temp) else 0
        col_temp=helper_mysql.get_raw_collection_by_id(col_id_temp)
        col_2 |= col_temp

    retain=col_1 & col_2
    fresh=col_1 - col_2
    lost=col_2 - col_1
    
    """
    print str(col_1)
    print str(col_2)
    print str(retain)
    print str(fresh)
    print str(lost)
    """

    retained_base_size,lost_base_size,fresh_base_size=len(retain),len(lost),len(fresh)
    
    if len(col_2)>0:
        retain_rate=1.0*len(retain)/len(col_2)
        
    if len(col_1)>0 and len(col_2)>0:
        fresh_rate=1.0*len(fresh)/len(col_1)
        
    if len(col_2)>0:
        lost_rate=1.0*len(lost)/len(col_2)
        
    return base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size




def calculate_retain_rate_of_2_collections(collection_id_current,collection_id_base):
        
    base_size,retain_rate,fresh_rate,lost_rate=0,0,1,1
    retained_base_size,lost_base_size,fresh_base_size=0,0,0
    
    col_1=helper_mysql.get_raw_collection_by_id(collection_id_current)
    col_2=helper_mysql.get_raw_collection_by_id(collection_id_base)

    base_size=len(col_1)

    retain=col_1 & col_2
    fresh=col_1 - col_2
    lost=col_2 - col_1
    
    retained_base_size,lost_base_size,fresh_base_size=len(retain),len(lost),len(fresh)
    
    if len(col_2)>0:
        retain_rate=1.0*len(retain)/len(col_2)
        
    if len(col_1)>0 and len(col_2)>0:
        fresh_rate=1.0*len(fresh)/len(col_1)
        
    if len(col_2)>0:
        lost_rate=1.0*len(lost)/len(col_2)
        
    return base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size



def calculate_date_range_average_life_cycle(date_unit,oem_name,category,key,sub_key,date,table_name='raw_data'):
    if date_unit<1:
        date_unit=1        
        
    #base_size,retain_rate,fresh_rate,lost_rate=0,0,1,1
    lost_col_average_life_cycle=0
    retained_col_average_life_cycle=0
    
    if not date:
        return lost_col_average_life_cycle,retained_col_average_life_cycle,{},{}
        #return base_size,retain_rate,fresh_rate,lost_rate
    
    #sql=r"select `date`,`value` from `%s` where `oem_name`='%s' and `category`='%s' and `key`='%s' and `sub_key`='%s'" \
    # %(table_name,oem_name,category,key,sub_key)
    
    key=key.replace('_collection_id','')+'_collection_id'
    sql=_get_sql_select_collection_id_by_date(oem_name,category,key,sub_key,table_name)

    collection_id_dict=helper_mysql.fetch_dict(sql)
    
    key_temp=collection_id_dict.keys()
    key_temp.sort(reverse=True)

    """
    print 'existing collection list:'    
    for i in key_temp[0:65]:
        print i+': '+str(collection_id_dict[i])
    """

    col_1=set([])
    for i in range(0,date_unit):
        date_temp=helper_regex.date_add(date,-i)
        col_id_temp=collection_id_dict[date_temp] if collection_id_dict.has_key(date_temp) else 0
        col_temp=helper_mysql.get_raw_collection_by_id(col_id_temp)
        col_1 |= col_temp
        
        if col_id_temp==0: #force return null when data not complete
            return lost_col_average_life_cycle,retained_col_average_life_cycle,{},{}
    
    base_size=len(col_1)
    
    col_2=set([])
    for i in range(0+date_unit,date_unit+date_unit):
        date_temp=helper_regex.date_add(date,-i)
        col_id_temp=collection_id_dict[date_temp] if collection_id_dict.has_key(date_temp) else 0
        col_temp=helper_mysql.get_raw_collection_by_id(col_id_temp)
        col_2 |= col_temp

    
    lost_col=col_2 - col_1
    retained_col=col_2 & col_1

    lost_col_len=len(lost_col)
    retained_col_len=len(retained_col)

    lost_col_dict=dict([(k, 0) for k in lost_col])
    retained_col_dict=dict([(k, 0) for k in retained_col])

    for i in range(0,2000):
        date_temp=helper_regex.date_add(date,-i)
        
        if date_temp=='2010-01-01':
            break

        col_id_temp=collection_id_dict[date_temp] if collection_id_dict.has_key(date_temp) else 0
        col_temp=helper_mysql.get_raw_collection_by_id(col_id_temp)

        for i in col_temp:
            if lost_col_dict.has_key(i):
                lost_col_dict[i]+=1
            if retained_col_dict.has_key(i):
                retained_col_dict[i]+=1

    if lost_col_len>0:
        lost_col_average_life_cycle=sum(lost_col_dict.values())*1.0/lost_col_len

    if retained_col_len>0:
        retained_col_average_life_cycle=sum(retained_col_dict.values())*1.0/retained_col_len

    return lost_col_average_life_cycle,retained_col_average_life_cycle,lost_col_dict,retained_col_dict





def calculate_count_distinct(date_unit,oem_name,category,key,sub_key,date,table_name='raw_data',allow_collection_empty=False):
    
    #date_unit accepts 1,2,3,...,'weekly','monthly'
    #for weekly, it produces result only when date is Sunday, else 0
    #for monthly, it produces result only when date is the last day of a week, else 0
    #for all cases, it doesn't produce value when required collections are not all ready

    unique=0
    total=0
    average=0

    if not date:
        return unique,total,average

    if date_unit=='weekly':
        if helper_regex.get_weekday_from_date_str(date)!=7:
            return unique,total,average
        date_unit=7

    elif date_unit=='monthly':
        if helper_regex.extract(helper_regex.date_add(date,1),r'\d+\-\d+\-(\d+)')!='01':
            return unique,total,average

        first_date=helper_regex.extract(date,r'(\d+\-\d+\-)\d+')+'01'
        date_unit=helper_regex.get_day_diff_from_date_str(date,first_date)+1

    if date_unit<1:
        date_unit=1        

    key=key.replace('_collection_id','')
    sql=_get_sql_select_collection_id_by_date(oem_name,category,key+'_collection_id',sub_key,table_name)

    collection_id_dict=helper_mysql.fetch_dict(sql)

    key_temp=collection_id_dict.keys()
    key_temp.sort(reverse=True)


    sql=_get_sql_select_collection_id_by_date(oem_name,category,key+'_base',sub_key,table_name)
    
    #print sql
    collection_base_dict=helper_mysql.fetch_dict(sql)

    #print collection_base_dict
    #exit()
    
    """
    print 'existing collection list:'    
    for i in key_temp[0:65]:
        print i+': '+str(collection_id_dict[i])
    """

    col_1=set([])
    base_total=0
    for i in range(0,date_unit):
        date_temp=helper_regex.date_add(date,-i)

        col_id_temp=collection_id_dict[date_temp] if collection_id_dict.has_key(date_temp) else 0
        col_temp=helper_mysql.get_raw_collection_by_id(col_id_temp)
        col_1 |= col_temp

        base_total+=int(collection_base_dict[date_temp]) if collection_base_dict.has_key(date_temp) else 0
        
        if col_id_temp==0: #force return null when data not complete
            if allow_collection_empty:
                print date_temp,table_name,oem_name,category,key,sub_key,date_temp,'collection empty error! passed.'
            else:
                print date_temp,table_name,oem_name,category,key,sub_key,date_temp,'collection empty error! exit.'
                return unique,total,average

    unique=len(col_1)
    total=base_total
    average=base_total*1.0/unique if unique>0 else 0

    return unique,total,average


def calculate_count_distinct_named_collection(date_unit,oem_name,category,key,sub_key,date,table_name='raw_data',allow_collection_empty=False):
    
    #date_unit accepts 1,2,3,...,'weekly','monthly'
    #for weekly, it produces result only when date is Sunday, else 0
    #for monthly, it produces result only when date is the last day of a week, else 0
    #for all cases, it doesn't produce value when required collections are not all ready

    unique=0
    total=0
    average=0

    if not date:
        return unique,total,average

    if date_unit=='weekly':
        if helper_regex.get_weekday_from_date_str(date)!=7:
            return unique,total,average
        date_unit=7

    elif date_unit=='monthly':
        if helper_regex.extract(helper_regex.date_add(date,1),r'\d+\-\d+\-(\d+)')!='01':
            return unique,total,average

        first_date=helper_regex.extract(date,r'(\d+\-\d+\-)\d+')+'01'
        date_unit=helper_regex.get_day_diff_from_date_str(date,first_date)+1

    if date_unit<1:
        date_unit=1        

    key=key.replace('_collection_id','')
    sql=_get_sql_select_collection_id_by_date(oem_name,category,key,sub_key,table_name)

    collection_id_dict=helper_mysql.fetch_dict(sql)

    key_temp=collection_id_dict.keys()
    key_temp.sort(reverse=True)


    sql=_get_sql_select_collection_id_by_date(oem_name,category,key+'_base',sub_key,table_name)
    
    #print sql
    collection_base_dict=helper_mysql.fetch_dict(sql)
    #print collection_base_dict

    """
    print 'existing collection list:'    
    for i in key_temp[0:65]:
        print i+': '+str(collection_id_dict[i])
    """

    col_1=set([])
    base_total=0
    for i in range(0,date_unit):
        date_temp=helper_regex.date_add(date,-i)

        col_id_temp=collection_id_dict[date_temp] if collection_id_dict.has_key(date_temp) else 0
        #col_temp=helper_mysql.get_raw_collection_by_id(col_id_temp)

        col_temp=helper_collection.get_named_collection(table_name=table_name,oem_name=oem_name,category=category, \
                                                        key=key,sub_key=sub_key,date=date_temp)
        col_1 |= col_temp

        base_total+=int(collection_base_dict[date_temp]) if collection_base_dict.has_key(date_temp) else 0
        
        if col_id_temp==0: #force return null when data not complete
            if allow_collection_empty:
                print date_temp,table_name,oem_name,category,key,sub_key,date_temp,'collection empty error! passed.'
            else:
                print date_temp,table_name,oem_name,category,key,sub_key,date_temp,'collection empty error! exit.'
                return unique,total,average

    unique=len(col_1)
    total=base_total
    average=base_total*1.0/unique if unique>0 else 0

    return unique,total,average




def calculate_collection_retain_rate(collection_previous,collection_current):
    #def calculate_collection_retain_rate(date_unit,oem_name,category,key,sub_key,date,table_name='raw_data'):
    #different from calculate_date_range_retain_rate that it receive 2 collection
    #collection could be {table_name:'',oem_name:'',category:'',key:'',sub_key:'',date:''} or Set([]),[],()

    base_size,retain_rate,fresh_rate,lost_rate=0,0,1,1
    retained_base_size,lost_base_size,fresh_base_size=0,0,0
    
    col_1=helper_mysql.get_raw_collection(collection_current)
    col_2=helper_mysql.get_raw_collection(collection_previous)

    retain=col_1 & col_2
    fresh=col_1 - col_2
    lost=col_2 - col_1
    
    base_size=len(collection_previous)
    retained_base_size,lost_base_size,fresh_base_size=len(retain),len(lost),len(fresh)
    
    if len(col_2)>0:
        retain_rate=1.0*len(retain)/len(col_2)
        
    if len(col_1)>0 and len(col_2)>0:
        fresh_rate=1.0*len(fresh)/len(col_1)
        
    if len(col_2)>0:
        lost_rate=1.0*len(lost)/len(col_2)
        
    return base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size

def filter_and_count_distinct(list_obj,pattern='(.*)',ignore_empty=True):
    count=0
    count_distinct=0
    count_distinct_dict={}

    for i in list_obj:
        flag=helper_regex.extract(str(i),pattern)
        if ignore_empty and not flag:
            continue
        if not count_distinct_dict.has_key(flag):
            count_distinct_dict[flag]=0
        count_distinct_dict[flag]+=1
        count+=1

    count_distinct=len(count_distinct_dict)
    avg=0
    if count_distinct>0:
        avg=1.0*count/count_distinct

    return (count,count_distinct,avg,count_distinct_dict)


def md5(s):
    m=hashlib.md5()
    m.update(str(s))
    return m.hexdigest()



def perm(items, n=None):
    if n is None:
        n = len(items)
    for i in range(len(items)):
        v = items[i:i+1]
        if n == 1:
            yield v
        else:
            for p in perm(items[i+1:], n-1):
                yield v + p

def get_all_perm(items):
    result = []
    for i in xrange(len(items)):
        result += perm(items, i+1)
    return result

def get_simple_dispersion(user_to_int_value,step=100):
    user_to_int_value=dict((int(k),int(v)) for k,v in user_to_int_value.iteritems())
    ret_level={}
    ret_level_name={}

    for user_id,int_value in user_to_int_value.iteritems():
        level=int(int_value/step)
        level_name='[%s,%s)' % (level*step,level*step+step)
        ret_level_name[level_name]=ret_level_name.setdefault(level_name,0)+1
        ret_level[level*step]=ret_level.setdefault(level*step,0)+1
    
    return ret_level,ret_level_name




def group_and_sum(list_obj):
    ret={}
    for i in list_obj:
        if not ret.has_key(i[0]):
            ret[i[0]]=0
        ret[i[0]]+=int(i[1])
    return ret        

def group_and_count_distinct(list_obj):
    temp={}
    for i in list_obj:
        if not temp.has_key(i[0]):
            temp[i[0]]=set([])
        temp[i[0]].add(i[1])
    ret={}
    for i,j in temp.iteritems():
        ret[i]=len(j)
        
    return ret        

def filter_by_mod(int_set,mod_base,mod_value):
    return set([id for id in int_set if int(id) % mod_base == mod_value])


def get_uuid():
    import uuid
    return str(uuid.uuid1())



def merge_set_dict_by_mapping_function(set_dict,mapping_function_on_dict_key):
    result={}
    for k,v in set_dict.iteritems():
        k2=mapping_function_on_dict_key(k)
        result.setdefault(k2,set([]))
        result[k2] |= v
    return result


def get_similarity_of_two_set(set_1,set_2):
    if isinstance(set_1,dict):
        set_1=set(set_1.keys())
    if isinstance(set_2,dict):
        set_2=set(set_2.keys())
    return 1.0*len(set_1 & set_2)/len(set_1 | set_2)

def get_standard_deviation(target_set):
    import numpy
    temp=[int(i) for i in target_set]
    return numpy.std(temp)

if __name__ =='__main__':

    print get_standard_deviation(set(['1','2','3']))
    exit()

    print sha256("fdsf")
    print filter_and_count_distinct([1,2,3,3,4,4,5,5])
    print calculate_date_range_retain_rate(5,oem_name='Test',category='Test',key='Test',sub_key='Test',date='2010-12-30')
    print calculate_date_range_average_life_cycle(5,oem_name='Test',category='Test',key='Test',sub_key='Test',date='2010-12-30')
    print calculate_count_distinct(date_unit=3,oem_name='Shabik_360',category='moagent',key='app_page_by_app_daily_visitor_unique',sub_key='friend',date='2011-12-01',table_name='raw_data_shabik_360')
    print md5('8.8.8.8')
    print get_simple_dispersion({1:6789324,2:647832},100000)
    print filter_by_mod(set(['123','11']),11,2)
    print get_uuid()
