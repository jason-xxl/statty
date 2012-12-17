
import config
import helper_file

default_filter_path=config.default_filter_path

filter_tpl=r"""

import re
import helper_regex
import datetime

def is_valid_user(line):
    key=helper_regex.extract(line,r"(?:monetid=)\s*(\d+)")
    if key:
        return user_list_dict.has_key(key)
    key=helper_regex.extract(line,r"(?:monetid=|PeerId=|from|id=|iMonetId:|monetId:|monet|\d+:\d+:\d+: )\s*(\d+)")
    return user_list_dict.has_key(key)

def is_not_valid_user(line):
    return not is_valid_user(line)

def get_filtered_dict(dict_obj):
    ret={}
    for k,v in dict_obj.iteritems():
        if user_list_dict.has_key(str(k)):
            ret[k]=v
    return ret

user_list_dict=%s

#len(user_list_dict): %s

def filter_user_base(filter_func):
    global user_list_dict
    user_list_dict_temp={}
    for k,v in user_list_dict.iteritems():
        if filter_func(k,v):
            user_list_dict_temp[k]=v
    user_list_dict=user_list_dict_temp

"""

def export_to_file(filter_name,user_id_dict):
    
    id_string=repr(user_id_dict)
    content=(filter_tpl % (id_string,len(user_id_dict)))
    file_full_path=default_filter_path+filter_name+'.py'
    helper_file.export_to_file(file_full_path,content)







filter_tpl_using_integer_set=r"""

import re
import helper_regex
import datetime
import helper_file

data_file_full_path=%s
user_list_set=None

if not user_list_set:
    user_list_set=helper_file.read_big_integer_set_from_file(data_file_full_path)
        
def is_valid_user(line): # using integer id set
    key=helper_regex.extract(line,r"(?:monetid=|PeerId=|from|id=|iMonetId:|monetId:|monet|\d+:\d+:\d+: )\s*(\d+)")
    if not key:
        return False
    return int(key) in user_list_set

def is_not_valid_user(line):
    return not is_valid_user(line)

def get_filtered_dict(dict_obj):
    ret={}
    for k,v in dict_obj.iteritems():
        if int(k) in user_list_set:
            ret[k]=v
    return ret

#len(user_list_set): %s

def filter_user_base(filter_func):
    global user_list_set
    user_list_set_temp=set([])
    for k in user_list_set:
        if filter_func(k):
            user_list_set_temp.add(k)
    user_list_dict=user_list_dict_temp

if __name__=='__main__':
    print data_file_full_path
    print len(user_list_set)

"""

def export_to_file_using_integer_set(filter_name,user_id_set):

    #write to file
    data_file_full_path=default_filter_path+filter_name+'.dat'
    helper_file.write_big_integer_set_to_file(data_file_full_path,user_id_set)

    #generate code    

    content=(filter_tpl_using_integer_set % (repr(data_file_full_path),len(user_id_set)))
    code_file_full_path=default_filter_path+filter_name+'.py'
    helper_file.export_to_file(code_file_full_path,content)
        







filter_tpl_using_string_set=r"""

import re
import helper_regex
import datetime
import helper_file

data_file_full_path=%s
user_list_set=None

if not user_list_set:
    user_list_set=helper_file.read_big_string_set_from_file(data_file_full_path)
        
def is_valid_user(line): # using string id set
    key=helper_regex.extract(line,r"(?:monetid=|PeerId=|from|id=|iMonetId:|monetId:|monet|\d+:\d+:\d+: )\s*(\d+)")
    return key in user_list_set

def is_not_valid_user(line):
    return not is_valid_user(line)

def get_filtered_dict(dict_obj):
    ret={}
    for k,v in dict_obj.iteritems():
        if k in user_list_set:
            ret[k]=v
    return ret

#len(user_list_set): %s

def filter_user_base(filter_func):
    global user_list_set
    user_list_set_temp=set([])
    for k in user_list_set:
        if filter_func(k):
            user_list_set_temp.add(k)
    user_list_dict=user_list_dict_temp

if __name__=='__main__':
    print data_file_full_path
    print len(user_list_set)

"""

def export_to_file_using_string_set(filter_name,user_id_set):

    #write to file
    data_file_full_path=default_filter_path+filter_name+'.dat'
    helper_file.write_big_string_set_to_file(data_file_full_path,user_id_set)

    #generate code    

    content=(filter_tpl_using_string_set % (repr(data_file_full_path),len(user_id_set)))
    code_file_full_path=default_filter_path+filter_name+'.py'
    helper_file.export_to_file(code_file_full_path,content)
        

if __name__=='__main__':

    pass