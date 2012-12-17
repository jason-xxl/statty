import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import config
import helper_regex 
import helper_mysql

'''
chain: chat page -> chat room -> send message

note:
1) old and new chat page: logs are on different servers
2) old and new versions:

Exp:
1) only focus on Black Berry 
2) Black Berry Version >= 6.4.0.120405

new chat page: sbk-chatroom.i.morange.com/mobile_chatroom_new.aspx%
old chat page: sub_key_pattern='sbk-chatroom.i.morange.com/mobile_chatroom.aspx%

''' 
    
def get_current_date(line):
    global current_date
    return current_date

def stat_chatroom(my_date):
    global current_date
    
    # INFO 2010-04-08 00:00:00 - [          workThread] (        CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)
    is_valid_user=lambda line:True

    oem_name='Shabik_360' 
    stat_category='chatroom'
    table_name = 'raw_data_shabik_360'
    


    ####### old chat room ##################
    stat_plan=Stat_plan()
    stat_sql_enter_room_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'enter_room':r'(\[doEnterChatroom\])','only_shabik_360':is_valid_user}, \
                        group_by={'daily':lambda line:current_date},
                        db_name=table_name)

    stat_sql_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'send_msg':r'(\[send_a_msg\])','only_shabik_360':is_valid_user}, \
                        group_by={'daily':lambda line:current_date},
                        db_name=table_name)
   
    stat_sql_enter_room_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'enter_room':r'(\[doEnterChatroom\])'}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_room_id':r'roomId: (\d+);'},
                        db_name=table_name)

    stat_sql_msg_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'send_msg':r'(\[send_a_msg\])'}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_room_id':r'iRoomId:(\d+)'},
                        db_name=table_name)

    stat_sql_room_full_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId:\s*(\d+)'}, \
                        where={'room_full':r'(\[memberFull\])'}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_room_id':r'iRoomId:\s*(\d+)'},
                        db_name=table_name)

    stat_plan.add_stat_sql(stat_sql_enter_room_daily)
    stat_plan.add_stat_sql(stat_sql_msg_daily)
    stat_plan.add_stat_sql(stat_sql_enter_room_by_room_id_daily)
    stat_plan.add_stat_sql(stat_sql_msg_by_room_id_daily)
    stat_plan.add_stat_sql(stat_sql_room_full_by_room_id_daily)

    # no longer available
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.103\ChatroomShabik\logs\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    # no longer available
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.103\chatroom-shabik\logs\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.118\logs_chatroom_stc\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.run()
    
    
    ##################### new chatroom log #########################################
    stat_plan=Stat_plan()
    stat_sql_enter_room_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'new_chatroom_enter_room':r'(\[doEnterChatroom\])','only_shabik_360':is_valid_user}, \
                        group_by={'daily':lambda line:current_date},
                        db_name=table_name)

    stat_sql_msg_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'new_chatroom_send_msg':r'(\[send_a_msg\])','only_shabik_360':is_valid_user}, \
                        group_by={'daily':lambda line:current_date},
                        db_name=table_name)
   
    stat_sql_enter_room_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                        where={'new_chatroom_enter_room':r'(\[doEnterChatroom\])'}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_room_id':r'roomId: (\d+);'},
                        db_name=table_name)

    stat_sql_msg_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId: (\d+)'}, \
                        where={'new_chatroom_send_msg':r'(\[send_a_msg\])'}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_room_id':r'iRoomId:(\d+)'},
                        db_name=table_name)

    stat_sql_room_full_by_room_id_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                        select_count_distinct_collection={'monet_id':r'iMonetId:\s*(\d+)'}, \
                        where={'new_chatroom_room_full':r'(\[memberFull\])'}, \
                        group_by={'daily':lambda line:current_date, \
                                  'by_room_id':r'iRoomId:\s*(\d+)'},
                        db_name=table_name)

    stat_plan.add_stat_sql(stat_sql_enter_room_daily)
    stat_plan.add_stat_sql(stat_sql_msg_daily)
    stat_plan.add_stat_sql(stat_sql_enter_room_by_room_id_daily)
    stat_plan.add_stat_sql(stat_sql_msg_by_room_id_daily)
    stat_plan.add_stat_sql(stat_sql_room_full_by_room_id_daily)

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.177\shabik_newchatroom_logs\service.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
#    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
#                                        my_date,r'.\log\service.log.%(date)s-%(hour)s', \
#                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
    #stat_plan.add_log_source(r'\\192.168.0.177\shabik_newchatroom_logs\service.log.'+datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))

    stat_plan.run()



    ########## process ##########
    # old chatpage 
    config.conn_stat_portal=config._conn_stat_portal_158_2
    old_chatpage_ids = helper_mysql.get_merged_dict_collection_from_compact_raw_data_with_sub_key_pattern(
                                   oem_name='Shabik_360',category='moagent',key='app_page_by_url_pattern_daily_visitor_unique_collection_id',\
                                   sub_key_pattern='sbk-chatroom.i.morange.com/mobile_chatroom.aspx%',date=current_date,\
                                   table_name='data_url_pattern_shabik_360',db_conn=config.conn_stat_portal)
    print '----------','old_chatpage_ids: ', len(old_chatpage_ids)
    
    # old enter room ids
    config.conn_stat_portal=config._conn_stat_portal_142
    old_enter_room_ids = helper_mysql.get_dict_of_raw_collection_from_key(oem_name=oem_name,category=stat_category,\
                                  key='enter_room_only_shabik_360_daily_monet_id_unique_collection_id',\
                                    date=current_date,table_name=table_name,db_conn=config.conn_stat_portal)
    print '----------','old_enter_room_ids: ', len(old_enter_room_ids)
    print '----------','old_chatpage_ids & old_enter_room_ids: ', len( set(old_chatpage_ids.keys()) & set(old_enter_room_ids.keys()) ) 
    
       
    # old send message ids
    config.conn_stat_portal= config._conn_stat_portal_142
    old_send_message_ids = helper_mysql.get_dict_of_raw_collection_from_key(oem_name=oem_name,category=stat_category,\
                                key='only_shabik_360_send_msg_daily_monet_id_unique_collection_id',\
                                    date=current_date,table_name=table_name,db_conn=config.conn_stat_portal)
    print '----------','old_send_message_ids: ', len(old_send_message_ids)
    print '----------','old_chatpage_ids & old_send_message_ids: ', len( set(old_chatpage_ids.keys()) & set(old_send_message_ids.keys()) )
    print '----------','old_enter_room_ids & old_send_message_ids: ', len( set(old_enter_room_ids.keys()) & set(old_send_message_ids.keys()) ) 
    


    # new chat page
    config.conn_stat_portal=config._conn_stat_portal_158_2
    new_chatpage_ids = helper_mysql.get_merged_dict_collection_from_compact_raw_data_with_sub_key_pattern(
                                   oem_name='Shabik_360',category='moagent',key='app_page_by_url_pattern_daily_visitor_unique_collection_id',\
                                   sub_key_pattern='sbk-chatroom.i.morange.com/mobile_chatroom_new.aspx%',date=current_date,\
                                   table_name='data_url_pattern_shabik_360',db_conn=config.conn_stat_portal)
    print '----------','new_chatpage_ids: ', len(new_chatpage_ids)
               
    # new enter room ids
    config.conn_stat_portal=config._conn_stat_portal_142
    new_enter_room_ids = helper_mysql.get_dict_of_raw_collection_from_key(oem_name=oem_name,category=stat_category,\
                                  key='new_chatroom_enter_room_only_shabik_360_daily_monet_id_unique_collection_id',\
                                    date=current_date,table_name=table_name,db_conn=config.conn_stat_portal)
    print '----------','new_enter_room_ids: ', len(new_enter_room_ids)
    print '----------','new_chatpage_ids & new_enter_room_ids: ', len( set(new_chatpage_ids.keys()) & set(new_enter_room_ids.keys()) )
    
    # new send message ids
    config.conn_stat_portal= config._conn_stat_portal_142
    new_send_message_ids = helper_mysql.get_dict_of_raw_collection_from_key(oem_name=oem_name,category=stat_category,\
                                key='new_chatroom_send_msg_only_shabik_360_daily_monet_id_unique_collection_id',\
                                    date=current_date,table_name=table_name,db_conn=config.conn_stat_portal)
    print '----------','new_send_message_ids: ', len(new_send_message_ids)
    print '----------','new_chatpage_ids & new_send_message_ids: ', len( set(new_chatpage_ids.keys()) & set(new_send_message_ids.keys()) )
    print '----------','new_enter_room_ids & new_send_message_ids: ', len( set(new_enter_room_ids.keys()) & set(new_send_message_ids.keys()) ) 
    
    
    # black berry users 
    config.conn_stat_portal= config._conn_stat_portal_142
    black_berry_ids = helper_mysql.get_dict_of_raw_collection_from_key(oem_name=oem_name,category='moagent',\
                                key='app_page_by_morange_version_type_daily_user_unique_collection_id', sub_key='BlackBerry', \
                                    date=current_date,table_name=table_name,db_conn=config.conn_stat_portal)
    print '----------','black berry ids: ', len(black_berry_ids)
   
   
    # BLack new and old version users
    # the following two have bugs
    #new_version_pattern = 'Morange-([\d\.]+)-BlackBerry'# sample: 
    #black_berry_key= 'app_page_by_morange_version_daily_user_unique_collection_id', subkey='blackberry'
    
#    new_version_pattern = '.+Morange-([\d\.]+)-.+'# sample: 6.6.0.207_6.0.0.546_Morange-6.4.0.120524-cbb, Morange-6.2.0-cs60-3
    new_version_pattern = 'Morange-([\d\.]+)'
    black_berry_key = 'app_page_only_blackberry_by_bb_os_version_by_morange_version_daily_user_unique_collection_id'
    lowest_black_berry_version = '6.4.0.120405' 
    config.conn_stat_portal= config._conn_stat_portal_142
    def sub_key_pattern(sub_key): # sub key 
        v =  helper_regex.extract(sub_key,new_version_pattern) # extract the version       
        if v:
            try:
                return True if compare_morange_version_black_berry(v,lowest_black_berry_version)>=0 else False
            except ValueError:
                return False
        else:
            return False
    
    # extract
    new_black_berry_version_ids = helper_mysql.get_merged_dict_collection_from_raw_data_with_sub_key_pattern(oem_name=oem_name,category='moagent',\
                                key=black_berry_key, \
                                sub_key_pattern=sub_key_pattern, \
                                date=current_date,table_name=table_name,db_conn=config.conn_stat_portal)
    
    print '----------','new black berry ids: ', len(new_black_berry_version_ids)
    
    # bug
    # black berry old version
    old_black_berry_version_ids = dict_diff(black_berry_ids, new_black_berry_version_ids) 
    print '----------','old black berry ids: ', len(old_black_berry_version_ids)
    
    
    # new bb base
    key = 'new_bb_one_of_six_only_shabik_360_daily_monet_id_unique'
    new_bb_assign = sum(1 for mid in new_black_berry_version_ids if int(mid) % 6 ==0) 
    helper_mysql.put_raw_data(oem_name=oem_name,category=stat_category,key=key,value=new_bb_assign,date=current_date,table_name=table_name,db_conn=config.conn_stat_portal)
    
    ########## generate input data
    dicts1 = [old_chatpage_ids, old_enter_room_ids, old_send_message_ids, new_chatpage_ids, new_enter_room_ids, new_send_message_ids]
    dicts1_names = ['old_chatpage', 'old_enter_room', 'old_send_message', 'new_chatpage', 'new_enter_room', 'new_send_message']
    dicts1_view_names = ['Old Chatpage', 'Old Enter Room', 'Old Send Message', 'New Chatpage', 'New Enter Room', 'New Send Message']    
    dicts2 = [old_black_berry_version_ids, new_black_berry_version_ids]
    dicts2_names = ['old_black_berry_version', 'new_black_berry_version']
    dicts2_view_names = ['Old BB', 'New BB']
    name_suffix = '_only_shabik_360_daily_monet_id_unique'
        

    #### modify for generating view sql code
    if False:
        selected_index = slice(0,3) # old chatroom
        selected_index = slice(3,6) # new chatroom
        dicts1 = dicts1[selected_index]  
        dicts1_names = dicts1_names[selected_index]
        dicts1_view_names = dicts1_view_names[selected_index]
        dicts1_view_names = ['Page', 'Room', 'Message']
    
    ########## get result  data  
    result_key_value_pairs = get_OrderedDict()
    result_key_value_pairs.update(get_dict_total_unique_ratio(dicts1, dicts1_names, dicts1_view_names))
    result_key_value_pairs.update(get_dict_total_unique_ratio(dicts2, dicts2_names, dicts2_view_names))
    result_key_value_pairs.update(get_product_dict_total_unique_ratio(dicts1,dicts1_names,dicts1_view_names,dicts2, dicts2_names,dicts2_view_names))
    print result_key_value_pairs
    
    ########## store to db
    db_key_value_pairs = get_OrderedDict()
    name_suffixes = [name_suffix+t for t in ('_base','','_average')] # total, unique, ratio
    view_name_suffixes = ['Total', 'UV', 'Avg'] # total, unique, ratio
    view_names = list()
    for name, total_unque_ratio_view in result_key_value_pairs.iteritems():
        for name_suffix, value, view_name_suffix in zip(name_suffixes,total_unque_ratio_view[0:3],view_name_suffixes): 
            key = name + name_suffix
            view_name = total_unque_ratio_view[3] + ' '+view_name_suffix # space
            db_key_value_pairs[key]=value
            view_names.append(view_name)
            print 'store: ----->', key, '->', value, ' -->', view_name
    
    # store
    for key, value in db_key_value_pairs.iteritems():
        helper_mysql.put_raw_data(oem_name=oem_name,category=stat_category,key=key,value=value,date=current_date,table_name=table_name,db_conn=config.conn_stat_portal)

    ### view
    #print get_view_sql(table_name=table_name,oem_name=oem_name,stat_category=stat_category,keys=db_key_value_pairs, view_names=view_names)
    

def get_view_sql(table_name='', oem_name='',stat_category='',keys='', view_names=''):
    
    new_keys = ["`oem_name`='%s' and category='%s' and `key` = '%s'" % (oem_name, stat_category, key) for key in keys]
#    print new_keys

    fields = ',\n\n'.join(['max( if( %s, %s, 0) ) as `%s`' % (key, 'format(`value`,2)' if key.endswith("_average'") else '`value`', view_name) for view_name, key in zip(view_names,new_keys)])
    
    where = 'OR'.join(['(\n'+k+'\n)' for k in new_keys])
    
    sql = '''
    select 
    date as `Time`,
    
    %s
    
    from %s
    
    where (
    
    %s
    
    )%s
    GROUP BY date
    ORDER BY date DESC
    ''' % (fields, table_name, where,'%where_and_more%')
    
    return sql
    

def get_OrderedDict():    
    import sys
    if sys.version_info < (2,7):
        import helper_python26 as t 
        return t.OrderedDict()
    else:
        import collections
        return collections.OrderedDict()
        
def get_product_dict_total_unique_ratio(dict_list_1, name_list_1, view_name_list_1, dict_list_2, name_list_2, view_name_list_2):
        '''
        row is dict_list_1, column is dict_list_2
        dict_list_1: actions
        dict_list_2: group condition
        '''
        result = get_OrderedDict()
        for dict_2, name_2, view_name_2 in zip(dict_list_2, name_list_2, view_name_list_2):
            for dict_1, name_1, view_name_1 in zip(dict_list_1, name_list_1, view_name_list_1):
                dct = dict_retain_all(dict_1, dict_2) # intersection, values taken from the first dict
                name =  name_1+"_"+name_2 # by underscore
                view_name = view_name_1+" "+view_name_2 # by space
                result.update(get_dict_total_unique_ratio([dct],[name],[view_name]))
#                print '----------', name_1, ': ', len(dict_1), '\t', name_2, ': ', len(dict_2), '\t inter: ',len(dct), sum(dct.values()), sum(dct.values())/(len(dct)+1e-15)
        return result
            
def get_dict_total_unique_ratio(dict_list, name_list, view_name_list):
    '''
    Parameters: 
      1) dict_list: a list of dictionaries of monet_id and weight pairs 
      2) name_list: a list of corresponding names shown in the database
      3) view_name_list: a list of corresponding names shown in the view
      
    Output: 
    ordered dict: name -> (total, unique, ratio, view_name)
    
    '''
    if len(dict_list) != len(name_list) != len(view_name_list):
        raise ValueError('dict, name, and view_name lists should be the same size.')
    result = get_OrderedDict()
    for dct, name, view_name in zip(dict_list, name_list, view_name_list):
        total = sum(dct.values())
        unique= len(dct)
        ratio = 1.0*total/unique if unique!=0 and total!=0 else 0
        result[name] = (total, unique, ratio, view_name)
        print name,'->', result[name]
    return result
    
def dict_diff(dict1, dict2):
    '''
    pre-condition: dict1 is a super set of dicts 
    find out those keys exists in dict1 but not in dict2
     
    '''
    return dict((k, dict1[k]) for k in (set(dict1.keys()) - set(dict2.keys())))
        
       
def dict_retain_all(_dict, dict2):
    '''
    return a sub dict of _dict, where the keys exists in the keys of dict2
    '''
    return dict((k, _dict[k]) for k in _dict.keys() if dict2.has_key(k))

#compare morange version
def compare_morange_version_black_berry(v1, v2):
    'it contains four parts. sample: 6.4.0.120405'
    v1s = str(v1).split('.')
    v2s = str(v2).split('.')
    if len(v1s) != len(v2s):
            raise ValueError('lengths of the two versions are not the same')
    for i in range(0,len(v1s)):
        if int(v1s[i]) > int(v2s[i]):
            return 1
        elif int(v1s[i]) < int(v2s[i]):
            return -1
    return 0
        

if __name__=='__main__':
    
    for i in range(config.day_to_update_stat,0,-1):
        stat_chatroom(time.time()-3600*24*i)

    