import helper_sql_server
import glob
import re
import os
import helper_regex
from helper_mysql import db
import _mysql
import helper_mysql
import helper_file
import config


target_keys={
    'All':'raw_data_shabik_360|Shabik_360|moagent|app_page_daily_visitor_unique||',
    'Mochat':'raw_data_shabik_360|All|mochat|only_shabik_360_send_msg_text_daily_monet_id_unique||',
    'Profile':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|profile|',
    'Friend':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|friend|',
    'Ocean Age':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|ocean_age|',
    'Photo':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|photo|',
    'IM':'raw_data_shabik_360|All|im|only_shabik_360_user_try_login_daily_monet_id_unique||',
    'Message':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|message|',
    'Nearby':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|location|',
    'Chatroom':'raw_data_shabik_360|Shabik_360|chatroom|enter_room_only_shabik_360_daily_monet_id_unique||',
    'Notification':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|notification|',
    'Happy Barn':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|happy_barn|',
    'Circle':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|circle|',
    'App Center':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|app_center|',
    'Facebook':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|facebook|',
    'Billing':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|billing|',
    'Status':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|status|',
    'Recent Visitor':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|recent_visitor|',
    'Star User':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|star_user|',
    'Poll':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|poll|',
    'Level System':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|level_system|',
    'Twitter':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|twitter|',
    'Heroes':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|heroes|',
    'Help':'raw_data_shabik_360|Shabik_360|moagent|app_page_by_app_daily_visitor_unique|help|',
}

def extract_key_name(combined_key_name):
    combined_key_name=combined_key_name.split('|')
    return {
        'table_name':combined_key_name[0],
        'oem_name':combined_key_name[1],
        'category':combined_key_name[2],
        'key':combined_key_name[3],
        'sub_key':combined_key_name[4],
        'date':combined_key_name[5],
    }


target_key=[
    'raw_data',
    'Vodafone',
    'login_service',
    'app_page_daily_visitor_unique_collection_id',
    '',
]

def export(target_key):
    
    lacked_dates=[]
    exported_dates=[]

    dir_name='.\\export_collection\\'+'_'.join(target_key).strip('_')

    collection_ids=helper_mysql.fetch_dict(sql=r'''
    
    select `date`,`value`
    from `%s`
    where 
    `oem_name`='%s'
    and `category`='%s'
    and `key`='%s'
    and `sub_key`='%s'
    order by date desc
    limit 200
    
    ''' % (target_key[0],target_key[1],target_key[2],target_key[3],target_key[4],))

    helper_file.prepare_directory_on_windows(dir_name)

    print 'collection_ids:',len(collection_ids) 


    # load user_id->msisdn mapping


    user_id_to_msisdn={}
    file_handler=open('E:\\WebStatShare\\vodafone_user_id_to_msisdn.csv',"r")
    for line in file_handler:
        line=line.strip(os.linesep).strip()
        #print line
        if not line:
            continue
        
        c=line.find(',')
        if c==-1:
            continue
        msisdn=line[c+1:].strip('X').strip()
        if msisdn.isdigit():
            user_id_to_msisdn[int(line[0:c])]=int(msisdn)

    file_handler.close()

    print 'user_id_to_msisdn:',len(user_id_to_msisdn)



    for date,collection_id in collection_ids.iteritems():
        
        collection=helper_mysql.get_raw_collection_by_id(collection_id)
        if not collection:
            lacked_dates.append(date)
            continue
        
        msisdn_set=set([])

        for i in collection:
            if not i.isdigit():
                continue
            user_id=int(i)
            
            if user_id_to_msisdn.has_key(user_id):
                msisdn_set.add(user_id_to_msisdn[user_id])
                #print user_id,user_id_to_msisdn[user_id]
                #continue
            #print user_id
        
        helper_file.write_big_string_set_to_file(dir_name+'\\'+date+'.txt',msisdn_set)

        exported_dates.append((date,len(collection),len(msisdn_set)))
        
    
    print 'lacked_dates:',lacked_dates
    print 'exported_dates',exported_dates
        




if __name__=='__main__':

    export(target_key)

