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


target_key=[
    'raw_data',
    'Vodafone',
    'moagent',
    'app_page_daily_visitor_unique_collection_id',
    '',
]

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

