from datetime import datetime
import time
import helper_regex
import helper_mysql
import config


whole_collection=set([])
element_existance_counters={}

client_dict=[
    'Symbian','JME',
    'Android','iOS','BlackBerry',
]

client_number_dict={
    'JME':1,
    'Symbian':2,
    'Android':3,
    'iOS':4,
    'BlackBerry':5,
}
        


sql_collection_id=r'''

select `date`,`value`
from `raw_data`
where `oem_name`='Vodafone'
and `category`='moagent'
and `key`='daily_login_user_by_client_type_collection_id'
and `sub_key`='%s'

''' 





def process(client_type):

    global whole_collection, element_existance_counters, sql_collection_id, client_dict

    current_collection={}

    type_number=client_number_dict[client_type]
  
    collection_ids = helper_mysql.fetch_dict(sql_collection_id % (client_type,))

    print collection_ids

    for date,collection_id in collection_ids.iteritems():

        collection=helper_mysql.get_raw_collection_by_id(collection_id)

        if not collection_id or not collection:
            print 'No collection of',collection_id
            continue
        
        whole_collection|=collection

        for i in collection:
            current_collection[i]=type_number

    print len(current_collection)

    field_name='client-type'

    sql=r'''
    
    update mozat_clustering.user_figure_base
    set `%s`='%s'
    where `oem_id`=7
    and `user_id`='%s'
    limit 1

    '''

    for user_id, t in current_collection.iteritems():
        helper_mysql.execute(sql % (field_name,t,user_id))



if __name__=='__main__':

    for t in client_dict:
        process(t)

    print len(whole_collection)
