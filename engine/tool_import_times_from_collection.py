from datetime import datetime
import time
import helper_regex
import helper_mysql
import config

config.collection_cache_enabled=True

whole_collection=set([])
element_existance_counters={}


sql_collection_id=r'''

select `value`
from mozat_stat.`raw_data`
where `oem_name`='Vodafone'
and `category`='moagent'
and `key`='app_page_daily_visitor_unique_collection_id'
and `sub_key`=''
and `date`='%s'

'''
"""

sql_collection_id=r'''

select `value`
from mozat_stat.`raw_data`
where `oem_name`='Vodafone'
and `category`='mochat'
and `key`='send_msg_text_daily_monet_id_unique_collection_id'
and `sub_key`=''
and `date`='%s'

'''

sql_collection_id=r'''

select `value`
from mozat_stat.`raw_data`
where `oem_name`='Vodafone'
and `category`='im'
and `key`='user_try_login_daily_monet_id_unique_collection_id'
and `sub_key`=''
and `date`='%s'

'''



sql_collection_id=r'''

select `value`
from mozat_stat.`raw_data`
where `oem_name`='Vodafone'
and `category`='chatroom'
and `key`='enter_room_daily_monet_id_unique_collection_id'
and `sub_key`=''
and `date`='%s'

'''
"""


def process(my_date):

    global whole_collection, element_existance_counters, sql_collection_id

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    
    collection_id = helper_mysql.get_one_value_int(sql_collection_id % (current_date,))

    collection=helper_mysql.get_raw_collection_by_id(collection_id)

    if not collection_id or not collection:
        print 'No collection of',current_date,collection_id
        return
    
    whole_collection|=collection

    for i in collection:
        if not element_existance_counters.has_key(i):
            element_existance_counters[i]=0
        element_existance_counters[i]+=1




if __name__=='__main__':

    for i in range(200,0,-1):
        my_date=time.time()-3600*24*i
        process(my_date)
    
    print len(element_existance_counters)

    field_name='login-login_days'
    #field_name='app-mochat_service_days'
    #field_name='app-im_service_days'
    #field_name='app-chatroom_service_days'
    
    sql=r'''
    
    update mozat_clustering.user_figure_base
    set `%s`='%s'
    where `oem_id`=7
    and `user_id`='%s'
    limit 1

    '''

    for user_id,count in element_existance_counters.iteritems():
        #print sql % (count,user_id)
        helper_mysql.execute(sql % (field_name,count,user_id))




