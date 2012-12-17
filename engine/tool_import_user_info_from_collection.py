from datetime import datetime
import time
import helper_regex
import helper_mysql
import config

sql_collection_id=r'''

select `value`
from mozat_stat.`raw_data`
where `oem_name`='Vodafone'
and `category`='chatroom'
and `key`='enter_room_daily_monet_id_unique'
and `sub_key`=''
and `date`='%s'

'''

def process():

    global sql_collection_id

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    info_dict=helper_mysql.fetch_dict(sql_collection_id)
    
    print len(info_dict)

    sql=r'''
    
    update mozat_clustering.user_figure_base
    set `%s`='%s'
    where `oem_id`=7
    and `user_id`='%s'
    limit 1

    '''

    field_name='client-type'

    for user_id,value in info_dict:
        helper_mysql.execute(sql % (field_name,value,user_id))



if __name__=='__main__':

    process()
    
