from datetime import datetime
import time
import helper_regex
import helper_mysql
import config


whole_collection={}
element_existance_counters={}

sql_collection_id=r'''

select `sub_key`,`value`
from mozat_stat.`raw_data`
where `oem_name`='Vodafone'
and `category`='moagent'
and `key`='app_page_by_app_daily_visitor_unique_collection_id'
and `date`='%s'

'''



def process(my_date):

    global whole_collection, element_existance_counters, sql_collection_id

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    
    collection_ids = helper_mysql.fetch_dict(sql_collection_id % (current_date,))

    for app,collection_id in collection_ids.iteritems():

        collection=helper_mysql.get_raw_collection_by_id(collection_id)

        if not collection_id or not collection:
            print 'No collection of',current_date,collection_id
            continue
        
        if not whole_collection.has_key(app):
            whole_collection[app]=set([])

        whole_collection[app]|=collection

        for i in collection:
            if not element_existance_counters.has_key(i):
                element_existance_counters[i]={}

            if not element_existance_counters[i].has_key(app):
                element_existance_counters[i][app]=0

            element_existance_counters[i][app]+=1




if __name__=='__main__':

    for i in range(110,0,-1):
        my_date=time.time()-3600*24*i
        process(my_date)
    
    print len(element_existance_counters)

    sql=r'''
    
    update mozat_clustering.user_figure_base
    set %s
    where `oem_id`=7
    and `user_id`='%s'
    limit 1

    '''
    counter=0
    for user_id,apps in element_existance_counters.iteritems():
        #print sql % (count,user_id)
        set_sql=''
        for app,count in apps.iteritems():
            if app=='unrecognized':
                continue
            set_sql+=',`app-'+app+'_days`='+str(count)
        
        if set_sql=='':
            continue

        #print sql % (set_sql[1:],user_id)
        #exit()
        helper_mysql.execute(sql % (set_sql[1:],user_id))
        counter+=1
        print counter


