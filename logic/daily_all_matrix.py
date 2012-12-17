import helper_sql_server
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_mysql
import config

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date

def stat_website(my_date):

    global current_date
    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    oem_name='All'
    stat_category='matrix'

    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    """
        Domain Name 	Remark
        ============================================
        mozat 	mozat production
        sbk-stc 	shabik stc
        sbk-bh 	shabik bahrain
        sbk-ku 	shabik kuwait viva
        voda-eg 	vodafone egypt
        telk-id 	telkomsel armor in indonesia 
    """

    available_domains=['voda-eg']

    sql_mutual_friend_by_domain={
        'voda-eg':r'''
                SELECT cast([user_id] as nvarchar)+'-'+cast([friend_id] as nvarchar) as [pair],null
                FROM [mozone_friend].[dbo].[friendship] with(nolock)
                where [ModifiedOn]>='%s'
                and [following]=1
                and [followed]=1'''
    }

    conn_by_domain={
        'voda-eg':config.conn_vodafone_88
    }

    successful_relation_counter_by_domain={
        'voda-eg':0
    }

    successful_relation_by_domain={
    }

    def get_recipient_count_add_friend(line):
        #print line
        operator=helper_regex.extract(line,r'friend req\s*(.*?)\s')
        sender=helper_regex.extract(line,r'\s+(\d+)\s+\[')
        ids=helper_regex.extract(line,r'\[([0-9\s,]*?)\]')
        if not ids:
            print 'not ids:'+line
            return 0
        ids=ids.split(',')
        if successful_relation_by_domain.has_key(operator):
            for i in ids:
                relation1=sender+'-'+i.strip()
                relation2=i.strip()+'-'+sender
                #print relation1
                #print relation2
                if successful_relation_by_domain[operator].has_key(relation1) or successful_relation_by_domain[operator].has_key(relation2):
                     successful_relation_counter_by_domain[operator]+=1
                     #print successful_relation_counter_by_domain[operator]
        return len(ids)


    def get_recipient_count(line):
        ids=helper_regex.extract(line,r'\[([0-9\s,]*?)\]')
        if not ids:
            print 'not ids:'+line
            return 0
        return ids.count(',')+1

    for i in available_domains:
        successful_relation_by_domain[i]=helper_sql_server.fetch_dict(conn_by_domain[i],sql_mutual_friend_by_domain[i] % (start_time))
        pass
   

    stat_plan=Stat_plan()
    
    #2011-04-18 11:25:37,309 [INFO] com.mozat.matrix.AutoFriend: 247 - [autofriend] friend req       voda-eg 3556248 164429  55025576        [55020826, 55008948, 55015462, 55009329, 55007176]

    stat_sql_auto_friend_request_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                       select_count_distinct_collection={'sender_id':r'\s+(\d+)\s+\['}, \
                                       select_sum={'recipient_count':get_recipient_count_add_friend}, \
                                       where={'auto_friend_request':r'(\[)autofriend\] friend req'}, \
                                       where_not={'non_empty':r'(\[\])'}, \
                                       group_by={'daily':get_current_date, \
                                                 'by_operator':r'friend req\s*(.*?)\s'})

    stat_plan.add_stat_sql(stat_sql_auto_friend_request_daily)
    
    #2011-04-18 11:25:37,309 [INFO] com.mozat.matrix.AutoFriend: 249 - [autofriend] added friend     voda-eg 3556248 164429  55025576        [55015492,55015482]

    stat_sql_auto_friend_add_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'sender_id':r'\s+(\d+)\s+\['}, \
                                   select_sum={'recipient_count':get_recipient_count}, \
                                   where={'auto_friend_added_friend':r'(\[)autofriend\] added friend'}, \
                                   where_not={'non_empty':r'(\[\])'}, \
                                   group_by={'daily':get_current_date, \
                                             'by_operator':r'added friend\s*(.*?)\s'})
    
    stat_plan.add_stat_sql(stat_sql_auto_friend_add_daily)

    stat_plan.add_log_source(r'\\192.168.0.82\log_matrix\stdout.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.run()    

    for i in available_domains:
        helper_mysql.put_raw_data(oem_name=oem_name,category=stat_category,key='auto_friend_request_by_operator_non_empty_daily_successful_count', \
                                    sub_key=i,value=successful_relation_counter_by_domain[i],date=current_date)


    # collected number
    
    key='daily_collected_phone_number_count'
    
    db=''
    sql="select count(*) from matrix_phone.phonenode where created_on>'%s' and created_on<'%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_mysql.get_one_value_string(sql,config.conn_vodafone_matrix)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)

    # collected relation
    
    key='daily_collected_relation_count'
    
    db=''
    sql=r'''select count(*)/2 from matrix_phone.phonelink where created_on>'%s' and created_on<'%s' ''' \
            % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_mysql.get_one_value_string(sql,config.conn_vodafone_matrix)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_website(my_date)
