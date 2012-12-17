
import helper_regex
import helper_mysql
import config

end_date='2011-09-21'
length_backwards=3

data_name_spaces=[
    ('raw_data_url_pattern_shabik_360','Shabik_360','moagent_error_page','',
    config._conn_stat_portal_158,config._conn_stat_portal_158_2),
    
]



def duplicate_record(item,target_date):

    step=10000
    current_idx=0
    helper_mysql.quick_insert=True
    helper_mysql.print_log=False

    sql=r"select * from %s where `oem_name`='%s' and `category`='%s'" % (item[0],item[1],item[2])
    if item[3]:
        sql+=r" and `key`='%s'" % (item[3],)
    sql+=r" and `date`='%s'" % (target_date,)

    for i in range(1000):
        counter=1

        sql_limit=r" limit %s,%s" % (current_idx,step)

        #print sql+sql_limit

        rows=helper_mysql.fetch_rows(sql+sql_limit,db_conn=item[4])
        if not rows:
            print 'end.'
            break
        
        print target_date,current_idx,len(rows),item
        
        #do copy
        
        for row in rows:
            helper_mysql.put_raw_data(
                oem_name=row['oem_name'],
                category=row['category'],
                key=row['key'],
                sub_key=row['sub_key'],
                value=row['value'],
                table_name=item[0],
                date=row['date'],
                created_on=row['created_on'],
                db_conn=item[5])

            counter+=1
            #print counter
            
        current_idx+=step



if __name__=='__main__':

    for i in range(0,length_backwards):
        
        target_date=helper_regex.date_add(end_date,-i)

        for item in data_name_spaces:
            duplicate_record(item,target_date)
        


    


 







