
import glob
import re
import helper_regex
from helper_mysql import db
import _mysql
import helper_mysql
import config

source_mysql_db=config.conn_stat_portal
target_mysql_db=config.conn_stat_portal_111

raw_data_table_list=[
    'raw_data_client_crash'
]


for table_name in raw_data_table_list:
    print 'start transfering: ',table_name
    transfer_one_table(table_name)
    
    pass    

def transfer_one_table(table_name):
    
    helper_mysql.execute('truncate table mozat_stat.'+table_name, db_conn=target_mysql_db)
    step=10000
    current_max_id=0

    






'''
Patch unresistly lost data by replacing lost value with average of previous data.
'''

def do(target_date,patch_date_start,patch_date_end,sql):

    add_offset_as_a_flag_for_trace='0'

    # define data template to guide patch

    sql=sql % (patch_date_end,)

    db.query(sql)

    result_view = db.store_result()

    while 1:
        
        row_view = result_view.fetch_row(how=2)
        if not row_view:
            break
        
        row_view=row_view[0]
        print str(row_view)

        sql=(r'''
        
        update raw_data
        set `key`=concat(`key`,'_expired')
        where
            `oem_name`='%s'
            and `category`='%s'
            and `key`='%s'
            and `sub_key`='%s'
            and `date`='%s'
        limit 1
        
        ''' % (row_view['raw_data.oem_name'],row_view['raw_data.category'],row_view['raw_data.key'],row_view['raw_data.sub_key'].replace("\'","\\\'"),target_date))

        print sql    
        db.query(sql)

        sql=(r'''
        
        insert into raw_data (
            `oem_name`
            ,`category`
            ,`key`
            ,`sub_key`
            ,`date`
            ,`value`
        ) select
            '%s' as `oem_name`
            ,'%s' as `category`
            ,'%s' as `key`
            ,'%s' as `sub_key`
            ,'%s' as `date`
            ,ceil(1.0*sum(if(value is null,0,value))/count(*)) + %s as `value`
        from raw_data
        where 
            `oem_name`='%s'
            and `category`='%s'
            and `key`='%s'
            and `sub_key`='%s'
            and `date`>='%s' and `date`<='%s'

        ''' % (row_view['raw_data.oem_name'],row_view['raw_data.category'],row_view['raw_data.key'],row_view['raw_data.sub_key'].replace("\'","\\\'"),target_date,add_offset_as_a_flag_for_trace, \
               row_view['raw_data.oem_name'],row_view['raw_data.category'],row_view['raw_data.key'],row_view['raw_data.sub_key'].replace("\'","\\\'"),patch_date_start,patch_date_end))

        print sql
        db.query(sql)

    #db.close()


if __name__=='__main__':


    for offset in range(0,1):

        print offset

        patch_date_start=helper_regex.date_add('2011-05-06',offset)
        patch_date_end=helper_regex.date_add('2011-05-09',offset)

        target_date=helper_regex.date_add('2011-05-10',offset)
        
        print target_date
        print patch_date_start
        print patch_date_end

        sql=r'''

        SELECT *
        FROM raw_data
        FORCE INDEX (`date`)  /* force index to prevent wide range scan */
        where
            `oem_name`='Vodafone'
            and `category`='login'
            and `key` = 'user_last_login_1_day_unique'
            and `key` not like '%%_collection_id'
            /*and `sub_key` = ''*/
            and `date`='%s'
        ORDER BY id ASC

        '''

        do(target_date=target_date, \
            patch_date_start=patch_date_start, \
            patch_date_end=patch_date_end,\
            sql=sql)

