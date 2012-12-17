
import glob
import re
import helper_regex
import helper_mysql
from helper_mysql import db
import _mysql

'''
Patch lost data by replacing lost value with estimation of other column.
'''

if __name__=='__main__':

    #definition

    target_date_start='2012-04-18'
    target_date_length=2

    sql_base_table_name=r'raw_data'
    sql_base_date_range_start=r'2012-04-15'
    sql_base_date_range_end=r'2012-04-15'
    sql_base_columns=r'''

        SELECT 
        `oem_name`,`category`,`key`,`sub_key`
        ,sum(if(`value` is not null,`value`,0))*1.0/if(sum(if(`value` is not null,1,0))>0,sum(if(`value` is not null,1,0)),1) as `value`
        ,max(`date`) as `date`

        FROM `%s`
        /*FORCE INDEX (`date`)  */ /* force index to prevent wide range scan */
        
        where
            `oem_name`='Mozat'
            and `category`='mosession_only_ais'
            and `key` not like '%%_collection_id'
            and `key` not like '%%_by_url_%%'
            and `key` not like '%%_expired'
            /*
            and (`key` like 'send_msg_text_daily_monet_id_unique%%' or `key` like 'enter_room_daily_monet_id_unique%%')
            and `key` like '%%msisdn_unique'
            and `sub_key` = ''
            */

            and `date`>='%s' and `date`<='%s'
        
        group by `oem_name`,`category`,`key`,`sub_key`
        order by `oem_name`,`category`,`key`,`sub_key`

    ''' % (sql_base_table_name,sql_base_date_range_start,sql_base_date_range_end)

    sql_reference_columns_tpl=r'''
        
        SELECT 
        sum(if(`value` is not null,`value`,0))*1.0/if(sum(if(`value` is not null,1,0))>0
        ,sum(if(`value` is not null,1,0)),1) as `value`

        FROM `raw_data`
        
        where
            `oem_name`='Mozat'
            and `category`='moagent_only_ais'
            and `key` = 'app_page_daily_visitor_unique' /*'app_page_daily_visitor_unique'*/

            /*and `key` not like '%%_collection_id'
            and `key` not like '%%_by_url_%%'*/
            /*and `sub_key` = ''*/

            and `date`>='%s' and `date`<='%s'
        
    '''

    #get reference value base

    reference_value_base_sql=sql_reference_columns_tpl % (sql_base_date_range_start,sql_base_date_range_end)

    #print reference_value_base_sql
    reference_value_base=helper_mysql.get_one_value_string(reference_value_base_sql)

    print 'reference_value_base: '+reference_value_base

    #loop taget dates

    for offset in range(0,target_date_length):

        current_date=helper_regex.date_add(target_date_start,offset)
        print 'current_date: '+current_date

        #get reference value of target date

        reference_value_target=helper_mysql.get_one_value_string(
                        sql_reference_columns_tpl 
                        % (current_date,current_date))

        print 'reference_value_target: '+reference_value_target

        #get base columns

        db.query(sql_base_columns)
        result_view = db.store_result()
        
        while 1:
            
            row_view = result_view.fetch_row(how=1)
            if not row_view:
                break
            
            row_view=row_view[0]
            print str(row_view)

            #exit()
            sql=r'''
            
            update `%s`
            set `key`=concat(`key`,'_expired')
            where
                `oem_name`='%s'
                and `category`='%s'
                and `key`='%s'
                and `sub_key`='%s'
                and `date`='%s'
            limit 1
            
            ''' % (sql_base_table_name,row_view['oem_name'],row_view['category'],row_view['key'],row_view['sub_key'].replace("\'","\\\'"),current_date)

            print sql  
            
            #db.query(sql)
            
            sql=r'''
            
            replace into `%s` (
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
                ,case 
                when '%s' like '%%_unique' then ceil(%s*1.0*%s/%s) 
                when '%s' like '%%_unique_base' then ceil(%s*1.0*%s/%s) 
                when '%s' like '%%_unique_average' then %s 
                else ceil(%s*1.0*%s/%s) end as `value`

            ''' % (sql_base_table_name,row_view['oem_name'],row_view['category'],row_view['key'],row_view['sub_key'].replace("\'","\\\'"),current_date,row_view['key'],row_view['value'],reference_value_target,reference_value_base,row_view['key'],row_view['value'],reference_value_target,reference_value_base,row_view['key'],row_view['value'],row_view['value'],reference_value_target,reference_value_base)

            print sql
            db.query(sql)
            #exit()

        

            """
            """







