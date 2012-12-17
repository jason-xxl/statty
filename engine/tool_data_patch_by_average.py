
import glob
import re
import helper_regex
from helper_mysql import db
import _mysql

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
        
        update raw_data_ais
        set `key`=concat(`key`,'_expired')
        where
            `oem_name`='%s'
            and `category`='%s'
            and `key`='%s'
            and `sub_key`='%s'
            and `date`='%s'
        limit 1
        
        ''' % (row_view['raw_data_ais.oem_name'],row_view['raw_data_ais.category'],row_view['raw_data_ais.key'],row_view['raw_data_ais.sub_key'].replace("\'","\\\'"),target_date))

        #print sql    
        #db.query(sql)

        sql=(r'''
        
        replace into raw_data_ais (
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
        from raw_data_ais
        where 
            `oem_name`='%s'
            and `category`='%s'
            and `key`='%s'
            and `sub_key`='%s'
            and `date`>='%s' and `date`<='%s'

        ''' % (row_view['raw_data_ais.oem_name'],row_view['raw_data_ais.category'],row_view['raw_data_ais.key'],row_view['raw_data_ais.sub_key'].replace("\'","\\\'"),target_date,add_offset_as_a_flag_for_trace, \
               row_view['raw_data_ais.oem_name'],row_view['raw_data_ais.category'],row_view['raw_data_ais.key'],row_view['raw_data_ais.sub_key'].replace("\'","\\\'"),patch_date_start,patch_date_end))

        print sql
        db.query(sql)

    #db.close()


if __name__=='__main__':


    for offset in range(0,3):

        print offset

        patch_date_start=helper_regex.date_add('2012-05-01',offset)
        patch_date_end=helper_regex.date_add('2012-05-04',offset)

        target_date=helper_regex.date_add('2012-05-05',offset)
        
        print target_date
        print patch_date_start
        print patch_date_end

        sql=r'''

        SELECT *
        FROM raw_data_ais
        /* FORCE INDEX (`date`)  force index to prevent wide range scan */
        where
            `oem_name`='Mozat'
            and `category`='login_only_ais'
            and `key` in ('user_last_login_1_day_unique','user_last_login_7_day_unique','user_last_login_30_day_unique','24h_new_sub_user_last_login_1_day_unique')

            /*and `key` = ''*/
            and `key` not like '%%_collection_id'
            and `key` not like '%%_weekly_unique%%'
            and `key` not like '%%_monthly_unique%%'
            and `key` not like '%%_by_url_%%'
            /*and `sub_key` = ''*/
            and `date`='%s'
        /*ORDER BY id ASC*/

        '''

        do(target_date=target_date, \
            patch_date_start=patch_date_start, \
            patch_date_end=patch_date_end,\
            sql=sql)

