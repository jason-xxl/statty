
import helper_sql_server
import config
import glob
import re
import helper_regex
import helper_mysql
import helper_file
import helper_math
from helper_mysql import db
import _mysql

def export():

    """
    mapping=helper_sql_server.fetch_dict(config.conn_umniah,sql=r'''

    SELECT user_id,replace(user_name,'@umniah.jor','') as [msisdn]
      FROM [mozone_user].[dbo].[Profile] with(nolock)
    where version_tag ='umniah.jor'

    ''')
    """



    print 'mapping size:',len(mapping)
    #print mapping

    collection_ids=helper_mysql.fetch_dict(r'''
    
    SELECT `date`,`value`
    FROM `raw_data`
    WHERE `oem_name` = 'Umniah'
    AND category = 'moagent'
    AND `key` = 'app_page_daily_visitor_unique_collection_id'
    ORDER BY date DESC
    
    ''')

    print 'collection_ids size:',len(collection_ids)
    
    file_name_pattern=r'E:\RoutineScripts\export\login_msisdn_%s.txt'
    unknown_title='Unknown MSISDN from monetid: %s'

    for date,collection_id in collection_ids.iteritems():
        collection=helper_mysql.get_raw_collection_by_id(int(collection_id))
        result=[]
        for id in collection:
            mapped_id=mapping.get(id,unknown_title % (id,))
            result.append(mapped_id)
            result.sort()

        file_name=file_name_pattern % (date,)
        helper_file.export_to_file(file_name, '\n'.join(result))
        print file_name






if __name__=='__main__':

    export()