import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import common_ais
import helper_view



def stat_auto(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='All'
    db_name='raw_data_auto'
        
    start_time=helper_regex.time_floor(my_date)
    end_time=helper_regex.time_ceil(my_date)
    date_today=start_time.replace(' 00:00:00','')

    target_dbs=(
        ('sql_server',config.conn_mt,'ais_th_mt',),    
        ('sql_server',config.conn_zoota_mt,'vivas_mt',),    
    )    
    
    for db in target_dbs:
        if db[0]=='sql_server':

            sql=r'''
            select name from %s.sys.sysobjects where type = 'P' and category = 0 and name like 'stats_%%'
            ''' % (db[2],)
            target_store_procedures=helper_sql_server.fetch_set(sql=sql,conn_config=db[1])

            for store_procedure in target_store_procedures:
                category=store_procedure.replace('stats_','')

                sql=r'''
                EXEC [%s].[dbo].[%s]
                @begin_time = N'%s',
                @end_time = N'%s'
                ''' % (db[2],store_procedure,start_time,end_time)

                result=helper_sql_server.fetch_row(sql=sql,conn_config=db[1])
                result=sorted([(k,v) for k,v in result.iteritems() if not isinstance(k,int)])
                
                for k,v in result:
                    helper_mysql.put_raw_data(oem_name=oem_name,category=category,key=k,sub_key='',value=v, \
                                          table_name=db_name,date=date_today,created_on=None,db_conn=None)

                view_name=category.replace('_',' ')
                view_sql='select date as `Time`'
                
                for k,v in result:
                    if 'rate' in k.lower():
                        view_sql+='\n,concat(format(100.0*max(if(oem_name="%s" and category="%s" and `key`="%s",`value`,0)),2),"%%") as `%s`' % \
                                (oem_name,category,k,helper_regex.regex_replace(r'^(\d+\.)','',k))
                    else:
                        view_sql+='\n,max(if(oem_name="%s" and category="%s" and `key`="%s",`value`,0)) as `%s`' % \
                                (oem_name,category,k,helper_regex.regex_replace(r'^(\d+\.)','',k))
                    
                view_sql+='\nfrom `'+db_name+'`\nwhere (\n0'
     
                for k,v in result:
                    view_sql+='\nor oem_name="%s" and category="%s" and `key`="%s"' % (oem_name,category,k)
                
                view_sql+='\n) %where_and_more% \ngroup by date \norder by date desc'

                view_id=helper_view.replace_view(view_name=view_name,view_sql=view_sql,view_description='')
                
                helper_view.grant_view(view_name,'5')
                helper_view.grant_view(view_name,'17')



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat_auto(my_date) 