import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import common_shabik_360


def stat_sub(my_date, sub_user_total_use_real_time=False): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='STC'
    stat_category='sub'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    

    # sub action total
    
    key='sub_action_total'
    
    db='stc_integral'
    sql="select count(*) from [stc_integral].dbo.logs with(nolock) where action in (1,7,30) and createdon >= '%s' and createdon < '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)



    # unsub action total
    
    key='unsub_action_total'
    
    db='stc_integral'
    sql="select count(*) from [stc_integral].dbo.logs with(nolock) where action in (101,107,130) and createdon >= '%s' and createdon < '%s'" \
         % (start_time,end_time)
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    







    # sub users total
    
    key='sub_user_total'
    
    db='stc_integral'

    if sub_user_total_use_real_time:
        sql="select count(*) from [stc_integral].dbo.accounts with(nolock) where is_deleted=0" 
    else:
        sql=r'''
        
        select top 1 NumOfSubscribers
        from [shabik_mt].[dbo].[num_of_subscribers]
        where [OfDate] > DATEADD(day,1,'%s')
        order by [OfDate] asc
        
        ''' % (date_today,)
        
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar(config.conn_stc_mt,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)
    
    
    


    ## for analysis purpose, sub user is who has sub action, unsub user is who finally unsubed




    # unique sub user
    
    db='stc_integral'
    sql="""

    select distinct rtrim(msisdn)
    from [stc_integral].[dbo].[logs] a with(nolock)
    where [CreatedOn]>='%s' and [CreatedOn]<'%s'
    and [action] in (1,7,30)
    --and exists(
    --    select msisdn from [stc_integral].dbo.accounts with(nolock)
    --    where msisdn=a.msisdn
    --    and is_deleted=0
    --)

    """ % (start_time,end_time)

    print 'SQL Server:'+sql
    sub_user_msisdn=helper_sql_server.fetch_set(config.conn_mt,sql)
    print sub_user_msisdn
        
    key='sub_user_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,len(sub_user_msisdn))
        

        
    # unique sub user - creation date dispersion

    sub_user_to_creation_date_dict=common_shabik_360.get_profile_creation_date_from_msisdn(sub_user_msisdn)
    sub_user_creation_date_dispersion_dict=helper_regex.get_time_to_day_diff_dispersion(sub_user_to_creation_date_dict, \
                                                                            day_options=[0,],target_date=date_today)
    print sub_user_creation_date_dispersion_dict
    for d,msisdn_set in sub_user_creation_date_dispersion_dict.iteritems():
        key='sub_user_dispersion_unique'
        helper_mysql.put_raw_data(oem_name,stat_category,key,str(d),len(msisdn_set),date=date_today)






    # unique unsub user
    
    db='stc_integral'
    sql="""

    select distinct rtrim(msisdn)
    from [stc_integral].[dbo].[logs] a with(nolock)
    where [CreatedOn]>='%s' and [CreatedOn]<'%s'
    and [action] in (101,107,130)
    and exists(
        select msisdn from [stc_integral].dbo.accounts with(nolock)
        where msisdn=a.msisdn
        and is_deleted=1
    )


    """ % (start_time,end_time)

    print 'SQL Server:'+sql
    unsub_user_msisdn=helper_sql_server.fetch_set(config.conn_mt,sql)
    print unsub_user_msisdn
        
    key='unsub_user_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,len(unsub_user_msisdn))
        



    
    # unique unsub user - creation date dispersion


    unsub_user_to_creation_date_dict=common_shabik_360.get_profile_creation_date_from_msisdn(unsub_user_msisdn)
    unsub_user_creation_date_dispersion_dict=helper_regex.get_time_to_day_diff_dispersion(unsub_user_to_creation_date_dict, \
                                                                            day_options=[0,7,30,60],target_date=date_today)


    for d,msisdn_set in unsub_user_creation_date_dispersion_dict.iteritems():
        key='unsub_user_dispersion_unique'
        helper_mysql.put_raw_data(oem_name,stat_category,key,str(d),len(msisdn_set),date=date_today)
        
        
        



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_sub(my_date)
