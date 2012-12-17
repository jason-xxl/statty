import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
from user_id_filter import user_id_filter_viva_bh
import config


def stat_user_properties(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Viva_BH'
    stat_category='user_properties'
        
    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')
    
    # gender
    
    key='gender'
    
    db=''
    sql=r"""

    SELECT [gender]
    ,count([user_id])
    FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [user_name] like '%@viva.bh'
    group by [gender]

    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)

    # relationship
    
    key='relationship'
    
    db=''
    sql=r"""

    SELECT [relationship_status]
            ,count([user_id])
    FROM [mozone_user].[dbo].[ProfileRelationships] with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    group by [relationship_status]
    
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)


    # relationship looking for
    
    key='relationship_looking_for'
    
    db=''
    sql=r"""

    SELECT [looking_for]
            ,count([user_id])
    FROM [mozone_user].[dbo].[ProfileRelationships] with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    group by [looking_for]
    
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)




    # profession
    
    key='profession'
    
    db=''
    sql=r"""

    SELECT profession
            ,count([user_id])
    FROM [mozone_user].[dbo].ProfileWork with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    group by profession
    
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)






    # education
    
    key='highest_education'
    
    db=''
    sql=r"""
    
    SELECT highest_education
            ,count([user_id])
    FROM [mozone_user].[dbo].ProfileEducation with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    group by highest_education
    
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)








    # favorite club
    
    key='favorite_club'
    
    db=''
    sql=r"""
    
    SELECT '1',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfileShabik with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    and club like '%1%'

    union

    SELECT '2',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfileShabik with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    and club like '%2%'

    union

    SELECT '3',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfileShabik with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    and club like '%3%'

    union

    SELECT '4',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfileShabik with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    and club like '%4%'

    union

    SELECT '5',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfileShabik with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    and club like '%5%'

    union

    SELECT '99',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfileShabik with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    and club like '%99%'

    union

    SELECT 'na',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfileShabik with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    and club is null
    
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)







    # hobby
    
    key='hobby'
    
    db=''
    sql=r"""
    
    SELECT '1',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '1'
        or interests like '1,%'
        or interests like '%,1'
        or interests like '%,1,%'
    )

    union

    SELECT '2',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '2'
        or interests like '2,%'
        or interests like '%,2'
        or interests like '%,2,%'
    )

    union

    SELECT '3',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '3'
        or interests like '3,%'
        or interests like '%,3'
        or interests like '%,3,%'
    )

    union

    SELECT '4',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '4'
        or interests like '4,%'
        or interests like '%,4'
        or interests like '%,4,%'
    )

    union

    SELECT '5',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '5'
        or interests like '5,%'
        or interests like '%,5'
        or interests like '%,5,%'
    )

    union

    SELECT '6',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '6'
        or interests like '6,%'
        or interests like '%,6'
        or interests like '%,6,%'
    )

    union

    SELECT '7',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '7'
        or interests like '7,%'
        or interests like '%,7'
        or interests like '%,7,%'
    )

    union

    SELECT '8',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '8'
        or interests like '8,%'
        or interests like '%,8'
        or interests like '%,8,%'
    )

    union

    SELECT '9',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '9'
        or interests like '9,%'
        or interests like '%,9'
        or interests like '%,9,%'
    )

    union

    SELECT '10',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '10'
        or interests like '10,%'
        or interests like '%,10'
        or interests like '%,10,%'
    )

    union

    SELECT '11',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '11'
        or interests like '11,%'
        or interests like '%,11'
        or interests like '%,11,%'
    )

    union

    SELECT '12',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '12'
        or interests like '12,%'
        or interests like '%,12'
        or interests like '%,12,%'
    )

    union

    SELECT '13',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '13'
        or interests like '13,%'
        or interests like '%,13'
        or interests like '%,13,%'
    )

    union

    SELECT '14',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '14'
        or interests like '14,%'
        or interests like '%,14'
        or interests like '%,14,%'
    )

    union

    SELECT '15',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '15'
        or interests like '15,%'
        or interests like '%,15'
        or interests like '%,15,%'
    )

    union

    SELECT '99',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests like '99'
        or interests like '99,%'
        or interests like '%,99'
        or interests like '%,99,%'
    )

    union

    SELECT 'na',count(distinct [user_id])
    FROM [mozone_user].[dbo].ProfilePersonal with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        interests is null
    )
    
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)



    
    key='age'
    
    db=''
    sql=r"""
    
    SELECT ltrim(rtrim(str(datediff(year,[birthday],getdate())))),count(distinct user_id)
      FROM [mozone_user].[dbo].[Profile]
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and [birthday] is not null
    group by datediff(year,[birthday],getdate())

    union

    SELECT 'na',count(distinct user_id)
      FROM [mozone_user].[dbo].[Profile]
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and [birthday] is null
        
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)




    
    key='language'
    
    db=''
    sql=r"""
    
    SELECT '1',count(distinct user_id)
      FROM [mozone_user].[dbo].[ProfilePersonal]
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        language_ability like '1'
        or language_ability like '1,%'
        or language_ability like '%,1'
        or language_ability like '%,1,%'
    ) and language_ability is not null

    union

    SELECT '44',count(distinct user_id)
      FROM [mozone_user].[dbo].[ProfilePersonal]
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and (
        language_ability like '44'
        or language_ability like '44,%'
        or language_ability like '%,44'
        or language_ability like '%,44,%'
    ) and language_ability is not null
        
    union

    SELECT 'na',count(distinct user_id)
      FROM [mozone_user].[dbo].[ProfilePersonal]
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    ) and language_ability is null
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)






    
    key='privacy'
    
    db=''
    sql=r"""
    
    SELECT [privacy]
            ,count([user_id])
      FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [user_id] in (
        select [user_id]
          FROM [mozone_user].[dbo].[Profile] with(nolock)
        where [user_name] like '%@viva.bh'
    )
    group by [privacy]
    
    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_dict(config.conn_viva_bh,sql)
    print value
    for k,v in value.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k+'_'+date_today,v)






    
    key='total_user'
    
    db=''
    sql=r"""

    select count(distinct [user_id])
      FROM [mozone_user].[dbo].[Profile] with(nolock)
    where [user_name] like '%@viva.bh'

    """
    
    print 'SQL Server:'+sql
    value=helper_sql_server.fetch_scalar_int(config.conn_viva_bh,sql)
    print value
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,value)


if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_user_properties(my_date)
