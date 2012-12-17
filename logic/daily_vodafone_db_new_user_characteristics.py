import helper_sql_server
import helper_mysql
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config



def stat_login(my_date):

    oem_name='Vodafone'
    db_name='raw_data_trend'
    
    for i in range(0,56):


        stat_category='login'

        start_time=helper_regex.time_floor(my_date-3600*24*i).replace(' 00:00:00',' 06:00:00')
        end_time=helper_regex.time_ceil(my_date-3600*24*i).replace(' 00:00:00',' 06:00:00')
        date_today=start_time.replace(' 06:00:00','')

        db='mozone_user'
        key='user_sub_login_retain_trend_unique_user'
        db_name='raw_data_trend'

        sql=r'''

        SELECT 

        count([user_id]) [total]
        ,sum(case when [lastLogin] is not null then 1 else 0 end) [loginned]
        ,sum(case when [lastLogin]>=dateadd(dd,1,[creationDate]) then 1 else 0 end) as [1day]
        ,sum(case when [lastLogin]>=dateadd(dd,2,[creationDate]) then 1 else 0 end) as [2day]
        ,sum(case when [lastLogin]>=dateadd(dd,3,[creationDate]) then 1 else 0 end) as [3day]
        ,sum(case when [lastLogin]>=dateadd(dd,4,[creationDate]) then 1 else 0 end) as [4day]
        ,sum(case when [lastLogin]>=dateadd(dd,5,[creationDate]) then 1 else 0 end) as [5day]
        ,sum(case when [lastLogin]>=dateadd(dd,6,[creationDate]) then 1 else 0 end) as [6day]
        ,sum(case when [lastLogin]>=dateadd(dd,7,[creationDate]) then 1 else 0 end) as [7day]
        ,sum(case when [lastLogin]>=dateadd(dd,14,[creationDate]) then 1 else 0 end) as [14day]
        ,sum(case when [lastLogin]>=dateadd(dd,21,[creationDate]) then 1 else 0 end) as [21day]
        ,sum(case when [lastLogin]>=dateadd(dd,28,[creationDate]) then 1 else 0 end) as [28day]
        ,sum(case when [lastLogin]>=dateadd(dd,35,[creationDate]) then 1 else 0 end) as [35day]
        ,sum(case when [lastLogin]>=dateadd(dd,42,[creationDate]) then 1 else 0 end) as [42day]
        ,sum(case when [lastLogin]>=dateadd(dd,49,[creationDate]) then 1 else 0 end) as [49day]
        ,sum(case when [lastLogin]>=dateadd(dd,56,[creationDate]) then 1 else 0 end) as [56day]
        ,sum(case when [picId] is not null and [picId]>0 then 1 else 0 end) as [with_photo]

        FROM [mozone_user].[dbo].[Profile] with(nolock)
        where
        [creationDate]>='%s'
        and [creationDate]<'%s'
        and [user_name] like '%%@voda%%'

        ''' % (start_time,end_time)
        
        print 'SQL Server:'+sql
        values=helper_sql_server.fetch_row(config.conn_vodafone_88,sql)

        print values

        helper_mysql.put_raw_data(oem_name,stat_category,key,'total',values['total'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'loginned',values['loginned'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'1day',values['1day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'2day',values['2day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'3day',values['3day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'4day',values['4day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'5day',values['5day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'6day',values['6day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'7day',values['7day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'14day',values['14day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'21day',values['21day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'28day',values['28day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'35day',values['35day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'42day',values['42day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'49day',values['49day'],db_name,date_today)
        helper_mysql.put_raw_data(oem_name,stat_category,key,'56day',values['56day'],db_name,date_today)
	

        # upload photo

        stat_category='profile'
        key='user_uploaded_photo_48_hours_unique'

        helper_mysql.put_raw_data(oem_name,stat_category,key,'with_photo',values['with_photo'],db_name,date_today)

        if i<3: # only track for 3 days

            # add friend

            stat_category='friend_relation'
            key='user_sub_add_friend_48_hours_unique'
            sql=r'''

            select
            avg([friend_count]*1.0)
            from (
                SELECT 
                p.[user_id]
                ,count(f.friend_id) as [friend_count]

                FROM [mozone_user].[dbo].[Profile] p with(nolock)
                left join
                [mozone_friend].[dbo].[friendship] f with(nolock)
                on p.user_id=f.user_id

                where
                [creationDate]>='%s'
                and [creationDate]<'%s'
                and [user_name] like '%%@voda%%'
                and (f.CreatedOn is null or datediff(hour,p.creationDate,f.CreatedOn)<=48)
                and f.following=1

                group by p.user_id
            ) a

            ''' % (start_time,end_time)
            
            print 'SQL Server:'+sql
            value=helper_sql_server.fetch_scalar(config.conn_vodafone_88,sql)

            print value
            helper_mysql.put_raw_data(oem_name,stat_category,key,'',value,db_name,date_today)



            # added as friend

            stat_category='friend_relation'
            key='user_sub_added_as_friend_48_hours_unique'
            sql=r'''

            select
            avg([friend_count]*1.0)
            from (
                SELECT 
                p.[user_id]
                ,count(f.friend_id) as [friend_count]

                FROM [mozone_user].[dbo].[Profile] p with(nolock)
                left join
                [mozone_friend].[dbo].[friendship] f with(nolock)
                on p.user_id=f.friend_id

                where
                [creationDate]>='%s'
                and [creationDate]<'%s'
                and [user_name] like '%%@voda%%'
                and (f.CreatedOn is null or datediff(hour,p.creationDate,f.CreatedOn)<=48)
                and f.following=1

                group by p.user_id
            ) a

            ''' % (start_time,end_time)
            
            print 'SQL Server:'+sql
            value=helper_sql_server.fetch_scalar(config.conn_vodafone_88,sql)

            print value
            helper_mysql.put_raw_data(oem_name,stat_category,key,'',value,db_name,date_today)



            # confirmed as mutual friend

            stat_category='friend_relation'
            key='user_sub_confirmed_as_mutual_friend_48_hours_unique'
            sql=r'''

            select
            avg([friend_count]*1.0)
            from (
                SELECT 
                p.[user_id]
                ,count(f.friend_id) as [friend_count]

                FROM [mozone_user].[dbo].[Profile] p with(nolock)
                left join
                [mozone_friend].[dbo].[friendship] f with(nolock)
                on p.user_id=f.friend_id

                where
                [creationDate]>='%s'
                and [creationDate]<'%s'
                and [user_name] like '%%@voda%%'
                and (f.CreatedOn is null or datediff(hour,p.creationDate,f.CreatedOn)<=48)
                and f.following=1
                and f.followed=1

                group by p.user_id
            ) a

            ''' % (start_time,end_time)
            
            print 'SQL Server:'+sql
            value=helper_sql_server.fetch_scalar(config.conn_vodafone_88,sql)

            print value
            helper_mysql.put_raw_data(oem_name,stat_category,key,'',value,db_name,date_today)


            # add IM
            
            stat_category='im'
            key='user_sub_add_im_48_hours_unique'
            sql=r'''

            select

            sum([Account of type 1]) as [Account of type 1]
            ,sum([Account of type 2]) as [Account of type 2]
            ,sum([Account of type 3]) as [Account of type 3]
            ,sum([Account of type 10]) as [Account of type 10]

            from (
            
                SELECT
                
                max(case when [iAccountType]=1 then 1 else 0 end) as [Account of type 1]
                ,max(case when [iAccountType]=2 then 1 else 0 end) as [Account of type 2]
                ,max(case when [iAccountType]=3 then 1 else 0 end) as [Account of type 3]
                ,max(case when [iAccountType]=10 then 1 else 0 end) as [Account of type 10]

                FROM (

                    select iMonetID,iAccountType,tCreate
                    from [moim].[dbo].[moimAccount] with(nolock)
                    where tCreate>='%s'
                    
                ) as a

                inner join (

                    select user_id,creationDate
                    from [DB88].[mozone_user].[dbo].[Profile]b with(nolock)
                    where [creationDate] BETWEEN '%s' and '%s'

                ) as b

                on a.[iMonetID]=b.user_id
                where datediff(hour,tCreate,creationDate)<=48
                group by iMonetID

            ) c

            ''' % (start_time,start_time,end_time)

            print 'SQL Server:'+sql
            values=helper_sql_server.fetch_row(config.conn_vodafone_87,sql)
            print values

            helper_mysql.put_raw_data(oem_name,stat_category,key,'Account of type 1',values['Account of type 1'],db_name,date_today)
            helper_mysql.put_raw_data(oem_name,stat_category,key,'Account of type 2',values['Account of type 2'],db_name,date_today)
            helper_mysql.put_raw_data(oem_name,stat_category,key,'Account of type 3',values['Account of type 3'],db_name,date_today)
            helper_mysql.put_raw_data(oem_name,stat_category,key,'Account of type 10',values['Account of type 10'],db_name,date_today)

            # add Twitter

            stat_category='twitter'
            key='user_sub_add_twitter_48_hours_unique'
            sql=r'''

            SELECT

            count(distinct a.user_id)

            FROM (
                select user_id,created_on
                from MYSQL54...accounts 
                where created_on>='%s'
            ) as a

            inner join (
                select user_id,creationDate
                from [DB88].[mozone_user].[dbo].[Profile]b with(nolock)
                where [creationDate] BETWEEN '%s' and '%s'
            ) as b

            on a.user_id=b.user_id

            where datediff(hour,created_on,creationDate)<=48

            ''' % (start_time,start_time,end_time)
            
            print 'MySQL:'+sql
            value=helper_sql_server.fetch_scalar(config.conn_helper_db,sql)
            print values

            helper_mysql.put_raw_data(oem_name,stat_category,key,'',value,db_name,date_today)


            

            #return



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1): 
        my_date=time.time()-3600*24*i
        stat_login(my_date)
