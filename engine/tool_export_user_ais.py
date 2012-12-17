import helper_sql_server
import glob
import re
import os
import helper_regex
from helper_mysql import db
import _mysql
import helper_mysql
import helper_sql_server
import helper_file
import config
import helper_mail


def export_no_login_user():
    
    rows=helper_sql_server.fetch_rows_dict(config.conn_mozat,sql=r'''
    
    SELECT [user_id]
      ,[email]
      ,[creationDate]
      ,[lastLogin]
    FROM [mozone_user].[dbo].[Profile]
    where lastLogin < DATEADD(day,-2,Getdate())
    and [version_tag]='fast_ais'
    and [email] is not null and [email] <> ''
    order by lastLogin desc
        
    ''')

    result=[]
    result.append('user_id'+'\t'+'email'+'\t'+'creationDate'+'\t'+'lastLogin')
    for k,row in rows.iteritems():
        result.append(str(row['user_id'])+'\t'+str(row['email'])+'\t'+str(row['creationDate'])+'\t'+str(row['lastLogin']))
        
    helper_mail.send_mail(title="AIS no-login user list in 48 hours [auto-generated]",content_html='<br/>'.join(result),target_mails=['xianli@mozat.com','taojun@mozat.com','marie@mozat.com'])    

def export_all_user():
    
    rows=helper_sql_server.fetch_rows(config.conn_mozat,sql=r'''
    
    SELECT [user_id]
      ,[creationDate]
      ,replace([user_name],'@fast_ais','') as msisdn
    FROM [mozone_user].[dbo].[Profile]
    where lastLogin < '2012-05-14 01:00:00'
    and [version_tag]='fast_ais'
    --and [email] is not null and [email] <> ''
    order by creationDate asc
        
    ''')

    result=[]
    result.append('user_id'+'\t'+'creationDate'+'\t'+'msisdn')
    for row in rows:
        result.append(str(row['user_id'])+'\t'+str(row['creationDate'])+'\t'+str(row['msisdn']))
        
    helper_mail.send_mail(title="AIS all user list before 2012-05-14 01:00:00 sg [auto-generated]", \
                          content_html='<br/>'.join(result),target_mails=['seahcm@mozat.com'])    

if __name__=='__main__':

    export_no_login_user()

    #export_all_user()