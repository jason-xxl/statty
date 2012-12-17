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


def export():
    
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
        
    helper_mail.send_mail(title="AIS no-login user list in 48 hours [auto-generated]",content_html='<br/>'.join(result),target_mails=['xianli@mozat.com','marie@mozat.com','taojun@mozat.com'])    

def export_unique_email():
    
    rows=helper_sql_server.fetch_rows_dict(config.conn_mozat,sql=r'''
    
    SELECT distinct [email],1
    FROM [mozone_user].[dbo].[Profile]
    where lastLogin < DATEADD(day,-2,Getdate())
    and [version_tag]='fast_ais'
    and [email] is not null and [email] <> ''
        
    ''')

    result=[]
    result.append('email')
    for k,row in rows.iteritems():
        result.append(str(row['email']))
        
    helper_mail.send_mail(title="AIS no-login unique email list in 48 hours [auto-generated]",content_html='<br/>'.join(result),target_mails=['xianli@mozat.com','marie@mozat.com','taojun@mozat.com'])    

if __name__=='__main__':

    #export()
    export_unique_email()

