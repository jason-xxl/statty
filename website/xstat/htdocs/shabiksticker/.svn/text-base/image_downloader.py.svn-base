'''
Created on Nov 2, 2012

@author: development
'''
import helper_mysql
import urllib
import os.path
_conn_sticker={
    'host':'m1-mysql-master-03.mozat.com',
    'port':'3333',
    'account':'mozone',
    'pwd':'morangerunmozone', 
    'db':'sticker',
    'db_type':'mysql',
    }

sql = 'select * from sticker.sticker'
sticker= helper_mysql.fetch_rows(sql, _conn_sticker)
print len(sticker)
for row in sticker:
    print row['id']
    print row['url']
    f_name='./%s.png'%(row['id'])
    if os.path.isfile(f_name):
        continue
    f = open(f_name,'wb')
    f.write(urllib.urlopen(row['url']).read())
    f.close()
#    break    
print 'finish'


if __name__ == '__main__':
    pass