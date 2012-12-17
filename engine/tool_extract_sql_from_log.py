
import glob
import re
import helper_regex


#filePath=r'E:\RoutineScripts\daily_all_mosession_ip_login.py.log'
#filePath=r'E:\RoutineScripts\log\daily_stc_moagent.py.2010-06-17.log'
#filePath=r'E:\RoutineScripts\daily_mozat_mosession_country.py.log'
#filePath=r'E:\RoutineScripts\daily_all_im.py.log'
#filePath=r'C:\RoutineScripts\log\daily_vodafone_client_crash.py*'
filePath=r'C:\RoutineScripts\daily_vodafone_moagent_detail.py.log'

found_sqls=[]
#re_key_sql=re.compile(r'SQL1:.*?\nSQL2:.*?\n')
re_key_sql=re.compile(r'SQL:.*?\n')

files=glob.glob(filePath)

for f in files:
    file=open(f,'r',1024*1024)
    content=file.read(-1)
    print 'file: '+f+' ('+str(len(content))+')'
    m=re.findall(re_key_sql,content)
    
    if m:
        for i in m:
            #sql_delete=helper_regex.extract(i,r'SQL1:(.*?)\n').replace('raw_data_test','raw_data_debug')
            sql_insert=helper_regex.extract(i,r'SQL:(.*?)\n')#.replace('raw_data_test','raw_data_debug')
            #found_sqls.append(sql_delete)
            found_sqls.append(sql_insert)

    file.close()

for k in found_sqls:
    print k+';'
        
    







