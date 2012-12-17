
import glob
import re
import helper_regex


filePath=r'E:\RoutineScripts\log\daily_all_login_service.py.2010-08-05.log'

found_keys={}
re_key_content=re.compile(r'where .*? limit 1')
re_key=re.compile(r'where (.*?`key`=".*?")')
files=glob.glob(filePath)

for f in files:
    file=open(f,'r',1024*1024)
    content=file.read(-1)
    print 'file: '+f+' ('+str(len(content))+')'
    m=re.findall(re_key_content,content)
    
    if m:
        for i in m:
            k=helper_regex.extract(i,re_key)
            if not found_keys.has_key(k):
                found_keys[k]=0
            found_keys[k]+=1

    file.close()

keys=found_keys.keys()
keys.sort()

for k in keys:
    print k
        
    







