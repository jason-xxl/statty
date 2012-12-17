import re

# [moagent] 18 Nov 00:56:43,761 - 7706880	390	15	32	15	328	3648	http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
# msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

re_map_file_name=re.compile(r'(\d\d):\d\d:\d\d',re.IGNORECASE)

target_file=r'C:\RoutineScripts\ex110305.log'
result_file_name_prefix=r'C:\RoutineScripts\ex110305.log.'

def map_file_name(line):
    m=re_map_file_name.search(line)
    if m and m.group(1):
        return result_file_name_prefix+m.group(1)
    else:
        return result_file_name_prefix+"none"


def split_log():

    result_log_file={}
    result_log_file_counter={}
    total_line_counter=0

    log_file=open(target_file,'r',1024*1024*128)
    for line in log_file:
        total_line_counter+=1

        key=map_file_name(line)
        if not result_log_file.has_key(key):
            result_log_file[key]=open(key,"w")
            result_log_file_counter[key]=0

        result_log_file[key].write(line)
        result_log_file_counter[key]+=1

    for k,v in result_log_file.iteritems():
        v.close()
        print k+': '+str(result_log_file_counter[k])


    


if __name__=='__main__':

    split_log()
