import helper_regex
import helper_file

buffer_pool={}
line_count_limit_per_buffer=1000
pool_size_limit=10000

def push_line_to_storage(line,root_path,key,sub_key_1,sub_key_2):
    sub_key_2=str(sub_key_2)
    global buffer_pool,line_count_limit_per_buffer
    buffer_pool.setdefault(sub_key_2,[])
    buffer_pool[sub_key_2].append(line)
    if len(buffer_pool[sub_key_2])>=line_count_limit_per_buffer:
        path=root_path+'\\'+key+'\\'+sub_key_1
        helper_file.prepare_directory_on_windows(path)
        full_path=(path+'\\'+sub_key_2+'.txt').replace('\\\\','\\')
        helper_file.append_to_file(full_path,''.join(buffer_pool[sub_key_2]))
        buffer_pool[sub_key_2]=[]

    if len(buffer_pool)>=pool_size_limit:
        clear_buffer_pool(root_path,key,sub_key_1)


def clear_buffer_pool(root_path,key,sub_key_1):
    global buffer_pool
    print 'clear_buffer_pool:',len(buffer_pool)
    path=root_path+'\\'+key+'\\'+sub_key_1
    helper_file.prepare_directory_on_windows(path)

    for k,v in buffer_pool.iteritems():
        if v:
            full_path=(path+'\\'+k+'.txt').replace('\\\\','\\')
            helper_file.append_to_file(full_path,''.join(v))

    buffer_pool={}        
        
    


if __name__=='__main__':

    for i in range(1000):
        print i
        for j in range(1005):
            push_line_to_storage(str(j)+'abcabcabcabcabcabcabcabcabcabcabcabcabcabcabc\n','.\\','test_dir','2011-11-16',i)        
    
    #clear_buffer_pool('.\\','test_dir','2011-11-16')    
