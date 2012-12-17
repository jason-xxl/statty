from stat_sql import Stat_sql
from threading import Thread
import glob
import time
from datetime import datetime
import config
import urllib
import copy
import codecs
import helper_math
import helper_ip
import helper_mysql
import helper_file

class Stat_plan(Thread):

    def __init__(self,plan_name='',encode_exception_treatment='replace'):
        self.final_stat_sqls=[]
        self.stat_sqls=[]
        self.log_sources=[] # full url with wildcast
        self.url_sources=[] # full url without wildcast
        self.raw_content_sources=[] # full url without wildcast
        self.plan_name=plan_name
        self.stop_condition=None
        self.current_file=None
        self.stat_brunch_filters=[] #filter set dict
        self.encode_exception_treatment=encode_exception_treatment

    def add_log_source(self,log_source):
        print 'add_log_source: ',log_source
        self.log_sources.append(log_source)

    def add_log_source_list(self,log_source_list):
        for i in log_source_list:
            print 'add_log_source_list: ',i
            self.add_log_source(i)
 
    def add_url_sources(self,url):
        print 'add_url_sources: ',str(url)
        if not isinstance(url,list):
            url=[url]
        for i in url:
            self.url_sources.append(i)
 
    def add_raw_content_source(self,content):
        self.raw_content_sources.append(content)

    def dump_sources(self):
        print "log_sources:"+str(self.log_sources)
        print "url_sources:"+str(self.url_sources)
        print "raw_content_sources:"+str(self.raw_content_sources)
    
    def add_stat_sql(self,stat_sql,mixed_group_bys={},keep_original_stat=True):
        
        if not mixed_group_bys:
            self.stat_sqls.append(stat_sql) #for simple stat
        else:
            if keep_original_stat:
                self.stat_sqls.append(stat_sql) #for over-all stat
        
            # a simplified data cube alike calculation

            pattern_list_names=mixed_group_bys.keys()
            pattern_list_names.sort() # Usually the condition names are with a number prefix like '1.xxxxxx'
            
            dimensions=helper_math.get_all_perm(pattern_list_names)
            dimensions_temp=[]

            for i in range(0,len(dimensions)):
                group_by={}
                for d in dimensions[i]:
                    group_by[d]=mixed_group_bys[d]
                sql=copy.deepcopy(stat_sql)
                sql.group_by.update(group_by)
                sql._calculate_prefix()
                print sql.group_by
                
                self.stat_sqls.append(sql)

        
    def add_stat_brunch_filters(self,filters_dict,target_stat_sqls=None,excluded_stat_sqls=None):
        self.stat_brunch_filters.append((filters_dict,target_stat_sqls,excluded_stat_sqls))

    def generate_final_stat_sqls(self):
        self.final_stat_sqls=[]
        
        for i in self.stat_sqls:
            self.final_stat_sqls.append(i)
            for j in self.stat_brunch_filters:
                filter_set,target_stat_sqls,excluded_stat_sqls=j[0],j[1],j[2]
                if isinstance(target_stat_sqls,Stat_sql):
                    target_stat_sqls=[target_stat_sqls]
                if isinstance(excluded_stat_sqls,Stat_sql):
                    excluded_stat_sqls=[excluded_stat_sqls]
                if (target_stat_sqls is None or i in target_stat_sqls) and (excluded_stat_sqls is None or i not in excluded_stat_sqls):
                    sql=copy.deepcopy(i)
                    sql.add_hidden_where(filter_set)
                    self.final_stat_sqls.append(sql)

    def reset(self):
        self.script_file_name=helper_file.get_current_script_file_name()
        self.processor_ip=helper_ip.get_current_server_ip()
        self.uuid=helper_math.get_uuid()

        self.line_processed=0
        self.total_file_size=0
        self.generate_final_stat_sqls()

        self.processed_log_files=set([])
        
        for stat_sql in self.final_stat_sqls:
            stat_sql.reset()
        
        self.start_time=time.time()
        self.start_time_str=datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')
        print 'Stat_plan('+self.plan_name+') reseted/started at: '+self.start_time_str
        print 'UUID:',self.uuid

        self.log_table_name='raw_data_monitor'
        self.log_oem_name=self.script_file_name
        self.log_category=helper_ip.get_current_server_ip()+'_'+self.start_time_str.replace(' ','_').replace(':','_').replace('-','_')+'_'+self.uuid

        

    def process_line(self,line):
        for stat_sql in self.final_stat_sqls:
            stat_sql.process_line(line)

    def do_calculation(self):
        for stat_sql in self.final_stat_sqls:
            stat_sql.calculate_result()
            stat_sql.export_result()
            try:
                pass
            except BaseException as error:
                print str(error)

        self.end_time=time.time()
        print 'Stat_plan('+self.plan_name+') exported results/ended at: '+datetime.fromtimestamp(self.end_time).strftime('%Y-%m-%d %H:%M:%S')
        print 'Stat_plan('+self.plan_name+') used: '+str(int(self.end_time-self.start_time))+' seconds'
        print 'Stat_plan('+self.plan_name+') line processed: '+str(int(self.line_processed))

    def run(self):
        self.reset()
        
        for log_source in self.log_sources:
            if isinstance(log_source, list):
                logFiles=log_source[:]
            else:
                logFiles=glob.glob(log_source)
            
            print logFiles
            
            for log_file_name in logFiles:
                self.current_file=log_file_name
                
                #check reprocess error
                if log_file_name.lower() in self.processed_log_files:
                    raise Exception('reprocessing log: ',log_file_name)
                else:
                    self.processed_log_files.add(log_file_name.lower())

                #log file start
                helper_mysql.put_raw_data(oem_name=self.log_oem_name, \
                                          category=self.log_category, \
                                          key='original_file_size', \
                                          sub_key=log_file_name, \
                                          value=helper_file.get_file_size(log_file_name), \
                                          table_name=self.log_table_name)
                
                file_size=0
                line_count=0

                print 'load file: '+log_file_name

                #log_file=open(log_file_name,'r',1024*1024*128)
                log_file=codecs.open(log_file_name,'r','utf-8',self.encode_exception_treatment,1024*1024*128)
                
                #pass the file path, name to stat sql's
                self.process_line('### Stat_Sql: File Path: '+log_file_name+' ###')

                for line in log_file:
                    #print line
                    line_count+=1
                    file_size+=len(line)

                    self.process_line(line)
                    if line_count % 100000==0:
                        print 'line:',line_count

                log_file.close()    
                
                print 'file size: ',file_size
                print 'line total: ',line_count

                self.line_processed+=line_count
                self.total_file_size+=file_size
        
                #log file end
                helper_mysql.put_raw_data(oem_name=self.log_oem_name, \
                                          category=self.log_category, \
                                          key='line_processed', \
                                          sub_key=log_file_name, \
                                          value=line_count, \
                                          table_name=self.log_table_name)

                helper_mysql.put_raw_data(oem_name=self.log_oem_name, \
                                          category=self.log_category, \
                                          key='file_size_processed', \
                                          sub_key=log_file_name, \
                                          value=file_size, \
                                          table_name=self.log_table_name)

        #dump processed logs
        print 'Dump processed log file list:'
        for i in sorted(list(self.processed_log_files)):
            print i
                    

        for url in self.url_sources:

            print 'url_sources:'+url
            
            self.current_file=url
            
            print 'load url: '+url
            log_file=urllib.urlopen(url)
                
            #pass the file path, name to stat sql's
            self.process_line('### Stat_Sql: Url Path: '+url+' ###')

            while True:
                line=log_file.readline()
                if not line:
                    break
                #print line

                self.line_processed+=1
                self.process_line(line)
        
        for raw_content in self.raw_content_sources:

            print 'raw_content_sources:'+str(len(self.raw_content_sources))
            
            self.current_file=raw_content[0:10]
            
            print 'load raw content: '+raw_content

            if not raw_content:
                print 'raw content empty..'
                break

            for line in raw_content.replace('\r\n','\n').replace('\r','\n').split('\n'):
                if not line:
                    break
                self.line_processed+=1
                self.process_line(line)

        self.do_calculation()

        

if __name__=='__main__':

    # INFO 2010-04-08 00:00:00 - [          workThread] (       CliPktProcMgr.java: 640) - [doEnterChatroom]; monetId: 13022167; roomId: 1; clientType: mobile; morangeVersion: 
    # INFO 2010-05-01 00:00:02 - [          workThread] (       CliPktProcMgr.java: 252) - [send_a_msg], type: text; iMonetId: 8181192; iRoomId:70

    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        
        stat_sql_1=Stat_sql(oem_name='Test',stat_category='chatroom', \
                        select_count_distinct_collection={'monet_id':{'key':r'monetId: (\d+)','with_value':True}}, \
                        #select_count_exist={'some_action_result':r'(.)'}, \
                        #select_average={'room_id':r'roomId: (\d+)'}, \
                        #select_sum={'room_id':r'roomId: (\d+)'}, \
                        where={'enter_room':r'(\[doEnterChatroom\])'}, \
                        group_by={'day':r'INFO ([\s0-9\-]{10})'}, \
                        db_name='raw_data', \
                        target_stat_portal_db_conn=config._conn_stat_portal_158_2)

        stat_plan=Stat_plan()

        stat_plan.add_stat_sql(stat_sql_1)#,mixed_group_bys={'2.hour':r'INFO ([\s0-9\-]{13})'})#,'3.a_id':r'INFO [\s0-9\-]{12}(.)'})
        #'1.r_id':r'roomId: (\d+)',
        
        stat_plan.add_log_source(r'\\192.168.0.118\logs_chatroom_stc\service.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d') \
           +'-01')


        stat_plan.run()    

        import helper_assertion
        helper_assertion.check_stat_plan_log_file_reading_completion(stat_plan)
