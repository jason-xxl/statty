from stat_sql import Stat_sql
from threading import Thread
import glob
import time
from datetime import datetime
import config
import helper_regex
import urllib
import copy
import heapq
import math
import codecs


class Stat_plan(Thread):

    def __init__(self,plan_name='',encode_exception_treatment=None):
        self.final_stat_sqls=[]
        self.stat_sqls=[]
        self.log_sources=[] # full url with wildcast
        self.url_sources=[] # full url without wildcast
        self.raw_content_sources=[] # full url without wildcast
        self.plan_name=plan_name
        self.stop_condition=None
        self.current_file=None
        self.stat_brunch_filters=[] #filter set dict
        self.stat_mode='normal' # normal|merged_log_by_timeline
        self.encode_exception_treatment=encode_exception_treatment
       
    def set_stat_mode(self,stat_mode='merged_log_by_timeline'):
        self.stat_mode='merged_log_by_timeline' if stat_mode=='merged_log_by_timeline' else 'normal'
        
    def add_stat_sql(self,stat_sql):
        self.stat_sqls.append(stat_sql)

    def add_log_source(self,log_source):
        print 'add_log_source: ',log_source
        self.log_sources.append(log_source)

    def add_log_source_list(self,log_source_list):
        for i in log_source_list:
            print 'add_log_source_list: ',i
            self.add_log_source(i)
 
    def add_url_sources(self,url):
        self.url_sources.append(url)
 
    def add_raw_content_source(self,content):
        self.raw_content_sources.append(content)

    def dump_sources(self):
        print "log_sources:"+str(self.log_sources)
        print "url_sources :"+str(self.url_sources)
        print "raw_content_sources:"+str(self.raw_content_sources)

    def add_stat_brunch_filters(self,filters_dict): # once a filter set added, the original stat_sqls are cloned with that filter set
        self.stat_brunch_filters.append(filters_dict)

    def generate_final_stat_sqls(self):
        self.final_stat_sqls=[]
        
        for i in self.stat_sqls:
            self.final_stat_sqls.append(i)
            for filter_set in self.stat_brunch_filters:
                sql=copy.deepcopy(i)
                sql.add_hidden_where(filter_set)
                self.final_stat_sqls.append(sql)

    def run(self):
        print 'Stat Mode: '+self.stat_mode

        if self.stat_mode=='merged_log_by_timeline':
            self._run_in_merged_log_by_timeline_mode()
        else:
            self._run_in_normal_mode()
     
        
    def _run_in_normal_mode(self):
        self.line_processed=0

        self.start_time=time.time()
        print 'Stat_plan('+self.plan_name+') started at: '+datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')

        self.generate_final_stat_sqls()
        
        for stat_sql in self.final_stat_sqls:
            stat_sql.reset()
        
        for log_source in self.log_sources:
            if isinstance(log_source, list):
                logFiles=log_source[:]
            else:
                logFiles=glob.glob(log_source)
            
            print logFiles
            
            for log_file_name in logFiles:
                self.current_file=log_file_name
                
                print 'load file: '+log_file_name
                log_file=codecs.open(log_file_name,'r','utf-8',self.encode_exception_treatment,1024*1024*16)

                for line in log_file:
                    #print line
                    self.line_processed+=1
                    for stat_sql in self.final_stat_sqls:
                        stat_sql.process_line(line)

                log_file.close()            

        
        for url in self.url_sources:

            print 'url_sources:'+url
            
            self.current_file=url
            
            print 'load url: '+url
            log_file=urllib.urlopen(url)

            while True:
                line=log_file.readline()
                if not line:
                    break
                print line

                self.line_processed+=1
                for stat_sql in self.final_stat_sqls:
                    stat_sql.process_line(line)
        
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
                for stat_sql in self.final_stat_sqls:
                    stat_sql.process_line(line)

        for stat_sql in self.final_stat_sqls:
            try:
                stat_sql.calculate_result()
                stat_sql.export_result()
            except BaseException as error:
                print str(error)

        self.end_time=time.time()
        print 'Stat_plan('+self.plan_name+') ended at: '+datetime.fromtimestamp(self.end_time).strftime('%Y-%m-%d %H:%M:%S')
        print 'Stat_plan('+self.plan_name+') used: '+str(int(self.end_time-self.start_time))+' seconds'
        print 'Stat_plan('+self.plan_name+') line processed: '+str(int(self.line_processed))
        
        
    def _run_in_merged_log_by_timeline_mode(self):

        #init
        self.line_processed=0

        self.start_time=time.time()
        print 'Stat_plan('+self.plan_name+') started at: '+datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')

        self.generate_final_stat_sqls()
        
        for stat_sql in self.final_stat_sqls:
            stat_sql.reset()

        #init log source 

        log_source_file_handlers=[]

        for log_source in self.log_sources:
            if isinstance(log_source, list):
                logFiles=log_source[:]
            else:
                logFiles=glob.glob(log_source)
            
            print logFiles

            for log_file_name in logFiles:
                self.current_file=log_file_name
                
                print 'load file: '+log_file_name
                log_file=codecs.open(log_file_name,'r','utf-8',self.encode_exception_treatment,1024*1024*128)
                log_source_file_handlers.append(log_file)

        #init heap

        log_lines_heap=[]

        for i in range(0,len(log_source_file_handlers)):
            log_lines_heap.append(('1970-01-01 00:00:00,00'+str(i),i,''))

        heapq.heapify(log_lines_heap)

        #iterate heap

        while log_lines_heap:
            line_obj=heapq.heappop(log_lines_heap)
            
            self.line_processed+=1
            for stat_sql in self.final_stat_sqls:
                stat_sql.process_line(line_obj[2])

            line=log_source_file_handlers[line_obj[1]].readline()
            if line:
                t=helper_regex.extract_log_date(line)
                #print t
                heapq.heappush(log_lines_heap,(t,line_obj[1],line))

        #close
        
        for i in log_source_file_handlers:
            i.close()

        #calculate

        for stat_sql in self.final_stat_sqls:
            try:
                stat_sql.calculate_result()
                stat_sql.export_result()
            except BaseException as error:
                print str(error)

        self.end_time=time.time()
        print 'Stat_plan('+self.plan_name+') ended at: '+datetime.fromtimestamp(self.end_time).strftime('%Y-%m-%d %H:%M:%S')
        print 'Stat_plan('+self.plan_name+') used: '+str(int(self.end_time-self.start_time))+' seconds'
        print 'Stat_plan('+self.plan_name+') line processed: '+str(int(self.line_processed))
        

def p(line='',exist='',group_key=''):
    print line,
    pass

if __name__=='__main__':


    stat_plan=Stat_plan()
    stat_sql_1=Stat_sql(oem_name='Test',stat_category='chatroom', \
                        process_exist={'process_log':{
                                            'pattern':r'(.)',
                                            'process':p}})
    
    stat_plan.add_stat_sql(stat_sql_1)
    
    stat_plan.set_stat_mode('merged_log_by_timeline')


    stat_plan.add_log_source(r'\\192.168.0.117\moAgent\internal_perf.log.2012-07-15-05')

    stat_plan.add_log_source(r'\\192.168.0.118\moAgent\internal_perf.log.2012-07-15-05')

    stat_plan.add_log_source(r'\\192.168.0.75\logs_moagent_shabik_360\internal_perf.log.2012-07-15-05')

    stat_plan.add_log_source(r'\\192.168.0.107\moAgent\internal_perf.log.2012-07-15-05')

    stat_plan.add_log_source(r'\\192.168.0.108\moAgent\internal_perf.log.2012-07-15-05')

    stat_plan.add_log_source(r'\\192.168.0.195\logs_moagent_shabik_360\internal_perf.log.2012-07-15-05')

    stat_plan.add_log_source(r'\\192.168.0.185\logs_moagent_shabik_360\internal_perf.log.2012-07-15-05')

    stat_plan.add_log_source(r'\\192.168.0.196\logs_moagent_shabik_360\internal_perf.log.2012-07-15-05')

    adding_friend_log_name='E:\\RoutineScripts\\log\\Shabik_360_friend_relation_adding_friend_record_from_friendship.friendship.log.2012-07-15'
    stat_plan.add_log_source(adding_friend_log_name)

    
    stat_plan.run()    



