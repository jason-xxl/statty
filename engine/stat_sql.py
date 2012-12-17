import helper_regex
import helper_mysql
import helper_math
import config
import operator
import sys


class Stat_sql:
    
    def __init__(self,oem_name='',stat_category='', \

                 #statistics functions
                 select_count_exist={}, 
                 select_count_distinct={}, # all select_count_distinct will all be reformed to select_count_distinct_collection
                 select_sum={},
                 select_average={},
                 select_span_average={}, #select_span_average={'page_response_time':{'key':r'BRe(?:q|p)\t\d+\t(\d+)','value':r'(\d+)(?:\s+(?:\d+\.){3}(?:\d+))?\s*$','sec_group_key_on_value':helper_regex.log10_moperf_span_level}}, \
                 select_sum_top={}, #select_sum_top={'top_max_100':{'key':r'\d{2}: (\d+)','value':get_data_usage_mosession_refined,'limit':100}} \
                 select_sum_average={}, #select_sum_average={'daily_online':{'key':r'\d{2}: (\d+)','value':get_online_time_mosession_refined},'daily_online_distribution':{'key':r'\d{2}: (\d+)','value':get_online_time_mosession_refined,'sec_group_key':get_online_time_level_mosession}} \
                 select_max={}, 
                 select_first_text_value={},  #select first line in group
                 select_first_int_value={},  #select first line in group

                 #collection comparison 
                 #elements should not contain comma or be empty
                 select_retain_rate_by_date={}, 
                 #select_retain_rate_by_date={'monet_id':{'key':r'monetid=(\d+)','date_units':[7],'with_average_life_cycle':True}}
                 select_count_distinct_collection={},
                 #select_count_distinct_collection={'monet_id':{'key':r'monetid=(\d+)','date_units':[7,'weekly','monthly']}}
                
                 #non statistics functions [iterate each row, ignoring grouping]
                 #select_text_value={}, 
                 #select_int_value={}, 

                 #external process functions, unlike other stat function, they have no output
                 process_exist={}, 
                 
                 where={}, 
                 where_not={}, 
                 
                 hidden_where={}, 
                 
                 group_by={}, 
                 
                 db_name='raw_data',
                 target_stat_portal_db_conn=None):

        self.select_count_exist=select_count_exist
        self.select_count_distinct=select_count_distinct
        self.select_count_distinct_collection=select_count_distinct_collection
        self.select_sum=select_sum
        self.select_average=select_average
        self.select_sum_top=select_sum_top
        self.select_sum_average=select_sum_average
        self.select_first_text_value=select_first_text_value
        self.select_first_int_value=select_first_int_value
        #self.select_text_value=select_text_value
        #self.select_int_value=select_int_value
        self.select_max=select_max
        self.process_exist=process_exist
        self.select_span_average=select_span_average
        self.select_retain_rate_by_date=select_retain_rate_by_date
        
        self.where=where
        self.where_not=where_not
        
        self.hidden_where=hidden_where
        
        self.group_by=group_by
        
        self.oem_name=oem_name
        self.stat_category=stat_category
        self.final_stat_category=''
        
        self.db_name=db_name

        self.target_stat_portal_db_conn=target_stat_portal_db_conn
        self.original_stat_portal_db_conn=-1

        self._calculate_prefix()

    def _convert_stat_portal_db_conn(self):
        if self.target_stat_portal_db_conn:
            self.original_stat_portal_db_conn=config.conn_stat_portal
            config.conn_stat_portal=self.target_stat_portal_db_conn
            #print '_convert_stat_portal_db_conn:',self.original_stat_portal_db_conn,config.conn_stat_portal

    def _revert_stat_portal_db_conn(self):
        if self.target_stat_portal_db_conn:
            config.conn_stat_portal=self.original_stat_portal_db_conn
            self.original_stat_portal_db_conn=-1
            #print '_revert_stat_portal_db_conn:',self.original_stat_portal_db_conn,config.conn_stat_portal

    def _calculate_prefix(self):

        key_prefix_1=helper_regex.merge_keys_sorted(self.where)#'_'.join(self.where.keys())
        if key_prefix_1:
            key_prefix_1+='_'

        key_prefix_2=helper_regex.merge_keys_sorted(self.where_not)#'_'.join(self.where_not.keys())
        if key_prefix_2:
            key_prefix_2+='_'

        key_prefix_3=helper_regex.merge_keys_sorted(self.group_by)#'_'.join(self.group_by.keys())
        if key_prefix_3:
            key_prefix_3+='_'

        self.key_prefix=key_prefix_1+key_prefix_2+key_prefix_3        
#        self.key_prefix = helper_regex.regex_replace('_+','_',self.key_prefix)

    # used by stat_plan for filter based stat brunching
    def add_hidden_where(self,where_dict):
        for k,v in where_dict.iteritems():
            self.hidden_where[k]=v

    def _rewrite_category_name(self):
        self.final_stat_category=self.stat_category
        if self.hidden_where:
            hidden_where_postfix=helper_regex.merge_keys_sorted(self.hidden_where)
            self.final_stat_category=self.stat_category+'_'+hidden_where_postfix
    
    def reset(self):
                
        self.total_lines=0
        self.processed_lines=0
        self.passed_lines=0

        # _stat_type[stat_name][group_key]=value
        # group_key=0 , ...

        self._rewrite_category_name()
    
        #select_count_exist
        self.result_select_count_exist={}
        for k,v in self.select_count_exist.iteritems():
            self.result_select_count_exist[k]={}
        
        #process_exist
        self.result_process_exist={}
        for k,v in self.process_exist.iteritems():
            self.result_process_exist[k]={}
            
        #select_count_distinct
        self.result_select_count_distinct_temp={}
        self.result_select_count_distinct_total={}
        self.result_select_count_distinct_average={}
        for k,v in self.select_count_distinct.iteritems():
            self.result_select_count_distinct_temp[k]={}
            self.result_select_count_distinct_total[k]={}
            self.result_select_count_distinct_average[k]={}

        self.result_select_count_distinct={}

        #select_count_distinct_collection
        self.result_select_count_distinct_collection_temp={}
        self.result_select_count_distinct_collection_total={}
        self.result_select_count_distinct_collection_average={}
        for k,v in self.select_count_distinct_collection.iteritems():
            #reform select_count_distinct_collection
            if not isinstance(v,dict):
                self.select_count_distinct_collection[k]={'key':v,'date_units':['weekly','monthly'],'with_value':True}
            self.result_select_count_distinct_collection_temp[k]={}
            self.result_select_count_distinct_collection_total[k]={}
            self.result_select_count_distinct_collection_average[k]={}

        self.result_select_count_distinct_collection={}
        self.result_select_count_distinct_collection_id={}

        #select_retain_rate_by_date
        self.result_select_retain_rate_by_date_temp={}
        for k,v in self.select_retain_rate_by_date.iteritems():
            self.result_select_retain_rate_by_date_temp[k]={}

        self.result_select_retain_rate_by_date={}
        
        #select_sum
        self.result_select_sum={}
        for k,v in self.select_sum.iteritems():
            self.result_select_sum[k]={}
        self.result_select_sum_count={}
        for k,v in self.select_sum.iteritems():
            self.result_select_sum_count[k]={}

        #select_sum_top
        self.result_select_sum_top_temp={}
        self.result_select_sum_top={}
        for k,v in self.select_sum_top.iteritems():
            self.result_select_sum_top_temp[k]={}
            self.result_select_sum_top[k]={}

        #select_max
        self.result_select_max={}
        for k,v in self.select_max.iteritems():
            self.result_select_max[k]={}
        self.result_select_max_count={}
        for k,v in self.select_max.iteritems():
            self.result_select_max_count[k]={}
            
        #select_sum_average
        self.result_select_sum_average_temp={}
        self.result_select_sum_average={}
        self.result_select_sum_average_seconary_grouping={}
        self.result_select_sum_average_seconary_grouping_sum={}
        self.result_select_sum_average_seconary_grouping_count={}
        for k,v in self.select_sum_average.iteritems():
            self.result_select_sum_average_temp[k]={}
            self.result_select_sum_average[k]={}
            self.result_select_sum_average_seconary_grouping[k]={}
            self.result_select_sum_average_seconary_grouping_sum[k]={}
            self.result_select_sum_average_seconary_grouping_count[k]={}

        #select_average
        self.result_select_average_sum={}
        self.result_select_average_count={}
        self.result_select_average={}
        for k,v in self.select_average.iteritems():
            self.result_select_average_sum[k]={}
            self.result_select_average_count[k]={}
            self.result_select_average[k]={}

        #select_span_average
        self.result_select_span_average={}
        self.result_select_span_average_start_value={}
        self.result_select_span_average_start_value_unmatched={}
        self.result_select_span_average_sum={}
        self.result_select_span_average_count={}
        for k,v in self.select_span_average.iteritems():
            self.result_select_span_average[k]={}
            self.result_select_span_average_start_value[k]={}
            self.result_select_span_average_start_value_unmatched[k]={}
            self.result_select_span_average_sum[k]={}
            self.result_select_span_average_count[k]={}
        
        #select_first_text_value_key
        self.result_select_first_text_value_key_temp={}
        for k,v in self.select_first_text_value.iteritems():
            self.result_select_first_text_value_key_temp[k]={}
        
        #select_first_int_value_key
        self.result_select_first_int_value_key_temp={}
        for k,v in self.select_first_int_value.iteritems():
            self.result_select_first_int_value_key_temp[k]={}
        
        self.group_by_order=self.group_by.keys()
        self.group_by_order.sort()
    
    def process_line(self,line):
        
        #TODO: handle over flow of any sum value

        self.total_lines+=1

        # where
        for k,v in self.where.iteritems():
            if callable(v):
                if not v(line):
                    self.passed_lines+=1
                    return
            else:
                if not helper_regex.extract(line,v):
                    self.passed_lines+=1
                    return
            
        # where not
        for k,v in self.where_not.iteritems():
            if callable(v):
                if v(line):
                    self.passed_lines+=1
                    return
            else:
                if helper_regex.extract(line,v):
                    self.passed_lines+=1
                    return

        # hidden where
        for k,v in self.hidden_where.iteritems():
            if callable(v):
                if not v(line):
                    self.passed_lines+=1
                    return
            else:
                if not helper_regex.extract(line,v):
                    self.passed_lines+=1
                    return
                
        self.processed_lines+=1
        
        # group_by
        group_key=''
        for k in self.group_by_order:
            if group_key:
                if callable(self.group_by[k]):
                    group_key+='_'+self.group_by[k](line)
                else:
                    group_key+='_'+helper_regex.extract(line,self.group_by[k])
            else:
                if callable(self.group_by[k]):
                    group_key=self.group_by[k](line)
                else:
                    group_key=helper_regex.extract(line,self.group_by[k])

        #select_count_exist
        for k,v in self.select_count_exist.iteritems():
            if callable(v):
                exist=v(line)
            else:
                exist=helper_regex.extract(line,v)
                
            if exist:
                #print line
                self.result_select_count_exist[k].setdefault(group_key,0)
                self.result_select_count_exist[k][group_key]+=1
                
        #process_exist
        for k,v in self.process_exist.iteritems():
            if callable(v['pattern']):
                exist=v['pattern'](line)
            else:
                exist=helper_regex.extract(line,v['pattern'])
                
            if exist:
                self.result_process_exist[k].setdefault(group_key,0)
                self.result_process_exist[k][group_key]+=1
                try:
                    v['process'](line=line,exist=exist,group_key=group_key)
                except Exception as e:
                    print e
                

        #select_count_distinct
        for k,v in self.select_count_distinct.iteritems():
            if callable(v):
                distinct_key=v(line)
            else:
                distinct_key=helper_regex.extract(line,v)
                
            if distinct_key:
                self.result_select_count_distinct_temp[k].setdefault(group_key,{})
                self.result_select_count_distinct_temp[k][group_key][distinct_key]=None
                
                self.result_select_count_distinct_total[k].setdefault(group_key,0)
                self.result_select_count_distinct_total[k][group_key]+=1


        #select_count_distinct_collection
        for k,v in self.select_count_distinct_collection.iteritems():
            if callable(v['key']):
                distinct_key=v['key'](line)
            else:
                distinct_key=helper_regex.extract(line,v['key'])
                
            if distinct_key:
                self.result_select_count_distinct_collection_temp[k].setdefault(group_key,{})
                self.result_select_count_distinct_collection_temp[k][group_key].setdefault(distinct_key,0)
                self.result_select_count_distinct_collection_temp[k][group_key][distinct_key]+=1

                self.result_select_count_distinct_collection_total[k].setdefault(group_key,0)
                self.result_select_count_distinct_collection_total[k][group_key]+=1
        


        #select_retain_rate_by_date
        for k,v in self.select_retain_rate_by_date.iteritems():
            if callable(v['key']):
                distinct_key=v['key'](line)
            else:
                distinct_key=helper_regex.extract(line,v['key'])
                
            if distinct_key:
                self.result_select_retain_rate_by_date_temp[k].setdefault(group_key,set([]))
                self.result_select_retain_rate_by_date_temp[k][group_key].add(distinct_key)

                
        #select_sum    
        for k,v in self.select_sum.iteritems():
            if callable(v):
                sum_value=v(line)
            else:
                sum_value=helper_regex.extract(line,v)

            if sum_value is not None and (isinstance(sum_value, (int, long, float, complex)) or len(sum_value))>0:
                sum_value=float(sum_value)
                self.result_select_sum[k].setdefault(group_key,0)
                self.result_select_sum[k][group_key]+=sum_value
        
                self.result_select_sum_count[k].setdefault(group_key,0)
                self.result_select_sum_count[k][group_key]+=1
                

        #select_max
        for k,v in self.select_max.iteritems():
            if callable(v):
                value=v(line)
            else:
                value=helper_regex.extract(line,v)

            if value is not None and (isinstance(value, (int, long, float, complex)) or len(value))>0:
                value=float(value)
                self.result_select_max[k].setdefault(group_key,0)
                    
                if value>self.result_select_max[k][group_key]:
                    self.result_select_max[k][group_key]=value
                    
                self.result_select_max_count[k].setdefault(group_key,0)
                self.result_select_max_count[k][group_key]+=1

                
        #select_average
        for k,v in self.select_average.iteritems():
            if callable(v):
                sum_value=v(line)
            else:
                sum_value=helper_regex.extract(line,v)
                if len(sum_value)==0:
                    continue
            
            if sum_value is None:
                continue

            sum_value=float(sum_value)

            #print 'sum_value:'+str(sum_value)
            #print line
            
            self.result_select_average_sum[k].setdefault(group_key,0)
            self.result_select_average_sum[k][group_key]+=sum_value
    
            self.result_select_average_count[k].setdefault(group_key,0)
            self.result_select_average_count[k][group_key]+=1
        
        #select_span_average sum count
        #note that group key may change in this stat method

        for k,v in self.select_span_average.iteritems():
            if callable(v['key']):
                key_value=v['key'](line)
            else:
                key_value=helper_regex.extract(line,v['key'])
                
            if callable(v['value']):
                value_value=v['value'](line)
            else:
                value_value=helper_regex.extract(line,v['value'])

            if value_value=='':
                continue

            value_value=float(value_value)

            if not self.result_select_span_average_start_value[k].has_key(key_value):
                if v.has_key('sec_group_key_on_log') and v['sec_group_key_on_log']:
                    if callable(v['sec_group_key_on_log']):
                        sec_group_key_on_log=v['sec_group_key_on_log'](line)
                    else:
                        sec_group_key_on_log=helper_regex.extract(v['sec_group_key_on_log'],line)
                else:
                    sec_group_key_on_log=None

                self.result_select_span_average_start_value[k][key_value]=[value_value,group_key,sec_group_key_on_log]

            else:
                real_value=value_value-self.result_select_span_average_start_value[k][key_value][0]

                select_span_average_group_key=group_key
                
                if v.has_key('sec_group_key_on_log') and v['sec_group_key_on_log']:
                    sec_group_key_on_log=self.result_select_span_average_start_value[k][key_value][2]
                    if sec_group_key_on_log:
                        if group_key:
                            select_span_average_group_key=group_key+'_'+str(sec_group_key_on_log)
                        else:
                            select_span_average_group_key=str(sec_group_key_on_log)
                
                if v.has_key('sec_group_key_on_value') and v['sec_group_key_on_value']:
                    if callable(v['sec_group_key_on_value']):
                        sec_group_key_on_value=v['sec_group_key_on_value'](real_value)
                    else:
                        sec_group_key_on_value=helper_regex.extract(v['sec_group_key_on_value'],real_value)

                    if sec_group_key_on_value:
                        if select_span_average_group_key:
                            select_span_average_group_key=select_span_average_group_key+'_'+str(sec_group_key_on_value)
                        else:
                            select_span_average_group_key=str(sec_group_key_on_value)

                del self.result_select_span_average_start_value[k][key_value]

                if not self.result_select_span_average_sum[k].has_key(select_span_average_group_key):
                    self.result_select_span_average[k][select_span_average_group_key]=0
                    self.result_select_span_average_sum[k][select_span_average_group_key]=0
                    self.result_select_span_average_count[k][select_span_average_group_key]=0

                self.result_select_span_average_sum[k][select_span_average_group_key] += real_value
                self.result_select_span_average_count[k][select_span_average_group_key] +=1

            
        #select_sum_top sum count

        for k,v in self.select_sum_top.iteritems():
            if callable(v['key']):
                key_value=v['key'](line)
            else:
                key_value=helper_regex.extract(line,v['key'])
                
            if callable(v['value']):
                value_value=v['value'](line)
            else:
                value_value=helper_regex.extract(line,v['value'])

            if value_value=='' or key_value=='':
                continue

            value_value=float(value_value)

            self.result_select_sum_top_temp[k].setdefault(group_key,{})
            self.result_select_sum_top_temp[k][group_key].setdefault(key_value,0)
            self.result_select_sum_top_temp[k][group_key][key_value]+=value_value

                        
        #select_sum_average sum count

        for k,v in self.select_sum_average.iteritems():
            if callable(v['key']):
                key_value=v['key'](line)
            else:
                key_value=helper_regex.extract(line,v['key'])
                
            if callable(v['value']):
                value_value=v['value'](line)
            else:
                value_value=helper_regex.extract(line,v['value'])

            if value_value=='' or key_value=='':
                continue

            value_value=float(value_value)

            self.result_select_sum_average_temp[k].setdefault(group_key,{})
            self.result_select_sum_average_temp[k][group_key].setdefault(key_value,0)
            self.result_select_sum_average_temp[k][group_key][key_value]+=value_value


        if self.select_first_text_value or self.select_first_int_value: # or self.select_text_value or self.select_int_value:
            self._convert_stat_portal_db_conn()

            #select_first_text_value
            for k,v in self.select_first_text_value.iteritems():
                if callable(v):
                    value_value=v(line)
                else:
                    value_value=helper_regex.extract(line,v)

                if not self.result_select_first_text_value_key_temp[k].has_key(group_key):
                    self.result_select_first_text_value_key_temp[k][group_key]=None

                    helper_mysql.put_raw_data_text(oem_name=self.oem_name,category=self.final_stat_category, \
                              key=self.key_prefix+k+'_first_text_value',sub_key=group_key,value=value_value,table_name=self.db_name)

                #print "select_first_text_value: "+str(key_value)

                            
            #select_first_int_value
            for k,v in self.select_first_int_value.iteritems():
                if callable(v):
                    value_value=v(line)
                else:
                    value_value=helper_regex.extract(line,v)
                    
                try:
                    value_value=float(value_value)
                    if not self.result_select_first_int_value_key_temp[k].has_key(group_key):
                        self.result_select_first_int_value_key_temp[k][group_key]=None
                        helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                  key=self.key_prefix+k+'_first_int_value',sub_key=group_key,value=value_value,table_name=self.db_name)
                        
                except:
                    print "Unexpected error:", sys.exc_info()[0]
                    print 'error select_first_int_value value: '+str(value_value)

                #print "select_first_int_value: "+str(key_value)

            """                
            #select text value
            for k,v in self.select_text_value.iteritems():
                if callable(v):
                    value_value=v(line)
                else:
                    value_value=helper_regex.extract(line,v)

                helper_mysql.put_raw_data_text(oem_name=self.oem_name,category=self.final_stat_category, \
                    key=self.key_prefix+k+'_text_value',sub_key=group_key,value=value_value,table_name=self.db_name)
                            
                            
            #select_int_value
            for k,v in self.select_int_value.iteritems():
                if callable(v):
                    value_value=v(line)
                else:
                    value_value=helper_regex.extract(line,v)
                    
                try:
                    value_value=float(value_value)
                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                        key=self.key_prefix+k+'_int_value',sub_key=group_key,value=value_value,table_name=self.db_name)

                except:
                    print "Unexpected error:", sys.exc_info()[0]
                    print 'error select_int_value value: '+str(value_value)
            """
            self._revert_stat_portal_db_conn()
            pass

    def calculate_result(self):

        self._convert_stat_portal_db_conn()

        print 'passed lines:'+str(self.passed_lines)
        print 'processed lines:'+str(self.processed_lines)
        
        #select_count_distinct
        for k,v in self.result_select_count_distinct_temp.iteritems():
            self.result_select_count_distinct[k]={}
            for kg,vg in v.iteritems():
                self.result_select_count_distinct[k][kg]=len(vg)
                if isinstance(vg, (int, long, float, complex)) or len(vg)>0:
                    self.result_select_count_distinct_average[k][kg]=float(\
                        self.result_select_count_distinct_total[k][kg]) \
                        /self.result_select_count_distinct[k][kg]
                else:
                    self.result_select_count_distinct_average[k][kg]=0
        
        #select_count_distinct_collection
        for k,v in self.result_select_count_distinct_collection_temp.iteritems():
            self.result_select_count_distinct_collection_id[k]={}
            self.result_select_count_distinct_collection[k]={}
            for kg,vg in v.iteritems():
                if self.select_count_distinct_collection[k].get('with_value',False):
                    self.result_select_count_distinct_collection_id[k][kg]=helper_mysql.put_raw_collection_with_value(vg)
                else:
                    self.result_select_count_distinct_collection_id[k][kg]=helper_mysql.put_raw_collection(vg)

                self.result_select_count_distinct_collection[k][kg]=len(vg)
                if isinstance(vg, (int, long, float, complex)) or len(vg)>0:
                    self.result_select_count_distinct_collection_average[k][kg]=float(\
                        self.result_select_count_distinct_collection_total[k][kg]) \
                        /self.result_select_count_distinct_collection[k][kg]
                else:
                    self.result_select_count_distinct_collection_average[k][kg]=0

        #select_retain_rate_by_date
        for k,v in self.result_select_retain_rate_by_date_temp.iteritems():
            self.result_select_retain_rate_by_date[k]={}
            for kg,vg in v.iteritems():
                self.result_select_retain_rate_by_date[k][kg]=helper_mysql.put_raw_collection(vg)
                self.result_select_retain_rate_by_date_temp[k][kg]=None
                
        #select_average_sum                    
        for k,v in self.result_select_average_sum.iteritems():
            for kg,vg in v.iteritems():
                if self.result_select_average_count[k].has_key(kg) and self.result_select_average_count[k][kg]>0:
                    self.result_select_average[k][kg]=float(self.result_select_average_sum[k][kg])/self.result_select_average_count[k][kg]
                else:
                    self.result_select_average[k][kg]=0
        
        #select_span_average
        for k,v in self.result_select_span_average.iteritems():
            for kg,vg in v.iteritems():
                if self.result_select_span_average_count[k].has_key(kg) and self.result_select_span_average_count[k][kg]>0:
                    self.result_select_span_average[k][kg]=float(self.result_select_span_average_sum[k][kg])/self.result_select_span_average_count[k][kg]
                    #print 're:',self.result_select_span_average_sum[k][kg],self.result_select_span_average_count[k][kg],self.result_select_span_average[k][kg]
                else:
                    self.result_select_span_average[k][kg]=0

        #select_span_average
        for k,v in self.result_select_span_average_start_value.iteritems():
            for kg,vg in v.iteritems():
                if not self.result_select_span_average_start_value_unmatched[k].has_key(vg[1]):
                    self.result_select_span_average_start_value_unmatched[k][vg[1]]=0
                self.result_select_span_average_start_value_unmatched[k][vg[1]]+=1

        #select_sum_top
        for k,v in self.result_select_sum_top_temp.iteritems():
            limit=50
            if self.select_sum_top[k].has_key('limit'):
                limit=int(self.select_sum_top[k]['limit'])
                
            for kg,vg in v.iteritems():
                self.result_select_sum_top[k][kg]=sorted(vg.iteritems(), key=operator.itemgetter(1))[-1:-(limit+1):-1]
            
        #select_sum_average
        for k,v in self.result_select_sum_average_temp.iteritems():
            for kg,vg in v.iteritems():
                if len(vg)==0:
                    self.result_select_sum_average[k][kg]=0
                else:
                    self.result_select_sum_average[k][kg]=sum(vg.values()) / len(vg)

            if self.select_sum_average[k].has_key('sec_group_key'):

                #calculate secondary group key

                group_func=self.select_sum_average[k]['sec_group_key']
                
                self.result_select_sum_average_seconary_grouping[k]={}
                self.result_select_sum_average_seconary_grouping_sum[k]={}
                self.result_select_sum_average_seconary_grouping_count[k]={}
                
                for kg,vg in v.iteritems():
                    if isinstance(vg, (int, long, float, complex)) or len(vg)>0:
                        for id,value in vg.iteritems():
                            if callable(group_func):
                                sec_group_key=group_func(value)
                            else:
                                sec_group_key=helper_regex.extract(value,group_func)
                                
                            group_key='_'.join([str(kg),str(sec_group_key)])
                            
                            if not self.result_select_sum_average_seconary_grouping[k].has_key(group_key):
                                print 'sec_group_key:'+group_key
                                self.result_select_sum_average_seconary_grouping[k][group_key]=0
                                self.result_select_sum_average_seconary_grouping_sum[k][group_key]=0
                                self.result_select_sum_average_seconary_grouping_count[k][group_key]=0                            

                            self.result_select_sum_average_seconary_grouping_sum[k][group_key]+=value                        
                            self.result_select_sum_average_seconary_grouping_count[k][group_key]+=1

                for kg,vg in self.result_select_sum_average_seconary_grouping[k].iteritems():
                    if self.result_select_sum_average_seconary_grouping_count[k][kg]>0:
                        self.result_select_sum_average_seconary_grouping[k][kg]= self.result_select_sum_average_seconary_grouping_sum[k][kg] \
                             / self.result_select_sum_average_seconary_grouping_count[k][kg]
        
        self._revert_stat_portal_db_conn()

    def export_result(self):
        
        self._convert_stat_portal_db_conn()

        print 'original stat portal connection: ',self.original_stat_portal_db_conn
        print 'current stat portal connection: ',self.target_stat_portal_db_conn

        #select_count_exist
        for k,v in self.result_select_count_exist.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_count',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_count_exist: "+repr(self.result_select_count_exist)
        
        #process_exist
        print "result_process_exist: "+repr(self.result_process_exist)
        
        #select_sum
        for k,v in self.result_select_sum.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_sum',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_sum: "+repr(self.result_select_sum)
            
        for k,v in self.result_select_sum_count.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_sum_count',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_sum_count: "+repr(self.result_select_sum_count)
        
        #select_max
        for k,v in self.result_select_max.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_max',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_max: "+repr(self.result_select_max)
        
        for k,v in self.result_select_max_count.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_max_count',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_max_count: "+repr(self.result_select_max_count)
        
        #select_average
        for k,v in self.result_select_average.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_average',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_average: "+repr(self.result_select_average)
        
        for k,v in self.result_select_average_count.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_average_base',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_average_base: "+repr(self.result_select_average_count)
        
        #select_span_average
        for k,v in self.result_select_span_average.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_span_average',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_span_average: "+repr(self.result_select_span_average)
        
        for k,v in self.result_select_span_average_count.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_span_average_base',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_span_average_count: "+repr(self.result_select_span_average_count)

        for k,v in self.result_select_span_average_start_value_unmatched.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_span_average_unmatched',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_span_average_start_value_unmatched: "+repr(self.result_select_span_average_start_value_unmatched)

        #select_sum_top
        for k,v in self.result_select_sum_top.iteritems():
            for kg,vg in v.iteritems():
                for i in range(1,len(vg)+1):
                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_sum_top_key',sub_key=kg+'_'+str(i),value=int(vg[i-1][0]), \
                                              table_name=self.db_name)
                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_sum_top_value',sub_key=kg+'_'+str(i),value=float(vg[i-1][1]), \
                                              table_name=self.db_name)
        
        print "result_select_sum_top: "+repr(self.result_select_sum_top)

        #select_sum_average
        for k,v in self.result_select_sum_average.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                          key=self.key_prefix+k+'_sum_average',sub_key=kg,value=self.result_select_sum_average[k][kg], \
                                          table_name=self.db_name)
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                          key=self.key_prefix+k+'_sum_average_base',sub_key=kg,value=len(self.result_select_sum_average_temp[k][kg]), \
                                          table_name=self.db_name)
        print "result_select_sum_average: "+repr(self.result_select_sum_average)

        for k,v in self.result_select_sum_average_seconary_grouping.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                          key=self.key_prefix+k+'_sum_average_grouped',sub_key=kg,value=self.result_select_sum_average_seconary_grouping[k][kg], \
                                          table_name=self.db_name)
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                          key=self.key_prefix+k+'_sum_average_grouped_base',sub_key=kg,value=self.result_select_sum_average_seconary_grouping_count[k][kg], \
                                          table_name=self.db_name)

        print "result_select_sum_average_seconary_grouping: "+repr(self.result_select_sum_average_seconary_grouping)
        print "result_select_sum_average_seconary_grouping_count: "+repr(self.result_select_sum_average_seconary_grouping_count)
 
        #select_count_distinct
        for k,v in self.result_select_count_distinct.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_unique',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_count_distinct: "+repr(self.result_select_count_distinct)
        
        for k,v in self.result_select_count_distinct_total.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_unique_base',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_count_distinct_total: "+repr(self.result_select_count_distinct_total)
        
        for k,v in self.result_select_count_distinct_average.iteritems():
            for kg,vg in v.iteritems():
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_unique_average',sub_key=kg,value=vg,table_name=self.db_name)
        print "result_select_count_distinct_average: "+repr(self.result_select_count_distinct_average)
        
        #select_count_distinct_collection

        for k,v in self.result_select_count_distinct_collection.iteritems():
            for kg,vg in v.iteritems():
                unique=self.result_select_count_distinct_collection[k][kg]
                total=self.result_select_count_distinct_collection_total[k][kg]
                average=self.result_select_count_distinct_collection_average[k][kg]
                collection_id=self.result_select_count_distinct_collection_id[k][kg]

                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_unique',sub_key=kg,value=unique,table_name=self.db_name)
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_unique_base',sub_key=kg,value=total,table_name=self.db_name)
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_unique_average',sub_key=kg,value=average,table_name=self.db_name)
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_unique_collection_id',sub_key=kg,value=collection_id,table_name=self.db_name)

        for key_name,value in self.select_count_distinct_collection.iteritems():
            if value.has_key('date_units'):
                for date_unit in value['date_units']:
                    k=key_name
                    v=self.result_select_count_distinct_collection[k]
                    for kg,vg in v.iteritems():
                        sub_key,date=helper_regex.extract_date(kg)
                        if date:
                            unique,total,average=helper_math.calculate_count_distinct(date_unit=date_unit,oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_unique',sub_key=sub_key,date=date,table_name=self.db_name)
                            print 'distinct collection calc '+date+': date_unit '+str(date_unit)+' unique '+str(unique)+' total '+str(total)+' average '+str(average)

                            if unique>0:
                                suffix=str(date_unit)
                                if isinstance(date_unit, (int, long)):
                                    suffix+='_days'

                                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_'+suffix+'_unique',sub_key=kg,value=unique,table_name=self.db_name)
                                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_'+suffix+'_unique_base',sub_key=kg,value=total,table_name=self.db_name)
                                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category,key=self.key_prefix+k+'_'+suffix+'_unique_average',sub_key=kg,value=average,table_name=self.db_name)

        print "result_select_count_distinct_collection: "+repr(self.result_select_count_distinct_collection)
        
        #select_retain_rate_by_date
        for k,v in self.select_retain_rate_by_date.iteritems():
            date_units=v['date_units'] if v.has_key('date_units') else [1]
            with_average_life_cycle=v['with_average_life_cycle'] if v.has_key('with_average_life_cycle') else False
                
            for kg,vg in self.result_select_retain_rate_by_date[k].iteritems():
                
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                          key=self.key_prefix+k+'_collection_id',
                                          sub_key=kg,
                                          value=self.result_select_retain_rate_by_date[k][kg], \
                                          table_name=self.db_name)
                
                helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                          key=self.key_prefix+k+'_element_count',
                                          sub_key=kg,
                                          value=helper_mysql.get_raw_collection_size_by_id(self.result_select_retain_rate_by_date[k][kg]), \
                                          table_name=self.db_name)
                
                sub_key_temp,date_temp=helper_regex.extract_date_hour_key(kg)

                for date_unit in date_units:
                    #retain rate
                    base_size,retain_rate,fresh_rate,lost_rate,retained_base_size,lost_base_size,fresh_base_size \
                                        =helper_math.calculate_date_range_retain_rate(\
                                               date_unit=date_unit, \
                                               oem_name=self.oem_name,\
                                               category=self.final_stat_category,\
                                               key=self.key_prefix+k+'_collection_id',\
                                               sub_key=sub_key_temp,\
                                               date=date_temp,\
                                               table_name=self.db_name)                

                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_'+str(date_unit)+'_day_base_size',
                                              sub_key=kg,
                                              value=base_size, \
                                              table_name=self.db_name)                

                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_'+str(date_unit)+'_day_retained_base_size',
                                              sub_key=kg,
                                              value=retained_base_size, \
                                              table_name=self.db_name)                

                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_'+str(date_unit)+'_day_lost_base_size',
                                              sub_key=kg,
                                              value=lost_base_size, \
                                              table_name=self.db_name)                

                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_'+str(date_unit)+'_day_fresh_base_size',
                                              sub_key=kg,
                                              value=fresh_base_size, \
                                              table_name=self.db_name)                

                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_'+str(date_unit)+'_day_retain_rate',
                                              sub_key=kg,
                                              value=retain_rate, \
                                              table_name=self.db_name)                

                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_'+str(date_unit)+'_day_fresh_rate',
                                              sub_key=kg,
                                              value=fresh_rate, \
                                              table_name=self.db_name)                

                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_'+str(date_unit)+'_day_lost_rate',
                                              sub_key=kg,
                                              value=lost_rate, \
                                              table_name=self.db_name)                

                    #avg life cycle
                    lost_col_average_life_cycle,retained_col_average_life_cycle,lost_col_dict,retained_col_dict=helper_math.calculate_date_range_average_life_cycle(\
                                               date_unit=date_unit, \
                                               oem_name=self.oem_name,\
                                               category=self.final_stat_category,\
                                               key=self.key_prefix+k+'_collection_id',\
                                               sub_key=sub_key_temp,\
                                               date=date_temp,\
                                               table_name=self.db_name)     
                                               
                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_'+str(date_unit)+'_day_lost_col_avg_life',
                                              sub_key=kg,
                                              value=lost_col_average_life_cycle, \
                                              table_name=self.db_name)                
                                               
                    helper_mysql.put_raw_data(oem_name=self.oem_name,category=self.final_stat_category, \
                                              key=self.key_prefix+k+'_'+str(date_unit)+'_day_retained_col_avg_life',
                                              sub_key=kg,
                                              value=retained_col_average_life_cycle, \
                                              table_name=self.db_name)                

        self._revert_stat_portal_db_conn()
