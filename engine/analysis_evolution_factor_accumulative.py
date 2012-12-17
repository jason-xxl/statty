import helper_sql_server
import helper_regex
import helper_mysql
import helper_file
import helper_math
import config
import sys

import time
from datetime import date

helper_mysql.quick_insert=True

class Analysis_evolution_factor: # based on user online/activeness history

    def __init__(self, begin_date, end_date, observation_day_length=30, observation_day_step=2, \
                 analysis_factor_name='',table_name='',oem_name='',category=''):
        self.begin_date=begin_date
        self.end_date=end_date
        self.observation_day_length=observation_day_length
        self.observation_day_step=observation_day_step


        self.analysis_factor_name=analysis_factor_name
        self.table_name=table_name
        self.oem_name=oem_name
        self.category=category
        self.temp_new_user_active_history_dict={}
        pass
    
    def _get_daily_created_user_dict(self, current_date): # return user id=>creation date
        raise NotImplementedError('NotImplementedError in _get_daily_created_user_dict')
        return set([])

    def _get_daily_active_user_set(self, current_date):
        raise NotImplementedError('NotImplementedError in _get_daily_active_user_set')
        return set([])

    def _get_daily_user_action(self, current_date):
        raise NotImplementedError('NotImplementedError in _get_daily_user_action')
        return set([])
    
    def _do_calculation(self):
        
        #generate new user set

        temp_new_user_dict={}

        temp_date=self.begin_date
        temp_end_date=self.end_date

        while True:
            if temp_date>temp_end_date:
                break
            temp_daily_new_user_set=self._get_daily_created_user_dict(temp_date)

            for user_id in temp_daily_new_user_set:
                temp_new_user_dict[user_id]=temp_date
            
            temp_date=helper_regex.date_add(temp_date,1)
            
        print len(temp_new_user_dict)
        print temp_new_user_dict
        #exit()

        #generate user active hitory

        self.temp_new_user_active_history_dict={}

        temp_date=self.begin_date
        temp_end_date=helper_regex.date_add(self.end_date,self.observation_day_length-1)

        while True:
            if temp_date>temp_end_date:
                break
            temp_active_user_set=self._get_daily_active_user_set(temp_date)
            for user_id in temp_active_user_set:
                if temp_new_user_dict.has_key(user_id):
                    self.temp_new_user_active_history_dict.setdefault(user_id,[])
                    if temp_date<=helper_regex.date_add(temp_new_user_dict[user_id],self.observation_day_length-1):
                        self.temp_new_user_active_history_dict[user_id].append(temp_date)
            
            temp_date=helper_regex.date_add(temp_date,1)
        
        print len(self.temp_new_user_active_history_dict)
        print self.temp_new_user_active_history_dict
        #exit()

        #generate user action history

        temp_new_user_action_history_dict={}

        temp_date=self.begin_date
        temp_end_date=helper_regex.date_add(self.end_date,self.observation_day_length-1)

        while True:
            if temp_date>temp_end_date:
                break
            temp_user_action_dict=self._get_daily_user_action(temp_date)
            temp_user_action_dict=dict((k,v) for k,v in temp_user_action_dict.iteritems() if k in temp_new_user_dict)
            temp_new_user_action_history_dict[temp_date]=temp_user_action_dict
            
            temp_date=helper_regex.date_add(temp_date,1)
        
        print len(temp_new_user_action_history_dict)
        print temp_new_user_action_history_dict
        #exit()

        #generate user group

        temp_user_group={}
        for user_group_by_max_active_time in range(1,self.observation_day_length+1,self.observation_day_step):
            temp_user_group[user_group_by_max_active_time]=set([user_id for user_id,history \
                                                            in self.temp_new_user_active_history_dict.iteritems() \
                                                            if len(history)>=user_group_by_max_active_time \
                                                            and len(history)<user_group_by_max_active_time \
                                                            +self.observation_day_step])

        print len(temp_user_group)
        print temp_user_group
        #exit()
        
        #generate evolution matrix

        temp_matrix_of_user_action={}                              #   Dimension-1:time    Dimension-2:user groups
        temp_matrix_of_user_online_day={}

        for active_time_period in range(1,self.observation_day_length+1,self.observation_day_step):
            for user_group_by_max_active_time in range(1,self.observation_day_length+1,self.observation_day_step):
                
                temp_matrix_of_user_action.setdefault(active_time_period,{})
                temp_matrix_of_user_action[active_time_period].setdefault(user_group_by_max_active_time,0)
                
                temp_matrix_of_user_online_day.setdefault(active_time_period,{})
                temp_matrix_of_user_online_day[active_time_period].setdefault(user_group_by_max_active_time,0)

                total_action=0
                total_online_day=0

                temp_user_set=temp_user_group[user_group_by_max_active_time]

                for user_id in temp_user_set:
                    """
                    dates=self.temp_new_user_active_history_dict[user_id][active_time_period-1:min(active_time_period+self.observation_day_step-1,len(self.temp_new_user_active_history_dict[user_id]))]
                    total_online_day+=len(dates)
                    
                    for d in dates:
                        if temp_new_user_action_history_dict[d].has_key(user_id):
                            total_action+=temp_new_user_action_history_dict[d][user_id]
                    """

                    if active_time_period>len(self.temp_new_user_active_history_dict[user_id]):
                        continue

                    dates=self.temp_new_user_active_history_dict[user_id]
                    
                    temp_begin_date=temp_new_user_dict[user_id] \
                                    if active_time_period==1 \
                                    else helper_regex.date_add(dates[active_time_period-1-1],1) # include those actions happend when user is offline

                    temp_end_date=dates[-1] \
                                  if active_time_period+self.observation_day_step-1>len(self.temp_new_user_active_history_dict[user_id]) \
                                  else dates[active_time_period+self.observation_day_step-1-1]
                    
                    for temp_d in helper_regex.date_iterator(temp_begin_date,temp_end_date):
                        if temp_new_user_action_history_dict[temp_d].has_key(user_id):
                            total_action+=temp_new_user_action_history_dict[temp_d][user_id]                        

                    total_online_day+=min(active_time_period+self.observation_day_step-1,len(self.temp_new_user_active_history_dict[user_id])) \
                                      -(active_time_period-1)

                temp_matrix_of_user_action[active_time_period][user_group_by_max_active_time]=total_action    
                temp_matrix_of_user_online_day[active_time_period][user_group_by_max_active_time]=total_online_day    

        print temp_matrix_of_user_action
        print temp_matrix_of_user_online_day
            
        #export result

        analysis_factor_name=self.analysis_factor_name      #'Mutual Friend Relation'
        table_name=self.table_name                          #'raw_data_test'
        oem_name=self.oem_name                              #'Shabik_360'
        category=self.category                              #'evolution_analysis'

        key_prefix=analysis_factor_name.lower().replace(' ','_')+'_ol%s,n%s,s%s_evolution_' % (self.observation_day_length,1+helper_regex.get_day_diff_from_date_str(self.end_date,self.begin_date),self.observation_day_step)
        date=self.begin_date
        view_name='Report %s Evolution Analysis %s (%s Days Step, %s Days)' % (oem_name,analysis_factor_name,self.observation_day_step, self.observation_day_length)
        view_description=r'''
        
        Date Range of Observed User: %s to %s
        Total Observed New User: %s
        Observing Users' First %s Days

        ''' % (self.begin_date,self.end_date,len(self.temp_new_user_active_history_dict),self.observation_day_length)

        for active_time_period,v in temp_matrix_of_user_action.iteritems():
            for user_group_by_max_active_time,total_action in v.iteritems():
                
                helper_mysql.put_raw_data(oem_name=oem_name,category=category,key=key_prefix+'unique_base', \
                                          sub_key='g'+str(user_group_by_max_active_time).zfill(2)+'_a'+str(active_time_period).zfill(2), \
                                          value=temp_matrix_of_user_action[active_time_period][user_group_by_max_active_time], \
                                          date=date,table_name=table_name)

                helper_mysql.put_raw_data(oem_name=oem_name,category=category,key=key_prefix+'unique', \
                                          sub_key='g'+str(user_group_by_max_active_time).zfill(2)+'_a'+str(active_time_period).zfill(2), \
                                          value=len(temp_user_group[user_group_by_max_active_time]), \
                                          date=date,table_name=table_name)

                adjusted_base=0
                if temp_matrix_of_user_online_day[active_time_period][user_group_by_max_active_time]>0:
                    adjusted_base=1.0*self.observation_day_step \
                                        *temp_matrix_of_user_action[active_time_period][user_group_by_max_active_time] \
                                        /temp_matrix_of_user_online_day[active_time_period][user_group_by_max_active_time]
                                        
                helper_mysql.put_raw_data(oem_name=oem_name,category=category,key=key_prefix+'unique_base_adjusted', \
                                          sub_key='g'+str(user_group_by_max_active_time).zfill(2)+'_a'+str(active_time_period).zfill(2), \
                                          value=adjusted_base,date=date,table_name=table_name)

                helper_mysql.put_raw_data(oem_name=oem_name,category=category,key=key_prefix+'total', \
                                          sub_key='g'+str(user_group_by_max_active_time).zfill(2)+'_a'+str(active_time_period).zfill(2), \
                                          value=len(self.temp_new_user_active_history_dict), \
                                          date=date,table_name=table_name)
        
        


        # generate view sql
        
        sql_template=r'''

        SELECT

        concat(
        'Online for '
        ,replace(SUBSTRING_INDEX(`sub_key`,'_',1),'g',''),
        'd-'
        ,lpad(replace(SUBSTRING_INDEX(`sub_key`,'_',1),'g','')+%(observation_day_step)s-1,2,'0')
        ,'d') as `Group Name`

        ,max(if(`oem_name`='%(oem_name)s' and category='%(category)s' and `key` = '%(key_prefix)sunique',`value`,0)) as `Group Size`

        ,concat(format(100.0
        *max(if(`oem_name`='%(oem_name)s' and category='%(category)s' and `key` = '%(key_prefix)sunique',`value`,0)) 
        /max(if(`oem_name`='%(oem_name)s' and category='%(category)s' and `key` = '%(key_prefix)stotal',`value`,0)) 
        ,2),'%%%%') as `Group Proportion`

        %%(column_sql)s

        FROM `%(table_name)s` 

        WHERE (

        `oem_name`='%(oem_name)s' and category='%(category)s' and `key` = '%(key_prefix)sunique'
        or `oem_name`='%(oem_name)s' and category='%(category)s' and `key` = '%(key_prefix)sunique_base'
        or `oem_name`='%(oem_name)s' and category='%(category)s' and `key` = '%(key_prefix)sunique_base_adjusted'
        or `oem_name`='%(oem_name)s' and category='%(category)s' and `key` = '%(key_prefix)stotal'

        )and date='%(date)s'

        GROUP BY `Group Name`
        ORDER BY `Group Name` DESC

        ''' % {
            'observation_day_step':self.observation_day_step,
            'oem_name':oem_name,
            'category':category,
            'key_prefix':key_prefix,
            'table_name':table_name,
            'date':date,
        }


        sql_column_template=r'''
        
        ,case
        when max(if(`oem_name`='%(oem_name)s' and category='%(category)s' and `key` = '%(key_prefix)sunique_base' and SUBSTRING_INDEX(`sub_key`,'_',-1)='a%(active_time_period)s',`value`,0))>0 then
            max(if(`oem_name`='%(oem_name)s' and category='%(category)s' and `key` = '%(key_prefix)sunique_base_adjusted' and SUBSTRING_INDEX(`sub_key`,'_',-1)='a%(active_time_period)s',`value`,0)) 
        else '-'
        end as `Avg %(analysis_factor_name)s Created in [%(active_time_period)sd,%(active_time_period_end)sd]`

        '''

        sql_columns=[]

        for active_time_period in range(1,self.observation_day_length+1,self.observation_day_step):
            temp_sql_column=sql_column_template % {
                'observation_day_step':self.observation_day_step,
                'oem_name':oem_name,
                'category':category,
                'key_prefix':key_prefix,
                'table_name':table_name,
                'date':date,
                'active_time_period':str(active_time_period).zfill(2),
                'active_time_period_end':str(active_time_period+self.observation_day_step-1).zfill(2),
                'analysis_factor_name':analysis_factor_name,
            }
            sql_columns.append(temp_sql_column)
            
        
        view_sql=sql_template % {
            'column_sql':'\n'.join(sql_columns)
        }

        print view_sql

        import helper_view
        helper_view.replace_view(view_name=view_name,view_sql=view_sql,view_description=view_description,charting_javascript=r'''
        
        add_highcharts_basic_line_chart({
            'tab_name':'Trend Comparison',
            'column_names_pattern':/Online /ig,
            'marginRight':300,
            'reverse_key_column':false,
            'reverse_table':true,
            'exclude_rows':['Group Size','Group Proportion'],
            'reverse_column':true
        });

        ''')

        helper_view.grant_view(view_name,'5')
        helper_view.grant_view(view_name,'17')

        pass

    def run(self):
        self._do_calculation()
        pass

        
if __name__ =='__main__':
    
    
    
    class Analysis_evolution_factor_mutual_friend(Analysis_evolution_factor):

        def _get_daily_created_user_dict(self, current_date):
            import common_shabik_360
            return common_shabik_360.get_user_ids_created_in_time_range(target_date=current_date,only_stc=True)

        def _get_daily_active_user_set(self, current_date):
            active_user_set=helper_mysql.get_raw_collection_from_key(oem_name='Shabik_360',category='moagent', \
                                    key='app_page_daily_visitor_unique',sub_key='', \
                                    date=current_date,table_name='raw_data_shabik_360',db_conn=None)
            if not active_user_set:
                raise Exception('empty active_user_set on '+current_date)
            return active_user_set

        def _get_daily_user_action(self, current_date):
            import helper_cache_sql_result
            
            start_time=helper_regex.time_add_by_hour(current_date+' 00:00:00',-3)
            end_time=helper_regex.time_add_by_hour(current_date+' 00:00:00',-3+24)
            
            sql=r'''

            select user_id,count(distinct friend_id) 
            from mozat_stat.production_copy_friendship_stc
            where modified_on>='%s' and modified_on<'%s'
            and following=1 and followed=1
            group by user_id

            ''' % (start_time,end_time)

            action_dict=helper_cache_sql_result.get_cached_sql_result_as_dict(sql=sql,db_conn=config._conn_stat_portal_142)
            
            try:
                action_dict=dict((str(k),int(v)) for k,v in action_dict.iteritems())
            except:
                print sql

            return action_dict

    analysis_evolution_factor_mutual_friend=Analysis_evolution_factor_mutual_friend(begin_date='2012-03-01', \
                                                                                  end_date='2012-03-30', \
                                                                                  observation_day_length=30, \
                                                                                  observation_day_step=3,\
                                                                                  analysis_factor_name='Mutual Friend Relation', \
                                                                                  table_name='raw_data_test', \
                                                                                  oem_name='Shabik_360', \
                                                                                  category='evolution_analysis')

    analysis_evolution_factor_mutual_friend.run()
    
    """
    #prepare
    for i in range(70):
        temp_date=helper_regex.date_add('2012-03-01',i)
        print temp_date
        print len(analysis_evolution_factor_mutual_friend._get_daily_user_action(temp_date))
    """

    pass
