import helper_sql_server
import time
import helper_regex
import config
import tool_qlx
import common_shabik_360

'''
Invitation.
'''
from tool_qlx import base, Worker

class invitation_test(base):

    def work(self):
        worker = Worker()
        
        ##### data source        
        sql = r'''
        SELECT 
           [inviter].[inviter_id] 
           
          ,[inviter].[invitee_msisdn]
          
          ,(select top 1 invitee.[monet_id] from [invitation_shabik].[dbo].[invitee] as invitee 
              where inviter.[invitee_msisdn] = invitee.[msisdn] 
              and invitee.[createdOn]>='{0}' and invitee.[createdOn]<'{1}'
           ) as invitee_id
          
          FROM [invitation_shabik].[dbo].[invitation] as inviter with(nolock)
      
          where [createdOn]>='{0}' and [createdOn]<'{1}'
        '''.format(self.begin_datetime, self.end_datetime)    
        print('SQL Server:'+sql)
        worker.addDataset('', helper_sql_server.fetch_rows(config.conn_mt,sql))


        ##### extractors
        worker.addExtractor('sent_total', lambda rows: len(list(row['inviter_id'] for row in rows)))
#        worker.addExtractor('sender_unique', lambda rows: len(set(row['inviter_id'] for row in rows)))
        worker.addExtractor('sender_unique', lambda rows: set(row['inviter_id'] for row in rows))
#        worker.addExtractor('receipient_unique', lambda rows: len(set(row['invitee_msisdn'] for row in rows)))
#        worker.addExtractor('accepted_unique', lambda rows: len(set(row['invitee_id'] for row in rows if row['invitee_id'])))
        worker.addExtractor('receipient_unique', lambda rows: set(row['invitee_msisdn'].strip() for row in rows))
        #worker.addExtractor('accepted_unique', lambda rows: set(row['invitee_id'] for row in rows if row['invitee_id']))
        worker.addExtractor('accepted_unique', lambda rows: set(row['invitee_msisdn'].strip() for row in rows if row['invitee_id']))
#        worker.addExtractor('accepted_unique', lambda rows: set(str(row['invitee_id']) +'\t'+row['invitee_msisdn'].strip() for row in rows if row['invitee_id']))
        worker.addExtractor('new_sender_unique', \
               lambda rows: len(set(str(row['invitee_id']) for row in rows) \
               & common_shabik_360.get_user_ids_created_in_time_range(target_date=self.current_date)))
        
        ##### filters
        worker.addFilter('inviter_id_mod_0', filter_=lambda data: True if data['inviter_id'] % 4 ==0 else False)
        worker.addFilter('inviter_id_mod_1', filter_=lambda data: True if data['inviter_id'] % 4 ==1 else False)
        worker.addFilter('inviter_id_mod_2', filter_=lambda data: True if data['inviter_id'] % 4 ==2 else False)
        worker.addFilter('inviter_id_mod_3', filter_=lambda data: True if data['inviter_id'] % 4 ==3 else False)
            
            
        ##### run
        results = worker.work()
        
        ##### add key suffix
        for r in results: 
            r['extractor']='daily_invite_'+r['extractor']
        print(str(results).replace('}, ','}\n'))
        
        self.save_to_db(results, key_name='extractor', sub_key_name='filter',value_name='value')
        
    def work_save_accepted_daily(self):
        # accepted invitation - invitation accepted today do not care when it is sent 
        sql = r'''
        SELECT 
        count(distinct [monet_id]) as accepted_today_unique
    
        FROM [invitation_shabik].[dbo].[invitee] with(nolock)
        
        where [monet_id] is not null
         
        and [sub_time]>='%s' and [sub_time]<'%s'
        ''' % (self.begin_datetime,self.end_datetime)
        
        print 'SQL Server:'+sql
        values=helper_sql_server.fetch_row(config.conn_mt,sql)
        print values
        rows = [dict(key='daily_invite_accepted_now_unique',sub_key='',value=values['accepted_today_unique'])]
        self.save_to_db(rows)
                
        
    def getPageView(self):
        ids = tool_qlx.get_shabik_360_url_stat(self.current_date,'stc-invitation-mobileweb.morange.com/index.aspx%')
        rows = []
        rows.append(dict(key='daily_invite_page_unique',sub_key='',value=len(ids)))
        rows.append(dict(key='daily_invite_page_unique_base',sub_key='',value=sum(ids.itervalues())))
        self.save_to_db(rows)
        
        
def main(my_date): 
    start_datetime=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_datetime=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_datetime.replace(' 05:00:00','')
    
    worker = invitation_test('raw_data_shabik_360','Shabik_360','invite',date_today,start_datetime, end_datetime)    
    worker.work()
    worker.work_save_accepted_daily()
    worker.getPageView()
        
if __name__ == '__main__':
    '''
    config.smtp_server='xx'
    config.mail_from='xx'
    config.mail_targets=['xx']
    '''
    
    track_days = 7 # track four days
    
    for i in range(config.day_to_update_stat+track_days,0,-1):
        
        my_date=time.time()-3600*24*i
        main(my_date)