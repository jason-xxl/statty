import helper_sql_server
import helper_mysql
import time
import helper_regex
import config
import tool_qlx
from tool_qlx import base, Worker
import StringIO
import helper_mail

'''
User generated Content. 
Operators: Shabik 360, Viva_bh, Viva
Aspects: photo, status, login, poll
send email at the end of a day or week or month. 
'''

op_shabik_360 = base(dbcon=config.conn_stat_portal, table='raw_data_shabik_360',\
                     operator='Shabik_360',service=None,current_date=None)
op_shabik_360.pid = '1'
op_shabik_360.view_name = 'Shabik 360 - User Generated Content Daily'
op_shabik_360.view_url = 'http://statportal.morange.com/xstat/view.php?id=5'


op_viva_bh = base(dbcon=config.conn_stat_portal, table='raw_data_viva_bh', \
                  operator='Viva_BH',service=None,current_date=None)
op_viva_bh.pid = '2'
op_viva_bh.view_name = 'Viva BH - User Generated Content Daily'
op_viva_bh.view_url = 'http://statportal.morange.com/xstat/view.php?id=461'
  
op_viva = base(dbcon=config.conn_stat_portal, table='raw_data_viva', \
               operator='Viva',service=None,current_date=None)
op_viva.pid = '3'
op_viva.view_name = 'Viva - User Generated Content Daily' 
op_viva.view_url = 'http://statportal.morange.com/xstat/view.php?id=112'

op_list = [op_shabik_360, op_viva_bh, op_viva]
     
     
class UserGeneratedContent(base):
    
    def stat_photo_status(self):        
        '''photo and status.
        
        They have partner id.
        
        photo upload
        photo comment
        status 
        status comment
        
        ''' 
        for partner in op_list:
            results = []
            # photo upload (index: partner_id, created_on)
            sql = '''
            SELECT 
            
            count(id) as total_count
            ,count(distinct owner) as unique_count
            
            FROM `photo`.`photo`
            
            where partner_id={p} 
            and created_on >='{s}' and created_on <'{e}' 
            '''.format(s=self.begin_datetime, e=self.end_datetime,p=partner.pid)
            print(sql)
            row = helper_mysql.fetch_row(sql, config.conn_stc_2)
            print('-----', len(row))
            print('-----', row)
            results.append(dict(key='photo_unique',sub_key='',value=row['unique_count']))
            results.append(dict(key='photo_unique_base',sub_key='',value=row['total_count']))
            
            
            # photo comment (index: partner_id,target_type, created_on)
            sql = '''
            SELECT 
             
            count(id) as total_count
            ,count(distinct target_id) as unique_count
            
            FROM `Comment`.`comment`
            
            where partner_id = {p} and target_type = 'photo'
            and created_on >='{s}' and created_on <'{e}' 
            '''.format(s=self.begin_datetime, e=self.end_datetime,p=partner.pid)
            print(sql)
            row = helper_mysql.fetch_row(sql, config.conn_stc_3)
            print('-----', len(row))
            print('-----', row)
            results.append(dict(key='photo_comment_unique',sub_key='',value=row['unique_count']))
            results.append(dict(key='photo_comment_unique_base',sub_key='',value=row['total_count']))
            
            # status (index: partner_id, created_on)
            sql = '''
            SELECT 
             
            count(id) as total_count
            ,count(distinct author) as unique_count
            
            FROM `Status`.`status`
            
            where partner_id = {p}
            and created_on >='{s}' and created_on <'{e}' 
            '''.format(s=self.begin_datetime, e=self.end_datetime,p=partner.pid)
            print(sql)
            row = helper_mysql.fetch_row(sql, config.conn_stc_1)
            print('-----', len(row))
            print('-----', row)
            results.append(dict(key='status_unique',sub_key='',value=row['unique_count']))
            results.append(dict(key='status_unique_base',sub_key='',value=row['total_count']))
            
            # status comment (index: partner_id,target_type, created_on)
            sql = '''
            SELECT 
            
            count(id) as total_count
            ,count(distinct target_id) as unique_count
            
            FROM `Comment`.`comment`
            
            where partner_id = {p} and target_type = 'status'
            and created_on >='{s}' and created_on <'{e}' 
            '''.format(s=self.begin_datetime, e=self.end_datetime,p=partner.pid)
            print(sql)
            row = helper_mysql.fetch_row(sql, config.conn_stc_3)
            print('-----', len(row))
            print('-----', row)
            results.append(dict(key='status_comment_unique',sub_key='',value=row['unique_count']))
            results.append(dict(key='status_comment_unique_base',sub_key='',value=row['total_count']))
            
            print('---',results)
            for r in results:
                r['key'] = self.sqlprefix + '_'+ r['key']
            print(str(results).replace('}, ','}\n'))
            partner.service = self.service
            partner.current_date = self.current_date
            partner.save_to_db(results)


    def stat_login_poll(self):
        '''
        no partner id, we have to manual separate them.
        
        two fields: monet_id and count
        '''
        # log in (index: date, monetid)
        # (do not care login or logout)
        sql = '''
        SELECT 
        
        monetid as monet_id
        , count(id) as `count`
        
        FROM `User`.`loginoff`
           
        where date >='{s}' and date<'{e}'
        
        group by `monetid` ########################################
        #group by `date`, `monetid` ########################################
        '''.format(s=self.begin_datetime, e=self.end_datetime)
        print(sql)
        login_rows = helper_mysql.fetch_rows(sql, config.conn_stc_login_mysql)
        print('-----', len(login_rows))
        print('-----', login_rows)
            
        # poll (index: onwer_type=12, owner_id, created_on)
        sql = '''
        select 
    
        [user_id] as monet_id
         ,count(id) as [count]
         
        from [mozone_poll].[dbo].[polls]
          
        where CreatedOn >='{s}' and CreatedOn <'{e}'
      
        group by [user_id]
        '''.format(s=self.begin_datetime, e=self.end_datetime)
        print(sql)
        poll_rows = helper_sql_server.fetch_rows(sql=sql, conn_config=config.conn_stc_121)
        print('-----', len(poll_rows))
        print('-----', poll_rows)  
  
        # poll comment by others
        sql = '''
        select
           
        [user_id] as [monet_id]
        ,count(id) as [count]
        
        from [mozone].[dbo].[comments]
        
        where owner_type=12
        and CreatedOn >='{s}' and CreatedOn <'{e}'
        
        group by [user_id]
        '''.format(s=self.begin_datetime, e=self.end_datetime)
        print(sql)
        poll_comment_rows = helper_sql_server.fetch_rows(sql=sql, conn_config=config.conn_stc_121)
        print('-----', len(poll_comment_rows))
        print('-----', poll_comment_rows)
    
        # poll comment of the owner
        sql = '''
        select
           
        [owner_id] as [monet_id]
        ,count(id) as [count]
        
        from [mozone].[dbo].[comments]
        
        where owner_type=12
        and CreatedOn >='{s}' and CreatedOn <'{e}'
        
        group by [owner_id]
        '''.format(s=self.begin_datetime, e=self.end_datetime)
        print(sql)
        poll_commented_rows = helper_sql_server.fetch_rows(sql=sql, conn_config=config.conn_stc_121)
        print('-----', len(poll_commented_rows))
        print('-----', poll_commented_rows)
        
        # all the user monet id
        monet_ids = set(str(row['monet_id']) for row in login_rows)\
                    | set(str(row['monet_id']) for row in poll_rows)\
                    | set(str(row['monet_id']) for row in poll_comment_rows)\
                    | set(str(row['monet_id']) for row in poll_commented_rows) 
        monet_id_partner_id_dict= tool_qlx.get_partner_id_dict(monet_ids,use_string_id=True)
        print('-----monet_id has_partner_id ',len(monet_ids),len(monet_id_partner_id_dict))
#        print('-----ids',monet_id_partner_id_dict)
        
        worker = Worker()
        worker.addDataset('login', login_rows)
        worker.addDataset('poll', poll_rows)
        worker.addDataset('poll_comment', poll_comment_rows)
        worker.addDataset('poll_commented', poll_commented_rows)
                            
        if '' in worker.filters: del worker.filters[''] # no global 
        worker.addFilter(op_shabik_360.operator, lambda row: monet_id_partner_id_dict.get(str(row['monet_id']),None)==op_shabik_360.pid)
        worker.addFilter(op_viva_bh.operator, lambda row: monet_id_partner_id_dict.get(str(row['monet_id']),None)==op_viva_bh.pid)
        worker.addFilter(op_viva.operator, lambda row: monet_id_partner_id_dict.get(str(row['monet_id']),None)==op_viva.pid)
#        worker.addFilter('others', lambda row: monet_id_partner_id_dict.get(str(row['monet_id']),None)==None)
        
        worker.addExtractor('unique', lambda rows: len(rows))
        worker.addExtractor('unique_base', lambda rows: sum(int(row['count']) for row in rows))
        
        results = worker.work()
        for r in results:
            r['extractor'] = self.sqlprefix + '_'+ r['dataset'] + "_" + r['extractor']
            r['sub_key'] = ''
             
        print(str(results).replace('}, ','}\n'))
        
        for operator in op_list: # save to each operator           
            res = [r for r in results if r['filter']==operator.operator]
            print('-------')
            print(str(res).replace('}, ','}\n'))
            operator.service = self.service
            operator.current_date = self.current_date
            operator.save_to_db(res, key_name='extractor', sub_key_name='sub_key', value_name='value')
        

    def send_email(self,timestamp_):
        operators = [op_shabik_360, op_viva_bh, op_viva]
        #operators = [op_shabik_360] 
         
        html = ''
        for operator in operators:
            html += '<p>'+self.create_email(timestamp_, operator)+'</p>'
#        print(html)
        helper_mail.send_mail(title='Middle East Social Statistics Report',\
                          content_html=html,target_mails=config.mail_targets_middle_east_social_stat,\
                          source_mail=config.mail_from)
            
    def create_email(self, timestamp_,operator):
        prefixes = ['daily']
        if tool_qlx.is_week_end(timestamp_=timestamp_, timezone_hours=config.timezone_offset_shabik_360):
            prefixes.append('weekly')
        if tool_qlx.is_month_end(timestamp_=timestamp_, timezone_hours=config.timezone_offset_shabik_360):
            prefixes.append('monthly')
        roots = ['login','photo','photo_comment','status','status_comment','poll','poll_commented','poll_comment']
        suffixes = ['unique','unique_base']
        
        # get result from db
        rows = []
        for prefix in prefixes:
            for root in roots:
                for suffix in suffixes:
                    key = prefix+'_'+root+'_'+suffix
                    print('----',key)
                    rows.append(dict(key=key,sub_key='',value=''))
        operator.service = self.service
        operator.current_date = self.current_date                    
        operator.load_raw_data_from_db(rows)
#        print(str(rows).replace('}, ','}\n'))
 
        # key -> value
        results = dict()
        for row in rows: 
            results[row['key']] = row['value']
#        print(str(results).replace(', ','\n'))
         
        # html
        rs = StringIO.StringIO()
        rs.write('<table border="1">')
        
        # caption
#        title = '<a href="'+operator.view_url+'">'+operator.operator.replace('_',' ').title()+'</a>'
#        rs.write('<caption>'+title+' '+self.current_date+'</caption>')
        rs.write('<caption>'+operator.operator.replace('_',' ').title()+' '+self.current_date+'</caption>')
        
        # column
        rs.write('<tr>')
        rs.write('<th>Service</th>')
        for prefix in prefixes: # column
            for suffix in suffixes:
                rs.write('<th>'+prefix.title()+' '+suffix.replace('unique_base','total').title()+'</th>')
        rs.write('</tr>\n')  
        num_columns = len(prefixes) * len(suffixes) +1
        
        # rows
        for root in roots:
            rs.write('<tr>')
            name = root.replace('_',' ').title()
            rs.write('<td>'+name+'</td>')
            for prefix in prefixes: # column
                for suffix in suffixes: 
                    rs.write('<td>'+str(results[prefix+'_'+root+'_'+suffix])+'</td>')
            rs.write('</tr>\n') 
        
        # footer
        rs.write('<tr><td align=center colspan='+str(num_columns)+'><a href="'+operator.view_url+'">'+operator.view_name+'</a></td></tr>')
        rs.write('</table>')
        
        html = rs.getvalue() 
        rs.close()
        return html
        
           
def main(my_time): 
    ''' it is in zero time zone. so 0am Singapore time corresponds to 21pm yesterday. 
    '''
    dates = dict({'daily':-1,'weekly':-7,'monthly':-tool_qlx.get_month_days(timestamp_=my_time, timezone_hours=config.timezone_offset_shabik_360)}) # must be fixed.
#    dates = dict({'daily':-1})
#    dates = dict({'weekly':-7})
#    dates = dict({'monthly':-tool_qlx.get_month_days(timestamp_=my_time, timezone_hours=config.timezone_offset_shabik_360)})
    for name, days in dates.items():
        if name == 'weekly' and not tool_qlx.is_week_end(timestamp_=my_time, timezone_hours=config.timezone_offset_shabik_360): continue
        elif name == 'monthly' and not tool_qlx.is_month_end(timestamp_=my_time, timezone_hours=config.timezone_offset_shabik_360): continue
         
        current_date = helper_regex.translate_date(sg_timestamp=my_time,timezone_offset_to_sg=config.timezone_offset_shabik_360)
        begin_datetime=helper_regex.date_add(current_date,days) +' 21:00:00'
        end_datetime=current_date+ ' 21:00:00'
        print(days, begin_datetime, end_datetime)
        
        # key prefix, daily
        ugc = UserGeneratedContent(None,None,'user_generated_content',current_date, begin_datetime, end_datetime)
        ugc.sqlprefix = name ### very important
        ugc.stat_photo_status()
        ugc.stat_login_poll()

        # send email
        # wait a 10s in case the data is not stored to the db
        if tool_qlx.is_month_end(timestamp_=my_time, timezone_hours=config.timezone_offset_shabik_360):
            if name=='monthly': ugc.send_email(my_time)
        elif tool_qlx.is_week_end(timestamp_=my_time, timezone_hours=config.timezone_offset_shabik_360):
            if name=='weekly': ugc.send_email(my_time)
        elif name == 'daily': 
            ugc.send_email(my_time)

 
if __name__=='__main__':

    '''
    import config
    config.smtp_server='xx'
    config.mail_from='xx'
    config.mail_targets=['xx']
    '''
    
    for i in range(config.day_to_update_stat,0,-1):
        my_time=time.time()-3600*24*i
        main(my_time)