import os
import urllib
import urllib2
import cookielib
import gzip
import config
import helper_regex
import time
import helper_mysql


def check_stat_plan_log_file_reading_completion(stat_plan):
    
    """
    self.log_table_name='raw_data_monitor'
    self.log_oem_name=self.script_file_name
    self.log_category=helper_ip.get_current_server_ip()+'_'+self.start_time_str.replace(' ','_').replace(':','_').replace('-','_')+'_'+self.uuid

    helper_mysql.put_raw_data(oem_name=self.log_oem_name, \
                              category=self.log_category, \
                              key='original_file_size', \
                              sub_key=log_file_name, \
                              value=helper_file.get_file_size(log_file_name), \
                              table_name=self.log_table_name)
    """

    # 1.check total log file number

    current_date=helper_regex.extract(stat_plan.log_category,r'_(\d{4}_\d{2}_\d{2})_').replace('_','-')
    previous_date=helper_regex.date_add(current_date,-1)
    previoud_date_category_like=helper_regex.extract(stat_plan.log_category.replace(current_date,previous_date),r'([\d\.]+_\d{4}_\d{2}_\d{2})')

    sql=r'''

    select 

    (select count(distinct sub_key)
    from raw_data_monitor
    where oem_name='%s'
    and category='%s')

    -

    (select count(distinct sub_key)
    from raw_data_monitor
    where oem_name='%s'
    and category=(
        select count(distinct sub_key)
        from raw_data_monitor
        where oem_name='%s'
        and category like '%s%%'
    ))

    ''' % (stat_plan.log_oem_name,stat_plan.log_category,stat_plan.log_oem_name,stat_plan.log_oem_name,previoud_date_category_like)

    print sql

    distance=helper_mysql.get_one_value_string(sql)

    print distance
    return distance

if __name__=='__main__':

    pass
