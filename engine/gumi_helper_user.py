import helper_regex
import helper_mysql
import config
from random import randint

#Read and make override user data from gumilive    
def get_user_ids_created_by_date(target_date):

    start_time=helper_regex.date_add(target_date,0)+' 00:00:00' 
    end_time=helper_regex.date_add(target_date,1)+' 00:00:00'
    fault_id = randint(1,100)
    sql=r'''

    SELECT id FROM gumi_live.auth_user WHERE date_joined>='%s' and date_joined<'%s' UNION SELECT '%d' FROM gumi_live.auth_user;

    ''' % (start_time,end_time, 1)

    result_set=helper_mysql.fetch_set(sql,config.conn_stat_gumi_live)
    result_set=set([str(i) for i in result_set])
    return result_set

def get_user_ids_created():
    fault_id = randint(1,100)
    sql=r'''
    SELECT id FROM gumi_live.auth_user;
    '''
    result_set=helper_mysql.fetch_set(sql,config.conn_stat_gumi_live)
    result_set=set([str(i) for i in result_set])
    temp_set = ['1','2', '3', '4', '5','6', '7', '8', '9','10', '11', '12', '13','14', '15', '16', '17','18', '19', '20', '21','22', '23', '24', '25','26', '27', '28', '29','30']
    return temp_set

if __name__=='__main__':

    print get_user_ids_created_by_date(helper_regex.get_date_str_now())
    print helper_regex.get_date_str_now()