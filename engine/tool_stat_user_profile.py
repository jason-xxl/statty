import helper_sql_server
import config
import glob
import re
import helper_regex
import helper_mysql
import helper_math
from helper_mysql import db
import _mysql
import common_mozat

'''
'''

config.collection_cache_enabled=True

def get_profile_statistics():
    
    start_date='2011-09-01'
    end_date='2012-04-23'

    user_set_daily_active={}

    date_temp=start_date
    while True:
        user_set_daily_active[date_temp]=helper_mysql.get_raw_collection_from_key(oem_name='Mozat',category='mosession', \
                                                                     key='login_by_country_daily_monet_id_unique', \
                                                                     sub_key='Nigeria',date=date_temp,table_name='raw_data', \
                                                                     db_conn=None)
        date_temp=helper_regex.date_add(date_temp,1)
        if date_temp>end_date:
            break
        
    user_set_monthly_active=helper_math.merge_set_dict_by_mapping_function(user_set_daily_active,lambda k:k[0:7])
    
    user_set_all_active=reduce(lambda s1,s2:s1|s2, user_set_daily_active.values())

    user_set_daily_sign_up={}

    date_temp=start_date
    while True:
        user_set_daily_sign_up[date_temp]=common_mozat.get_user_ids_created_by_date(date_temp) & user_set_all_active
        date_temp=helper_regex.date_add(date_temp,1)
        if date_temp>end_date:
            break
        
    user_set_monthly_sign_up=helper_math.merge_set_dict_by_mapping_function(user_set_daily_sign_up,lambda k:k[0:7])
    
    user_set_all_sign_up=reduce(lambda s1,s2:s1|s2, user_set_daily_sign_up.values())

    user_set_all_active_with_gender=helper_sql_server.fetch_dict_map_to_collection(collection_set=user_set_all_active, sql_template=r'''
    
    select user_id,gender
    from mozone_user.dbo.profile with(nolock)
    where user_id in (%s)

    ''',conn_config=config.conn_mozat,step=1000)

    user_set_all_active_with_age=helper_sql_server.fetch_dict_map_to_collection(collection_set=user_set_all_active, sql_template=r'''
    
    select user_id,case 
        when floor(DATEDIFF(d, birthday, getdate()) / 365.25)>=5 and floor(DATEDIFF(d, birthday, getdate()) / 365.25)<90 
        then floor(DATEDIFF(d, birthday, getdate()) / 365.25)
        else 0
        end
    from mozone_user.dbo.profile with(nolock)
    where user_id in (%s)

    ''',conn_config=config.conn_mozat,step=1000)

    report={}   

    report['date_of_lacking_data']=','.join([k for (k,v) in user_set_daily_active.iteritems() if len(v)==0])
    
    report['existing_active_user']=len(user_set_all_active)

    report['highest_daily_active_user']=max(len(v) for (k,v) in user_set_daily_active.iteritems())
    report['highest_daily_active_user_date']=','.join([k for (k,v) in user_set_daily_active.iteritems() if len(v)==report['highest_daily_active_user']])

    report['highest_monthly_active_user']=max(len(v) for (k,v) in user_set_monthly_active.iteritems())
    report['highest_monthly_active_user_date']=','.join([k for (k,v) in user_set_monthly_active.iteritems() if len(v)==report['highest_monthly_active_user']])

    report['highest_daily_sign_up']=max(len(v) for (k,v) in user_set_daily_sign_up.iteritems())
    report['highest_daily_sign_up_date']=','.join([k for (k,v) in user_set_daily_sign_up.iteritems() if len(v)==report['highest_daily_sign_up']])

    report['highest_monthly_sign_up']=max(len(v) for (k,v) in user_set_monthly_sign_up.iteritems())
    report['highest_monthly_sign_up_date']=','.join([k for (k,v) in user_set_monthly_sign_up.iteritems() if len(v)==report['highest_monthly_sign_up']])

    report['monthly_active_user_trend']=dict((k,len(v)) for (k,v) in user_set_monthly_active.iteritems())
    report['monthly_sign_up_trend']=dict((k,len(v)) for (k,v) in user_set_monthly_sign_up.iteritems())

    report['male_in_existing_active_user']=len([k for k,v in user_set_all_active_with_gender.iteritems() if v=='m'])
    report['female_in_existing_active_user']=len([k for k,v in user_set_all_active_with_gender.iteritems() if v=='f'])
    report['unknown_gender_in_existing_active_user']=report['existing_active_user']-report['male_in_existing_active_user']-report['female_in_existing_active_user']
    
    report['age_dispersion_in_existing_active_user']=helper_math.get_simple_dispersion(user_set_all_active_with_age,10)

    print str(sorted(report.iteritems()))



if __name__=='__main__':

    get_profile_statistics()