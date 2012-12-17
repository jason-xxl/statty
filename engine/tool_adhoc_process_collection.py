
import config
import glob
import re
import helper_regex
import helper_mysql
import helper_math
from helper_mysql import db
import _mysql

'''
'''

config.collection_cache_enabled=True
config.conn_stat_portal=config._conn_stat_portal_158_2


def do_calculate(current_date):
    
    urls=[
        'mobileshabik.morange.com/mophoto_popular_photos.aspx?src_evflg_1',
        'mobileshabik.morange.com/mophoto_popular_photos.aspx?src_evflg_1&isprefetch',
        'mobileshabik.morange.com/mophoto_popular_photos.aspx?isprefetch&src_evflg_0',
        'mobileshabik.morange.com/mophoto_popular_photos.aspx?src_evflg_0',
        'mobileshabik.morange.com/mophoto_popular_photos.aspx?',
        'mobileshabik.morange.com/mophoto_popular_photos.aspx?isprefetch',
    ]

    urls=[
        'mobileshabik.morange.com/mophoto_photo.aspx?albumid&src_pe&tag&photoid&type',
        'mobileshabik.morange.com/mophoto_photo.aspx?photoid&albumid&src_pe&tag&type&isprefetch',
    ]
    
    urls=[
        'mobilevoda.morange.com/mophoto_popular_photos_[digits].aspx?src_app',
        'mobilevoda.morange.com/mophoto_popular_photos_[digits].aspx?src_feed',
        'mobilevoda.morange.com/mophoto_popular_photos_[digits].aspx?src_myphoto',
        'mobilevoda.morange.com/mophoto_popular_photos.aspx?start',
        'mobilevoda.morange.com/mophoto_popular_photos.aspx?src_app',
    ]
    
    urls=[
        'mobilevoda.morange.com/mophoto_photo.aspx?albumid&src_pe&tag&photoid&type'
    ]

    collection_current=set([])

    for u in urls:
        collection_temp=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                                            key='app_page_by_url_pattern_daily_visitor_unique', \
                                                            sub_key=u,date=current_date, \
                                                            table_name='data_url_pattern_vodafone',db_conn=None)
        collection_current |= collection_temp
        #print len(collection_current),len(collection_temp)
    

    collection_current_1=set([])

    for u in urls:
        collection_temp=helper_mysql.get_raw_collection_from_key(oem_name='Vodafone',category='moagent', \
                                                            key='app_page_by_url_pattern_daily_visitor_unique', \
                                                            sub_key=u,date=helper_regex.date_add(current_date,-1), \
                                                            table_name='data_url_pattern_vodafone',db_conn=None)
        collection_current_1 |= collection_temp
        #print len(collection_current),len(collection_temp)


    
    retained = collection_current_1 & collection_current
    #print set([1,2,3,4,8]) & set([9,3,4,8,10])

    print len(collection_current_1 | collection_current)
    print len(collection_current),len(collection_current_1)
    print len(retained)
    print 1.0*len(retained)/len(collection_current_1)

if __name__=='__main__':

    do_calculate(current_date='2011-12-10')