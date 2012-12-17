import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_file
import helper_mysql


def stat_qos(my_date): # run on 5:00 a.m. , calculate yesterday's data

    date_today=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    oem_name='Vodafone'
    stat_category='qos'

    # IM accounts

    key='daily_accumulative_im_account'
    sub_key='msn'
    content=helper_file.get_http_content(url=r'http://192.168.1.71:7810/?action=getTotalAccount&accounttype=1')
    value=helper_regex.extract(content,r'"result":\s*(\d+)')
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key+'_'+date_today,value)

    key='daily_accumulative_im_account'
    sub_key='gtalk'
    content=helper_file.get_http_content(url=r'http://192.168.1.71:7810/?action=getTotalAccount&accounttype=2')
    value=helper_regex.extract(content,r'"result":\s*(\d+)')
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key+'_'+date_today,value)

    key='daily_accumulative_im_account'
    sub_key='yahoo'
    content=helper_file.get_http_content(url=r'http://192.168.1.71:7810/?action=getTotalAccount&accounttype=3')
    value=helper_regex.extract(content,r'"result":\s*(\d+)')
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key+'_'+date_today,value)

    key='daily_accumulative_im_account'
    sub_key='facebook'
    content=helper_file.get_http_content(url=r'http://192.168.1.71:7810/?action=getTotalAccount&accounttype=10')
    value=helper_regex.extract(content,r'"result":\s*(\d+)')
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key+'_'+date_today,value)


    # Mochat msg

    key='daily_mochat_message'
    sub_key=''
    content=helper_file.get_http_content(url=r'http://192.168.1.60:4032/')
    value=helper_regex.extract(content,r'dayMessage":\s*(\d+)')
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key+'_'+date_today,value)

    # sns account

    key='daily_accumulative_sns_account'
    sub_key='facebook'
    content=helper_file.get_http_content(url=r'http://192.168.1.76:3557/FacebookVoda/accountStatistic.jsp?action=getTotalAccount')
    value=helper_regex.extract(content,r'result:\s*(\d+)')
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key+'_'+date_today,value)

    key='daily_accumulative_sns_account'
    sub_key='twitter'
    content=helper_file.get_http_content(url=r'http://voda-motwitter.i.mozat.com/ViewLog.ashx')
    value=helper_regex.extract(content,r'"Total":\s*(\d+)')
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key+'_'+date_today,value)


    # game uv

    """
    key='daily_game_player_unique'
    sub_key='ocean_age'
    #content=helper_file.get_http_content(url=r'http://voda-oa.i.mozat.com:8081/OceanAge/uvCount.jsp')
    content=helper_file.get_http_content(url=r'''http://voda-oa.i.mozat.com:8081/OceanAge/uv_count.jsp?year=%s&month=%s&day=%s'''
                                                % ( datetime.fromtimestamp(my_date).strftime('%Y'),
                                                    datetime.fromtimestamp(my_date).strftime('%m'),
                                                    datetime.fromtimestamp(my_date).strftime('%d')))

    #value=helper_regex.extract(content,r'OA UV":\s*(\d+)')
    value=helper_regex.extract(content,r':\s*(\d+)')
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key+'_'+date_today,value)
    """

    key='daily_game_player_unique'
    sub_key='football'
    #content=helper_file.get_http_content(url=r'http://voda-footballwar.i.mozat.com/Admin/viewshow.aspx')
    content=helper_file.get_http_content(url=r'http://voda-footballwar.i.mozat.com/Admin/ViewShow.aspx?querydate='+datetime.fromtimestamp(my_date).strftime('%Y/%m/%d'))
    value=helper_regex.extract(content,r'uv:\s*(\d+)')
    helper_mysql.put_raw_data(oem_name,stat_category,key,sub_key+'_'+date_today,value)






if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_qos(time.time()-3600*24*i)
