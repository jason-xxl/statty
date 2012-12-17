import helper_mysql
import time
import helper_regex
import config


def stat_mt(my_date): # run on 5:00 a.m. , calculate yesterday's data

    oem_name='Shabik_360'
    stat_category='game_heroes'

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)

    start_time=helper_regex.time_floor(my_date).replace(' 00:00:00',' 05:00:00')
    end_time=helper_regex.time_ceil(my_date).replace(' 00:00:00',' 05:00:00')
    date_today=start_time.replace(' 05:00:00','')


    # new player

    key='daily_new_player'
    sql=r'''

    select count(*) as `daily_new_player`
    from `woh`.`player`
    where `CreatedTime`>='%s' and `CreatedTime`<'%s'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_mysql.fetch_row(sql,config.conn_stc_heroes)
    print values['daily_new_player']
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['daily_new_player'],table_name='raw_data_shabik_360')


    # total player

    key='total_player'
    sql=r'''

    select count(*) as `total_new_player`
    from `woh`.`player`
    where `CreatedTime`<'%s'

    ''' % (end_time,)
    print 'SQL Server:'+sql
    values=helper_mysql.fetch_row(sql,config.conn_stc_heroes)
    print values['total_new_player']
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values['total_new_player'],table_name='raw_data_shabik_360')


    # credit revenue

    sql=r'''

    SELECT
    abs(sum(b.`BuyingPrice`)) as `daily_credit_renenue_total`
    ,count(distinct `PlayerID`) as `daily_credit_renenue_purchaser_unique`
    ,count(`RecordID`) as `daily_credit_renenue_transaction_total`
    FROM `woh`.`credittransaction`  a
    left join `woh`.`bagitems` b
    on a.itemID=b.bagItemID
    where `Time`>='%s' and `Time`<'%s'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_mysql.fetch_row(sql,config.conn_stc_heroes)
    print values

    key='daily_credit_renenue_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_shabik_360')

    key='daily_credit_renenue_purchaser_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_shabik_360')

    key='daily_credit_renenue_transaction_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_shabik_360')


    # credit revenue by item
    '''
    Amount has nothing to do with it:
        #,abs(sum(a.`Amount` * b.`BuyingPrice`)) as `daily_by_item_credit_renenue_total`
    '''
    
    sql=r'''

    SELECT

    b.`Name` as `item_id`
    ,abs(sum(b.`BuyingPrice`)) as `daily_by_item_credit_renenue_total`
    ,count(distinct `PlayerID`) as `daily_by_item_credit_renenue_purchaser_unique`
    ,count(`RecordID`) as `daily_by_item_credit_renenue_transaction_total`

    FROM `woh`.`credittransaction` a
    left join `woh`.`bagitems` b
    on a.itemID=b.bagItemID

    where `Time`>='%s' and `Time`<'%s'
    group by `ItemId`

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    rows=helper_mysql.fetch_rows(sql,config.conn_stc_heroes)
    print rows

    for values in rows:
        
        key='daily_by_item_credit_renenue_total'
        helper_mysql.put_raw_data(oem_name,stat_category,key,values['item_id'],values[key],table_name='raw_data_shabik_360',date=date_today)

        key='daily_by_item_credit_renenue_purchaser_unique'
        helper_mysql.put_raw_data(oem_name,stat_category,key,values['item_id'],values[key],table_name='raw_data_shabik_360',date=date_today)

        key='daily_by_item_credit_renenue_transaction_total'
        helper_mysql.put_raw_data(oem_name,stat_category,key,values['item_id'],values[key],table_name='raw_data_shabik_360',date=date_today)



    # gold revenue

    sql=r'''

    SELECT
    abs(sum(b.`BuyingPrice`)) as `daily_gold_renenue_total`
    ,count(distinct `PlayerID`) as `daily_gold_renenue_purchaser_unique`
    ,count(`RecordID`) as `daily_gold_renenue_transaction_total`
    
    FROM `woh`.`goldtransaction` a
    left join `woh`.`items` b
    on a.itemID=b.ItemID

    where `Time`>='%s' and `Time`<'%s'

    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    values=helper_mysql.fetch_row(sql,config.conn_stc_heroes)
    print values

    key='daily_gold_renenue_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_shabik_360')

    key='daily_gold_renenue_purchaser_unique'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_shabik_360')

    key='daily_gold_renenue_transaction_total'
    helper_mysql.put_raw_data(oem_name,stat_category,key,date_today,values[key],table_name='raw_data_shabik_360')


    
    # gold revenue by item
    '''
    some item in does not exist
        group by a.`ItemId` # include more items 
        group by b.`ItemId` # only db items
    '''
    sql=r'''

     SELECT
    ifnull(b.`Name`,a.`itemID`) as `item_id`
    ,abs(sum(b.`BuyingPrice`)) as `daily_by_item_gold_renenue_total`
    ,count(distinct `PlayerID`) as `daily_by_item_gold_renenue_purchaser_unique`
    ,count(`RecordID`) as `daily_by_item_gold_renenue_transaction_total`

    FROM `woh`.`goldtransaction` a
    left join `woh`.`items` b
    on a.itemID=b.itemID

    where `Time`>='%s' and `Time`<'%s'
    
    group by a.`ItemId`
    ''' % (start_time,end_time)

    print 'SQL Server:'+sql
    rows=helper_mysql.fetch_rows(sql,config.conn_stc_heroes)
    print rows

    for values in rows:
        
        key='daily_by_item_gold_renenue_total'
        helper_mysql.put_raw_data(oem_name,stat_category,key,values['item_id'],values[key],table_name='raw_data_shabik_360',date=date_today)

        key='daily_by_item_gold_renenue_purchaser_unique'
        helper_mysql.put_raw_data(oem_name,stat_category,key,values['item_id'],values[key],table_name='raw_data_shabik_360',date=date_today)

        key='daily_by_item_gold_renenue_transaction_total'
        helper_mysql.put_raw_data(oem_name,stat_category,key,values['item_id'],values[key],table_name='raw_data_shabik_360',date=date_today)
    
    

    # player level dispersion
    ''' be careful: this code should only be ran at a specified time because
        the time when a user is upgraded to a new level is not recorded.  
    '''
    key='total_player_level_dispersion'
    sql=r'''

    select 
    count(case when level<=5 then 1 else null end) as `[0,5]`
    ,count(case when level>5 and level<=10 then 1 else null end) as `[6,10]`
    ,count(case when level>10 and level<=15 then 1 else null end) as `[11,15]`
    ,count(case when level>15 and level<=20 then 1 else null end) as `[16,20]`
    ,count(case when level>20 and level<=25 then 1 else null end) as `[21,25]`
    ,count(case when level>25 and level<=30 then 1 else null end) as `[26,30]`
    ,count(case when level>30 and level<=35 then 1 else null end) as `[31,35]`
    ,count(case when level>35 and level<=40 then 1 else null end) as `[36,40]`
    ,count(case when level>40 and level<=45 then 1 else null end) as `[41,45]`
    ,count(case when level>45 and level<=50 then 1 else null end) as `[46,50]`
    from `woh`.`player`
    where `CreatedTime`<'%s'

    ''' % (end_time,)
    print 'SQL Server:'+sql
    values=helper_mysql.fetch_row(sql,config.conn_stc_heroes)
    print values

    for k,v in values.iteritems():
        helper_mysql.put_raw_data(oem_name,stat_category,key,k,v,table_name='raw_data_shabik_360',date=date_today)
        pass
    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        
        my_date=time.time()-3600*24*i
        stat_mt(my_date)
        
