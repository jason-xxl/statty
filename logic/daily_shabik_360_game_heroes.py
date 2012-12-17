import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_mysql
import config

helper_mysql.quick_insert=True

def get_handler_url_key(line):

    #2012-04-15 00:12:35,945 INFO  [(null)] - http://heroes.mozat.com/Handler/getPlayer.ashx?playerID=40192062&monetid=40192062&cli_ip=116.87.166.37: {"code":1,"name":"Terminator","monetid":40192062,"level":10,"credit":10,"race":"0","gentle":"M","avatar":"http://pc.mozat.com/icon32/201112/21/8e255156_51886668.jpg","max_mana":20,"max_health":100,"max_spirit":8,"attack":26,"defense":21,"skill_points":0,"health":100,"mana":20,"spirit":8,"gold":1171,"buildings":[{"buildingID":1,"status":100},{"buildingID":2,"status":100},{"buildingID":3,"status":100}],"ra":0,"ram":"You were attacked by  105 days ago! You lost the battle.","experience":429,"max_experience":655,"battle_won":42,"battle_lost":61,"quest_won":52,"quest_lost":3,"notification_count":30,"completed_mission_count":3,"gift_count":1,"fighter_count":21,"user_guide_status":964231103,"max_army":30}
    #2012-04-15 00:12:41,430 INFO  [(null)] - http://heroes.mozat.com/Handler/getQuestList.ashx?categoryID=1&monetid=40192062&cli_ip=116.87.166.37: {"code":1,"current_mana":2,"max_mana":20,"data":[{"name":"Wild Boar","id":51,"unlock_level":1,"mana":1,"gold":11,"exp":1,"total_time":3,"status":2,"drop_loot":0,"drop_item":0,"url":"%2fimages%2fQuest%2fQuestList%2fWild+Boar.jpg%3fv2"},{"name":"Black Bear","id":52,"unlock_level":2,"mana":2,"gold":27,"exp":2,"total_time":180,"status":2,"drop_loot":1,"url":"%2fimages%2fQuest%2fQuestList%2fBlack+Bear.jpg%3fv2"},{"name":"Giant Centipede","id":53,"unlock_level":3,"mana":3,"gold":39,"exp":3,"total_time":240,"status":0,"url":"%2fimages%2fQuest%2fQuestList%2fGiant+Centipede.jpg%3fv2"},{"name":"Goblin","id":54,"unlock_level":4,"mana":4,"gold":54,"exp":4,"total_time":360,"status":0,"url":"%2fimages%2fQuest%2fQuestList%2fGoblin.jpg%3fv2"},{"name":"Undead Goblin","id":55,"unlock_level":5,"mana":5,"gold":66,"exp":5,"total_time":420,"status":2,"drop_loot":1,"url":"%2fimages%2fQuest%2fQuestList%2fUndead+Goblin.jpg%3fv2"},{"name":"Ogre","id":56,"unlock_level":6,"mana":6,"gold":78,"exp":7,"total_time":480,"status":0,"url":"%2fimages%2fQuest%2fQuestList%2fOgre.jpg%3fv2"},{"name":"Vicious Ogre","id":57,"unlock_level":7,"mana":7,"gold":93,"exp":9,"total_time":600,"status":0,"url":"%2fimages%2fQuest%2fQuestList%2fVicious+Ogre.jpg%3fv2"},{"name":"Blood Hound","id":58,"unlock_level":8,"mana":8,"gold":105,"exp":11,"total_time":660,"status":0,"url":"%2fimages%2fQuest%2fQuestList%2fBlood+Hound.jpg%3fv2"},{"name":"Hell Hound","id":59,"unlock_level":9,"mana":9,"gold":117,"exp":14,"total_time":720,"status":0,"url":"%2fimages%2fQuest%2fQuestList%2fHell+Hound.jpg%3fv2"},{"name":"3 Head Hound","id":60,"unlock_level":10,"mana":10,"gold":141,"exp":17,"total_time":840,"status":2,"drop_loot":1,"drop_item":1,"url":"%2fimages%2fQuest%2fQuestList%2f3+Head+Hound.jpg%3fv2"}]}
    #2012-04-15 00:12:45,822 INFO  [(null)] - http://heroes.mozat.com/Handler/fightQuest.ashx?action=2&questID=51&monetid=40192062&cli_ip=116.87.166.37: {"status":2,"drop_loot":0,"drop_item":0,"level_up":0,"exp":1,"gold":11,"code":1}

    #url=helper_regex.extract(line,r'(http://[^:]+)')
    #key=helper_regex.get_simplified_url_unique_key(url)
    
    key=helper_regex.extract(line,r'http://([\w\.\/]+)').lower()

    #print key
    #print url

    return key

def get_controller_url_key(line):

    #2012-04-12 14:58:10,348 INFO  [(null)] - request url is :/?monetid=20028&moclientwidth=320&userAgent=iOS%2F4.3.5+CiOS%2F120103+Encoding%2FUTF-8+Locale%2Fen_SG+Lang%2Fen+Morange%2F6.3.3+Caps%2F7389+PI%2Fe07221450e6aac3782ee77c271b918c4+Domain%2F%40shabik.com&moclientheight=416&devicewidth=320&deviceheight=480&isprefetch=0&cli_ip=203.116.251.233
    #2012-04-12 14:58:28,429 INFO  [(null)] - request url is :/Quest/QuestList?categoryID=1&monetid=20028&moclientwidth=320&userAgent=iOS%2F4.3.5+CiOS%2F120103+Encoding%2FUTF-8+Locale%2Fen_SG+Lang%2Fen+Morange%2F6.3.3+Caps%2F7389+PI%2Fe07221450e6aac3782ee77c271b918c4+Domain%2F%40shabik.com&moclientheight=416&devicewidth=320&deviceheight=480&isprefetch=0&cli_ip=203.116.251.233

    #url='http://heroes.mozat.com'+helper_regex.extract(line,r'url is :(.+)')
    #key=helper_regex.get_simplified_url_unique_key(url)
    
    key='http://heroes.mozat.com'+helper_regex.extract(line,r'url is :([\w\.\/]+)').lower()

    #print key
    #print url

    return key


def stat_mochat(my_date):
    
    oem_name='Shabik_360'
    stat_category='game_heroes'
    
    stat_plan=Stat_plan()

    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=0)

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'monetid=\s*(\d+)'}, \
                                     where={'handler':r'(http:)'}, \
                                     group_by={'daily':lambda line:current_date, \
                                               'by_url_pattern':get_handler_url_key}, \
                                     db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql)

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'monetid=\s*(\d+)'}, \
                                     where={'request':r'(r)equest url is :'}, \
                                     group_by={'daily':lambda line:current_date, \
                                               'by_url_pattern':get_controller_url_key}, \
                                     db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql)

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                     select_count_distinct_collection={'monet_id':r'monetid=\s*(\d+)'}, \
                                     where={'whole_game':r'(.)'}, \
                                     group_by={'daily':lambda line:current_date}, \
                                     db_name='raw_data_shabik_360')
    
    stat_plan.add_stat_sql(stat_sql)


    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.177\logs_game_heroes_shabik_360\Controller.log%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y%m%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.177\logs_game_heroes_shabik_360\Handler.log%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y%m%d'))

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_mochat(time.time()-3600*24*i)



