
targets=['small-missile',
'middle-missile',
'big-missile',
'shield',
'small-toolbox',
'middle-toolbox',
'big-toolbox',
'A-bomb',
'Missile Combo',
'Big Missile Combo',
'Toolbox Combo',
'Big Toolbox Combo',
'Sailor',
'Thief',
'Guard',
'Prophet',
'Spy',
'Insurance',
'Robber',
'Fish_Trader',
'Fishman_Combo',
'Pirate_Combo',
'pyramid-background',
'pyramid-shipyard',
'pyramid-warehouse',
'Christmas-background',
'Christmas-shipyard',
'Christmas-warehouse',
'Halloween-background',
'Halloween-shipyard',
'Halloween-warehouse',
'Destroy-background',
'EiffelTower-background',
'greatwall-background',
'liberty-background',
'Romancoliseum-background',
'4backgrounds',
'Level1',
'Level2',
'Level3',
'Level4',
'Anti-Missile',
'Anti-Abomb',
'Anti-Missilecomb',
'Anti-Abombcomb',
'DefenceMissileCombo',
'big_bomb',
'Big-bomb',
'5_A-bomb',
'10_A-bomb',
'100A-bomb',
'100super',
'5_super_toolbox',
'10_super_toolbox',
'5_big_bomb',
'10_big_bomb',
'10_anti_missile_combo',
'10_anti_abomb_combo',
'Big_Toolbox_Combo',
'Toolbox_Combo',
'Big_Missile_Combo',
'Missile_Combo',
'5_missile_combo',
'5_big_missile_combo',
'5_bigtoolbox_combo',
'10_missile_combo',
'10_bigtoolbox_combo',
'10_big_missile_combo',
'super-toolbox',
'MissileAbombcomb']

targets=sorted(targets)

for i in targets:
    
    print r'''
    
,max( if( `oem_name`='Zoota' and category='payment' and `key` = 'payment_ammount_by_item' and `sub_key`='%s' , `value` , 0 ) ) AS `%s Income`
,max( if( `oem_name`='Zoota' and category='payment' and `key` = 'payment_count_by_item' and `sub_key`='%s' , `value` , 0 ) ) AS `%s Quantity`
,max( if( `oem_name`='Zoota' and category='payment' and `key` = 'payment_unique_user_by_item' and `sub_key`='%s' , `value` , 0 ) ) AS `%s UV`
    ''' % (i,i,i,i,i,i,)    

exit()


targets=[
('dogfood2000001','Dog food (Dog)'),
('dog1000000','Stray Dog (Dog)'),
('dog1000002','French Bulldog (Dog)'),
('DogHouse1200000','Dog House (Dog)'),
('item73','Ruby Tree (Seed)'),
('item74','Sapphire Tree (Seed)'),
('item75','Topaz Tree (Seed)'),
('extendtool3000001','Super Thief (Other)'),
('extendtool3000000','Magic Stick (Other)'),
('deco100000','Romantic (Homepage Background)'),
('deco18','Winter (Homepage Background)'),
('deco19','Evening (Homepage Background)'),
('Fertilizers1','-1 Hour (Fertilizer)'),
('Fertilizers2','-2.5 Hour (Fertilizer)'),
('Fertilizers3','-3.5 Hour (Fertilizer)'),
('Fertilizers4','-4.5 Hour (Fertilizer)'),
]

tpl=r'''
,max( if( `oem_name`='Zoota' and category='payment' and `key` = 'payment_ammount_by_item' and `sub_key`='%(id)s' , `value` , 0 ) ) AS `%(name)s Income`
,max( if( `oem_name`='Zoota' and category='payment' and `key` = 'payment_count_by_item' and `sub_key`='%(id)s' , `value` , 0 ) ) AS `%(name)s Quantity`
,max( if( `oem_name`='Zoota' and category='payment' and `key` = 'payment_unique_user_by_item' and `sub_key`='%(id)s' , `value` , 0 ) ) AS `%(name)s UV`

'''

for i in targets:
    print tpl  % {'id':i[0],'name':i[1]}
    

exit()

targets=['profile','greeting_cards','football_war','help','saying','ocean_age_world','photo','twitter','mochat','photo_server','ocean_age','im','gomoku','new_user_wizard','message','homepage_old_version','event','tab_apps','poke','matrix','billing','notification','app_center','leader_board','happy_barn','client_prefetch','setting','poll','location','chatroom','circle','flickr','homepage','email','friend','status','hot_photo','invite','recent_visitor','star_user','netlog','texas_holdem','facebook','linkedin','baloot','rss','aladdin','phone_backup','level_system','register','youtube','public_photo','browser']

tpl=r'''

,concat('<a href="view.php?id=%s&date1=',date,'&date2=',date,'&key2=1&key3=%s" target="_blank">',max( if( `oem_name`='Shabik_360' and `category`='moagent_error_page' and `key`='is_url_by_app_by_url_type_daily_error_count' and `sub_key` like '%s_%%' and `sub_key` not like '%%_non_app_page' , `value` , 0 ) ),'</a>') AS `%s App Page Error` 

,concat(format(
	max( if( `oem_name`='Shabik_360' and `category`='moagent_error_page' and `key`='is_url_by_app_by_url_type_daily_error_count' and `sub_key` like '%s_%%' and `sub_key` not like '%%_non_app_page' , `value` , 0 ) ) / (
		max( if( `oem_name`='Shabik_360' and `category`='moagent_error_page' and `key`='is_url_by_app_by_url_type_daily_error_count' and `sub_key` like '%s_%%' and `sub_key` not like '%%_non_app_page' , `value` , 0 ) )
		+max( if( `oem_name`='Shabik_360' and `category`='moagent' and `key`='app_page_by_app_daily_visitor_unique_base' and `sub_key` = '%s' , `value` , 0 ) )
	)*100
,2),'%%') AS `%s App Page Error Rate` 

,concat('<a href="view.php?id=%s&date1=',date,'&date2=',date,'&key2=2&key3=%s" target="_blank">',max( if( `oem_name`='Shabik_360' and `category`='moagent_error_page' and `key`='is_url_by_app_by_url_type_daily_error_count' and `sub_key` like '%s_%%' and `sub_key` like '%%_non_app_page' , `value` , 0 ) ),'</a>') AS `%s Non App Page Error` 
'''


for i in sorted(targets):
    print tpl % (1052,i,i,i.replace('_',' ').title(),i,i,i,i.replace('_',' ').title(),1052,i,i,i.replace('_',' ').title(),)
    



exit()


targets=['profile','greeting_cards','football_war','help','saying','ocean_age_world','photo','twitter','mochat','photo_server','ocean_age','im','gomoku','new_user_wizard','message','homepage_old_version','event','tab_apps','poke','matrix','billing','notification','app_center','leader_board','happy_barn','client_prefetch','setting','poll','location','chatroom','circle','flickr','homepage','email','friend','status','hot_photo','invite','recent_visitor','star_user','netlog','texas_holdem','facebook','linkedin','baloot','rss','aladdin','phone_backup','level_system','register','youtube','public_photo','browser']

tpl=r'''<option value="%s">%s</option>'''

print r'''<option value="">(all apps)</option>''',
for i in sorted(targets):
    print tpl % (i,i.replace('_',' ').capitalize()),
    

exit()


tpl=r'''
,format(100.0
*max(if(`oem_name`='Shabik_360' and category='moagent' and `key` ="client_homepage_loding_time_step_10_dispersion_user_unique" and `sub_key`='%s',`value`,0))
/max(if(`oem_name`='Shabik_360' and category='moagent' and `key` ="client_homepage_loding_time_user_unique",`value`,0))
,4) as `[%s,%s) %%`
'''

for i in range(0,101,10):
    print tpl % (i,i,i+10)
    
exit()



targets=['profile','greeting_cards','football_war','help','saying','ocean_age_world','photo','twitter','mochat','photo_server','ocean_age','im','gomoku','new_user_wizard','message','homepage_old_version','event','tab_apps','poke','matrix','billing','notification','app_center','leader_board','happy_barn','client_prefetch','setting','poll','location','chatroom','circle','flickr','homepage','email','friend','status','hot_photo','invite','recent_visitor','star_user','netlog','texas_holdem','facebook','linkedin','baloot','rss','phone_backup','level_system','youtube','public_photo','browser']



tpl=r'''

,max(if(`oem_name`='Shabik_360' and `category`='moagent_error_page' and `key`='is_url_by_app_by_url_type_daily_error_count' and replace(`sub_key`,'_app_page','')='%(name)s',`value`,0)) AS `%(uc_name)s App Page Error`

,concat(format(
    100.0
    *max(if(`oem_name`='Shabik_360' and `category`='moagent_error_page' and `key`='is_url_by_app_by_url_type_daily_error_count' and replace(`sub_key`,'_app_page','')='%(name)s',`value`,0))
    /(
        max(if(`oem_name`='Shabik_360' and `category`='moagent_error_page' and `key`='is_url_by_app_by_url_type_daily_error_count' and replace(`sub_key`,'_app_page','')='%(name)s',`value`,0))
        +max(if(`oem_name`='Shabik_360' and `category`='moagent' and `key`='app_page_by_app_daily_visitor_unique' and `sub_key`='%(name)s',`value`,0))
    )
,2),'%%') AS `%(uc_name)s App Page Error Rate` 


,max(if(`oem_name`='Shabik_360' and `category`='moagent_error_page' and `key`='is_url_by_app_by_url_type_daily_error_count' and replace(`sub_key`,'_non_app_page','')='%(name)s',`value`,0)) AS `%(uc_name)s Non App Page Error`

'''


for i in sorted(targets):
    print tpl % {'name':i,'uc_name':i.replace(' ','').capitalize()}
    




exit()


lines=[

",max(if(`oem_name`='Shabik_360' and `category`='website' and `key`='filtered_9.file-type-start_daily_msisdn_unique' and `sub_key`='client-start-file',`value`,0)) as `Overall Download Unique MSISDN`",
",max(if(`oem_name`='Shabik_360' and `category`='website' and `key`='filtered_1.cli-type_daily_msisdn_unique' and `sub_key`='Android',`value`,0)) as `Android Download Unique MSISDN`",
",max(if(`oem_name`='Shabik_360' and `category`='website' and `key`='filtered_8.special-file-type_daily_msisdn_unique' and `sub_key`='bb-start-file',`value`,0)) as `BlackBerry Download Unique MSISDN`",
",max(if(`oem_name`='Shabik_360' and `category`='website' and `key`='filtered_8.special-file-type_daily_msisdn_unique' and `sub_key`='jme-start-file',`value`,0)) as `JME Download Unique MSISDN`",
",max(if(`oem_name`='Shabik_360' and `category`='website' and `key`='filtered_1.cli-type_daily_msisdn_unique' and `sub_key`='Symbian-3',`value`,0)) as `Symbian-3 Download Unique MSISDN`",
",max(if(`oem_name`='Shabik_360' and `category`='website' and `key`='filtered_1.cli-type_daily_msisdn_unique' and `sub_key`='Symbian-5',`value`,0)) as `Symbian-5 Download Unique MSISDN`",

]

prefixes=[

"ever_sub_",
"in_sub_",
"logined_",
"fresh_ever_sub_",
"fresh_in_sub_",
"fresh_logined_",

]


for i in lines:
    print i
    for j in prefixes:
        print i.replace('filtered_',j+'filtered_').replace('_unique','_unique_element_count').replace(' as `',' as `'+j)#.replace(' as ','*1.0/'+i[1:i.find('as `')]+' as ')
    print    
    print    
    

exit()









for i in range(50,0,-1):
    print "		'key%s'=> if_blank($_GET['key%s'],'')," % (i,i)
    
exit()





for i in range(1,57,3):
    
    tpl=r'''

,format(1.0
*max(if(`oem_name`='Shabik_360' and category='login_retain' and `key` like 'daily_new_360_user_3_day_logined_%%' and `key` like '%%_unique' and `sub_key`='%s',`value`,0)) 
/max(if(`oem_name`='Shabik_360' and category='login_retain' and `key` like 'daily_new_360_user_initial_%%' and `key` like '%%_unique',`value`,0))
,3) AS `%s 3-Day Login Rate`

    ''' % (i,i)

    print tpl    


exit()


for i in range(1,57,3):
    
    tpl=r'''
,format(1.0
*max(if(`oem_name`='Shabik_360' and category='login_retain' and `key` like 'daily_new_360_user_3_day_logined_%%' and `key` like '%%_unique' and `sub_key`='%s',`value`,0)) 
/max(if(`oem_name`='Shabik_360' and category='login_retain' and `key` like 'daily_new_360_user_initial_%%' and `key` like '%%_unique',`value`,0))
,3) AS `%s 3-Day Login Rate`
    ''' % (i,i)

    print tpl    


exit()


for i in range(1,57,3):
    
    tpl=r'''
,format(100.0*max(if(`oem_name`='Vodafone' and category='trend' and `key`='daily_3d_active_new_subscriber_unique' and `sub_key`='%s',`value`,0))
	/ max(if(`oem_name`='Vodafone' and category='trend' and `key`='daily_new_subscriber_unique',`value`,0))
,2) as `3d Active Rate %% %s`
    ''' % (i,i)

    print tpl    
    


    
