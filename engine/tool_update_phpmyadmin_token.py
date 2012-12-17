import helper_sql_server
import glob
import re
import os
import helper_regex
from helper_mysql import db
import _mysql
import helper_mysql
import helper_file
import config

#update the phpmyadmin2 token in view.php

def do_update():
    script_path='E:\\AppServ\\www\\xstat\\htdocs\\subpages\\view.php'
    script_content=helper_file.get_content_from_file(script_path)
    old_token=helper_regex.extract(script_content,r'token=(\w+)')
    url=helper_regex.extract(script_content,r'href="(http://192.168.0.158:81/phpmyadmin-2[^"]+)"').replace("<?=$_PAGE['view_id']?>",'150')

    page_content=helper_file.get_http_content(url)
    new_token=helper_regex.extract(page_content,r'token=(\w+)')

    new_token='da1b5116e305194ca8fd7806df008453'

    script_content=script_content.replace(old_token,new_token)
    #print page_content
    helper_file.put_content_to_file(script_content,script_path)
    print old_token,'to',new_token

    url=url.replace(old_token,new_token)
    helper_file.get_http_content(url)

    script_path='E:\\AppServ\\www\\xstat\\htdocs\\subpages\\view.php'
    script_content=helper_file.get_content_from_file(script_path)
    current_token=helper_regex.extract(script_content,r'token=(\w+)')
    print current_token

if __name__=='__main__':

    do_update() # wait for fix

