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



def do_merge():
    
    target_dir='E:\\WebStatShare\\raw_data_Vodafone_moagent_app_page_daily_visitor_unique_collection_id_2'
    for m in ['2011-06','2011-07','2011-08','2011-09','2011-10','2011-11','2011-12',]:
        temp_set=set([])
        target_files=helper_file.get_filtered_file_list_from_dir_tree(base_path=target_dir,name_pattern='('+m.replace('-','\\-')+')')
        for f in target_files:
            temp=helper_file.read_big_string_set_from_file(f)
            temp_set |= temp
            print len(temp)

        print len(temp_set)
        print len(target_files)
        exit()






if __name__=='__main__':

    do_merge()

