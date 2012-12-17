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

def export_shared_folder_list():
    
    root_dir=config.execute_dir

    def process_line(line='',exist='',group_key=''):
        shared_folder_dir=helper_regex.extract('(\\{2,}\d+\.\d+\.\d+\.\d+\\+(\w+\\+)*)',line)
        print shared_folder_dir

    stat_plan=Stat_plan()

    stat_sql=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                    select_count_distinct_collection={'monet_id':r'monetId: (\d+)'}, \
                    where={'folder':r'(\[doEnterChatroom\])'})
        



if __name__=='__main__':

    pass
