import os
import config
import shell

os.chdir(config.execute_dir)

##### task #####

##### daily routine #####

shell.system('daily_all_data_check.py>>')
shell.system('daily_all_view_check.py>>')
shell.system('daily_all_log_path_check.py>>')

