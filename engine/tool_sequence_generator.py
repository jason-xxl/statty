
import glob
import re
import helper_regex
import helper_mysql 
'''
Patch unresistly lost data by replacing lost value with average of previous data.
'''

def generate(start_date,end_date):


    for d in helper_regex.date_iterator(start_date,end_date):
        
        helper_mysql.put_raw_data(table_name='raw_data_sequence_generator',key='date_sequence',date=d,value=0)



if __name__=='__main__':

    generate('2008-06-01','2012-12-21')

