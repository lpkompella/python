# Python script to sort the names in a text file.

import pandas as pd
import logging
#print(pd.__version__)

print('Python Script to Sort contents of Text File')
print('-------------------------------------------')

logging.basicConfig(filename='textfilesorting.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO, datefmt='%d-%b-%y %H:%M:%S')
logging.info('Log for TextFileSorting.log')

try:
    logging.info('Reading contents of text file')
    df2= pd.read_csv('input_data.txt', delimiter =',')
    df2.sort_values(by=['firstname'], inplace=True, key=lambda col: col.str.lower())
    print(df2)
except Exception as ex:
    logging.exception('Error reported %s' %(ex))
    print(ex)
