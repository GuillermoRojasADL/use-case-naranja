import os
import pandas as pd
import pickle
import time
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split

###################
# transform text files to h5 files
INPUT_EXTENSION = '.pkl'

INPUT_DATA_PATH = '/mnt/work/mdt/'
CES_PV_PREF_PATH = '/mnt/work/datasets/preMDT_cesantias_pi_def//'
FILE_NAME = '20190827-final-master_table_churn-5cats_dummies-rem-leakage'
#OUTPUT_DATA_PATH = '/home/pandrade/'
print('loading data from' + INPUT_DATA_PATH + FILE_NAME + INPUT_EXTENSION)
ini=time.time()

year_month =\
    pd.DataFrame(pd.date_range(start = '2017-05-01', end='2019-06-01',
                               freq='MS').map(lambda x: np.str(x.year) + "" + np.str(x.month).rjust(2,'0')))
year_month.columns = ['YearMonth']


with open(INPUT_DATA_PATH + FILE_NAME + INPUT_EXTENSION, 'rb') as input:
    dataset = pickle.load(file=input)
#Archivo de cesantias
file = 'cesantias_preMDT.snappy.parquet'
ces = pd.read_parquet(os.path.join(CES_PV_PREF_PATH,file))

file = 'pv_preMDT.snappy.parquet'
pv = pd.read_parquet(os.path.join(CES_PV_PREF_PATH,file))

## Join  con MDT
pv = pv.rename(columns = {'SALDO_TOTAL':'SALDO_TOTAL_PV'})


pv['FEC_APERTURA'] = pv['FEC_APERTURA'].replace('"', np.nan)
pv['FEC_APERTURA'] = pd.to_datetime(pv['FEC_APERTURA'].apply(str), format='%d-%b-%y')


pv.drop(columns='COD_CUENTA_ANONI', inplace=True)

df = dataset[['IDENTIFICADOR', 'YearMonth']]



join_pv = pd.merge(df,
                     pv,
                     how='left',
                     left_on=['IDENTIFICADOR'],
                     right_on = ['IDENTIFICADOR'])



join_pv['YearMonth'] = pd.to_datetime(join_pv['YearMonth'].apply(str)+'01', format='%Y%m%d', errors='coerce')
join_pv['SALDO_FINAL_PV'] = np.where((join_pv['YearMonth']<= join_pv['FEC_ULT_APORTE']) &
                               (join_pv['YearMonth']>= join_pv['FEC_APERTURA']),
                                     join_pv['SALDO_TOTAL_PV'],
                                     0)

join_pv['TIENE_PV'] = np.where((join_pv['YearMonth']>= join_pv['FEC_APERTURA']), 1, 0 )



join_pv = join_pv.drop_duplicates()

join_pv.shape
join_pv['YearMonth'] = 100*join_pv['YearMonth'].dt.year + join_pv['YearMonth'].dt.month
join_pv['YearMonth'] = join_pv['YearMonth'].astype(str)



join_pv_dataset \
    = pd.merge(dataset,
                     join_pv,
                     how='left',
                     left_on=['IDENTIFICADOR','YearMonth'],
                     right_on = ['IDENTIFICADOR','YearMonth'])



join_pv_dataset.to_parquet(os.path.join(INPUT_DATA_PATH, '20190827-final-master_table_churn-5cats_dummies-rem-leakage-PI' + '.snappy.parquet'),compression='snappy')

