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

file = 'cesantias_preMDT.snappy.parquet'
ces = pd.read_parquet(os.path.join(CES_PV_PREF_PATH,file))

file = 'pv_preMDT.snappy.parquet'
pv = pd.read_parquet(os.path.join(CES_PV_PREF_PATH,file))

file = 'preferencial_web_preMDT.snappy.parquet'
pref_web = pd.read_parquet(os.path.join(CES_PV_PREF_PATH,file))

file = 'preferencial_mov_preMDT.snappy.parquet'
pref_movil = pd.read_parquet(os.path.join(CES_PV_PREF_PATH,file))

#dataset.dtypes.to_csv(os.path.join(CES_PV_PREF_PATH, 'columns_MDT.csv'))

#crear variable tiene_producto

ces['TIENE_CES'] = 1
pv['TIENE_PV'] = 1
pref_movil['TIENE_PREF_MOVIL'] = 1
pref_web['TIENE_PREF_WEB'] = 1

## Join  con MDT
pv = pv.rename(columns = {'SALDO_TOTAL':'SALDO_TOTAL_PV'})


pv['FEC_APERTURA'] = pv['FEC_APERTURA'].replace('"', np.nan)
pv['FEC_APERTURA'] = pd.to_datetime(pv['FEC_APERTURA'].apply(str), format='%d-%b-%y')


#pv['SALDO_TOTAL_PV'].describe()
#dataset['YearMonth'].value_counts()



pv.drop(columns='COD_CUENTA_ANONI', inplace=True)

df = dataset[['IDENTIFICADOR', 'YearMonth']]

df.shape

join_pv = pd.merge(df,
                     pv,
                     how='left',
                     left_on=['IDENTIFICADOR'],
                     right_on = ['IDENTIFICADOR'])

join_pv.shape
join_pv.columns

join_pv['YearMonth'] = pd.to_datetime(join_pv['YearMonth'].apply(str)+'01', format='%Y%m%d', errors='coerce')

join_pv['SALDO_FINAL_PV'] = np.where((join_pv['YearMonth']<= join_pv['FEC_ULT_APORTE']) &
                               (join_pv['YearMonth']>= join_pv['FEC_APERTURA']),
                                     join_pv['SALDO_TOTAL_PV'],
                                     0)

join_pv['TIENE_PV'] = np.where((join_pv['YearMonth']>= join_pv['FEC_APERTURA']), 1, 0 )


#para validación de registros
#
# join_pv[(join_pv['FEC_APERTURA']>='2018-01-01') & (join_pv['TIENE_PV']>=1) ].head()
#

#aa1_1 = join_pv[join_pv['IDENTIFICADOR']=='063461D948D5B72774B19DA8FE5D29E1']

#aa3=pv[pv['IDENTIFICADOR']=='A93D3846326BB831AD5D0358C1FC9B14']
#aa1 = join_pv[join_pv['IDENTIFICADOR']=='A93D3846326BB831AD5D0358C1FC9B14']
#aa1.shape
#aa2.shape
#aa2 = join_pv_filter[join_pv_filter['IDENTIFICADOR']=='A93D3846326BB831AD5D0358C1FC9B14']


#### limpiar duplicados

#join_pv[join_pv[['IDENTIFICADOR','YearMonth']].duplicated()].count()
#join_pv[join_pv[['IDENTIFICADOR','YearMonth']].duplicated()].head()

#join_pv[(join_pv['IDENTIFICADOR']== '036EB1A57259D43126DF1DE1178E953E') & (join_pv['YearMonth'] == '2017-06-01')].to_csv('test_pv2.csv',sep='\t')

join_pv = join_pv.drop_duplicates()

join_pv.shape
join_pv['YearMonth'] = 100*join_pv['YearMonth'].dt.year + join_pv['YearMonth'].dt.month
join_pv['YearMonth'] = join_pv['YearMonth'].astype(str)

join_pv['YearMonth'].dtypes

join_pv_dataset \
    = pd.merge(dataset,
                     join_pv,
                     how='left',
                     left_on=['IDENTIFICADOR','YearMonth'],
                     right_on = ['IDENTIFICADOR','YearMonth'])

join_pv_dataset.columns
join_pv_dataset.shape
join_pv_dataset.dtypes


join_pv_dataset.to_parquet(os.path.join(INPUT_DATA_PATH, '20190827-final-master_table_churn-5cats_dummies-rem-leakage-PI' + '.snappy.parquet'),compression='snappy')


#####
join_pv = join_pv.drop(['FEC_ULT_APORTE','FEC_APERTURA','SALDO_TOTAL_PV'], axis=1)
join_pv.shape # (16761588, 302)
join_pv_dataset.dtypes

#pref_web = pref_web.drop(['YearMonth'], axis=1)
pref_web = pref_web.rename(columns = {'YearMonth':'FEC_PREF_WEB'})

# Cruzar con df, crear variable de tiene porvenir_pref
join_pv_web = pd.merge(df,
                     pref_web,
                     how='left',
                     left_on=['IDENTIFICADOR'],
                     right_on = ['IDENTIFICADOR'])

join_pv_web.shape ## (16811884, 3
join_pv_web.dtypes
join_pv_dataset.shape
join_pv_web.columns


join_pv_web['YearMonth'] = join_pv_web['YearMonth'].astype(int)
join_pv_web['FEC_PREF_WEB'] = join_pv_web['FEC_PREF_WEB'].astype(str)

#join_pv_web['FEC_PREF_WEB'].value_counts()
#pd.to_datetime(join_pv_web['YearMonth'].apply(str)+'01', format='%Y%m%d', errors='coerce')
join_pv_web['FEC_PREF_WEB'].replace(np.nan, '209912')
#join_pv_web['FEC_PREF_WEB'] = \


join_pv_web['FEC_PREF_WEB'].fillna('222004')

join_pv_web['FEC_PREF_WEB']= join_pv_web['FEC_PREF_WEB'].astype(float)
join_pv_web['FEC_PREF_WEB']= join_pv_web['FEC_PREF_WEB'].fillna(222004)
join_pv_web['YearMonth'] = join_pv_web['YearMonth'].astype(float)

join_pv_web['TIENE_PREF_WEB'] = \
    np.where((join_pv_web['YearMonth'] >= join_pv_web['FEC_PREF_WEB']),
                                         1, 0 )


join_pv_web[(join_pv_web['FEC_PREF_WEB']=='201709.0') &
            (join_pv_web['TIENE_PREF_WEB']==1)].head(3)

#para validación de registros
# join_pv_web[(join_pv_web['FEC_PREF_WEB']>='2018-01-01') & (join_pv_web['TIENE_PREF_WEB']==1) ].head()


#17DBCBA216E15EFE22CA7897C2AF9B19
#para validación de registros
#
# join_pv_web[(join_pv_web['FEC_PREF_WEB']=='201711.0') & (join_pv_web['FEC_PREF_WEB']==1) ].head()
join_pv_web[(join_pv_web['FEC_PREF_WEB']=='201812.0')& (join_pv_web['FEC_PREF_WEB']==1)].head()

join_pv_web[(join_pv_web['FEC_PREF_WEB']==1)].head()

join_pv_web['FEC_PREF_WEB'].value_counts()
#aa1_1 = join_pv_web[join_pv_web['IDENTIFICADOR']=='DBCBA216E15EFE22CA7897C2AF9B19']

#aa3=pv[pv['IDENTIFICADOR']=='A93D3846326BB831AD5D0358C1FC9B14']
#aa1 = join_pv_web[join_pv_web['IDENTIFICADOR']=='A93D3846326BB831AD5D0358C1FC9B14']
#aa1.shape
#aa2.shape
#aa2 = join_pv_filter[join_pv_filter['IDENTIFICADOR']=='A93D3846326BB831AD5D0358C1FC9B14']

#### limpiar duplicados

#join_pv_web[join_pv_web[['IDENTIFICADOR','YearMonth']].duplicated()].count()
#join_pv_web[join_pv_web[['IDENTIFICADOR','YearMonth']].duplicated()].head()
#join_pv_web[(join_pv_web['IDENTIFICADOR']== '036EB1A57259D43126DF1DE1178E953E') & (join_pv_web['YearMonth'] == '2017-06-01')].to_csv('test_pv2.csv',sep='\t')

join_pv_web = join_pv_web.drop_duplicates()

join_pv_web.shape
join_pv_web['YearMonth'] = 100*join_pv_web['YearMonth'].dt.year + join_pv_web['YearMonth'].dt.month
join_pv_web['YearMonth'] = join_pv_web['YearMonth'].astype(str)

join_pv_web['YearMonth'].dtypes

join_pv_web_dataset \
    = pd.merge(dataset,
                     join_pv_web,
                     how='left',
                     left_on=['IDENTIFICADOR','YearMonth'],
                     right_on = ['IDENTIFICADOR','YearMonth'])

join_pv_dataset.columns
join_pv_dataset.shape
join_pv_dataset.dtypes





###


##

###
###
pref_movil = pref_movil.drop(['YearMonth'], axis=1)

join_pv_web_mov = pd.merge(join_pv_web,
                     pref_movil,
                     how='left',
                     left_on=['IDENTIFICADOR'],
                     right_on = ['IDENTIFICADOR'])
join_pv_web_mov.shape #(17047270, 304
print("Done, PI-WEB-MOVIL")


del(join_pv, join_pv_web, pref_movil, pv, pref_web)

ces = ces.drop(['CUENTA_ID_HASH','FECHA_ACTIVACION'], axis=1)
join_pv_web_mov_ces = pd.merge(join_pv_web_mov,
                     ces,
                     how='left',
                     left_on=['IDENTIFICADOR'],
                     right_on = ['IDENTIFICADOR_HASH'])

print(join_pv_web_mov_ces.shape) #(18725028, 306)

del(join_pv_web_mov,ces)


join_pv_web_mov_ces = join_pv_web_mov_ces.drop(['IDENTIFICADOR_HASH'], axis=1)

join_pv_web_mov_ces.to_hdf(INPUT_DATA_PATH + '20190915-final-master_table_churn-5cats_dummies-rem-leakage-ces-pv-pref' + '.h5',
                 key='df',
                 mode='w')

print('DONE \n Time elapsed : {a} minutes'.format(a=(time.time() - ini) / 60))

#with open(INPUT_DATA_PATH + '20190915-final-master_table_churn-5cats_dummies-rem-leakage-ces-pv-pref' + '.pkl', 'wb') as output:
#    pickle.dump(obj=join_pv_web_mov_ces, file=output)