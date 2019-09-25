import pandas as pd
import os

input_path = '/mnt/work/datasets'
input_ext = '.csv'


FILE_MDT_PI = os.path.join('/mnt/work/mdt', '20190827-final-master_table_churn-5cats_dummies-rem-leakage-PI' + '.snappy.parquet')
FILE_EMPRESA = os.path.join('/mnt/s3-refined-porvenir/Sg4','sbgr4_cta_empleador.txt')

FILE_RENTABILIDAD = 'CLIENTES_IDS_PROB_VALOR_EDAD_BOGOTA'

#load df
df_rent = pd.read_csv(os.path.join(input_path, FILE_RENTABILIDAD + input_ext),sep=',', encoding='latin1',engine='python')
df_pi = pd.read_parquet(FILE_MDT_PI,engine='pyarrow')
df_empresa = pd.read_csv(FILE_EMPRESA  ,sep='\|\|', encoding='latin1',engine='python')

df_rent.loc[(df_rent['BOGOTA'] == 1),:].shape #(905008, 13)

df_rent = df_rent.loc[(df_rent['BOGOTA'] == 1),:].copy()

cols_pi = ['IDENTIFICADOR','TIENE_PV']
df_pi = df_pi.loc[:,cols_pi]

df_pi = df_pi.groupby('IDENTIFICADOR')['TIENE_PV'].max()
df_pi = df_pi.reset_index()

df_rent.dtypes
df_pi.shape
df_empresa.shape

df_rent_pi = pd.merge(df_rent,
                     df_pi,
                     how='left',
                     left_on=['IDENTIFICADOR'],
                     right_on = ['IDENTIFICADOR'])


print(df_rent_pi.shape)
df_rent_pi.drop(columns=['Unnamed: 0','index'], inplace=True)
print(df_rent_pi.columns)


df_rent_pi_empresa =  pd.merge(df_rent_pi,
                     df_empresa,
                     how='left',
                     left_on=['IDENTIFICADOR'],
                     right_on = ['IDENTIFICADOR'])

df_rent_pi_empresa.shape



df_rent_pi_empresa.to_csv(os.path.join(input_path, 'CLIENTES_IDS_PROB_VALOR_EDAD_BOGOTA_PI_EMPRESA.txt'))