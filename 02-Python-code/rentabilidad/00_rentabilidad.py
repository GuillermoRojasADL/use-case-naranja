
import numpy as np
import pandas as pd
import os
import pickle



#input_path = '/mnt/s3-refined-porvenir/Clientes/h5/temp_files/datasets'
input_ext = '.pkl'
output_path = '/mnt/work/datasets/'

fec_nac_file = 'dataset_clients_sample_'
fec_nac_file2 = 'sbgr1_ps_rd_person.txt'
ibc_file = 'sbgr4_cta_cuenta.txt'

id_sg1_file = 'sbgr1_ps_aa_rd_person.txt'
id_sg4_file = 'sbgr4_cta_afiliado.txt'
city_file = 'sbgr1_ps_bo_cm.txt'
cm_file = 'sbgr1_ps_cm.txt'


rentabilidad_file = 'rentabilidad_cliente_po.csv'


po_file = 'dateset_PO_201905.pkl'
#'sbgr1_ps_rd_person'

date = '201903'
#with open(os.path.join(input_path, fec_nac_file + date + input_ext), 'rb') as input:
#    df1 = pickle.load(file=input)

#archivo con edad y genero
df1 = pd.read_csv(os.path.join('/mnt/s3-refined-porvenir/Clientes/', fec_nac_file2),sep='\|\|', encoding='latin1',engine='python')
df1.shape
# BOGOTA = 11001

df1['FECHA_NACIMIENTO'] = pd.to_datetime(df1['FECHA_NACIMIENTO'].apply(str), format='%Y-%m-%d', errors='coerce')
df1['FECHA_NACIMIENTO'].isnull().sum()
df1 = df1.dropna(subset=['FECHA_NACIMIENTO'])
#AGE column
df1['EDAD'] = df1['FECHA_NACIMIENTO'].apply(lambda x: 2019-x.year)

# ciudad file load
df1_city = pd.read_csv(os.path.join('/mnt/s3-refined-porvenir/Clientes/', city_file),sep='\|\|', encoding='latin1',engine='python')
df1_city = df1_city[df1_city['BO_CM_END_DT'] == '2999-12-31']

df1_city.dtypes

# identificación de ciudades
df1_cm_city = pd.read_csv(os.path.join('/mnt/s3-refined-porvenir/Clientes/', cm_file),sep='\|\|', encoding='latin1',engine='python')
df1_cm_city.dtypes
df1_cm_city.shape

df1_join_city = pd.merge(df1_city,
                     df1_cm_city,
                     how='inner',
                     left_on=['CM_ID'],
                     right_on = ['CM_ID'])

df1_join_city.dtypes
df1_join_city.shape


df1_join_city_edad_sexo = pd.merge(df1,
                     df1_join_city,
                     how='inner',
                     left_on=['BO_ID'],
                     right_on = ['BO_ID'])

df1_join_city_edad_sexo.dtypes
df1_join_city_edad_sexo.shape

df1_join_city_edad_sexo['CITY'].value_counts()


#identificacion
df1_id = pd.read_csv(os.path.join('/mnt/s3-refined-porvenir/Clientes/', id_sg1_file),sep='\|\|', encoding='latin1',engine='python')

df1_join_final = pd.merge(df1_join_city_edad_sexo,
                     df1_id,
                     how='inner',
                     left_on=['BO_ID'],
                     right_on = ['BO_ID'])


#
df1_join_final = df1_join_final.drop_duplicates(subset=['IDENTIFICADOR'])




#PO
df2 = pd.read_csv(os.path.join('/mnt/s3-refined-porvenir/Sg4/', ibc_file),sep='\|\|', encoding='latin1',engine='python')
#Cast date
df2['ULTIMA_FECHA_PAGO'].head(6)
df2['ULTIMA_FECHA_PAGO'] = pd.to_datetime(df2['ULTIMA_FECHA_PAGO'].apply(str), format='%d-%b-%y', errors='coerce')
df2['YearMonth'] = 100*df2['ULTIMA_FECHA_PAGO'].dt.year + df2['ULTIMA_FECHA_PAGO'].dt.month

df2['YearMonth'].head(2)


df2_1 = pd.read_csv(os.path.join('/mnt/s3-refined-porvenir/Sg4/', id_sg4_file),sep='\|\|', encoding='latin1',engine='python')

df2_1.shape
df2_1.dtypes

df2_join = pd.merge(df2,
                     df2_1,
                     how='inner',
                     left_on=['AFILIADO_FONDO_ID'],
                     right_on = ['AFILIADO_FONDO_ID'])


df2_join.shape
df2_join.dtypes


#### tabla final
rent = pd.merge(df1_join_final,
                 df2_join,
                 how='inner',
                 left_on=['IDENTIFICADOR'],
                 right_on = ['IDENTIFICADOR'])

rent.shape
rent.dtypes

#rent['GENERO'].value_counts()
# Crear variable REGION (bogota 11001; e.o.c. RP)
rent_cols = ['IDENTIFICADOR','CUENTA_ID_ANONI','CITY','GENERO','EDAD','ULTIMO_IBC_PAGO']
df = rent[rent_cols][rent['ULTIMO_IBC_PAGO']>600000]


### Creación de variables segun excel.

df['REGION'] = np.where(df['CITY']=='11001', 'B', 'RP')
df['IBC'] = df['ULTIMO_IBC_PAGO'] / 828116
df['GENERO'] = np.where(df['GENERO']=='M', 'H',
                        np.where(df['GENERO']=='F', 'M', np.NaN))

# carga del archivo de excel con los valores de las categorias.
conditions = pd.read_csv(os.path.join('/mnt/s3-refined-porvenir/Clientes/', rentabilidad_file),sep=';', encoding='latin1',engine='python')

conditions.shape
conditions.columns

df['Valor'] = 0
df.columns
conditions.columns
for idx in range(conditions.shape[0]):
    print(idx)
    df.loc[(df['REGION'] == conditions['Region'].iloc[idx]) & \
           (df['GENERO'] == conditions['Genero'].iloc[idx]) & \
           (df['EDAD'] >= conditions['EDAD_MIN'].iloc[idx]) & \
           (df['EDAD'] <= conditions['EDAD_MAX'].iloc[idx]) & \
           (df['IBC'] >= conditions['IBC_MIN'].iloc[idx]) & \
           (df['IBC'] <= conditions['IBC_MAX'].iloc[idx]), 'Valor'] =\
        conditions['Valor'].iloc[idx]



#cargar archivo de PO
with open(os.path.join('/mnt/s3-refined-porvenir/Clientes/h5/temp_files/datasets', po_file), 'rb') as input:
    po_salario = pickle.load(file=input)

po_salario.dtypes
# Merge

value_df = pd.merge(df,
                 po_salario,
                 how='inner',
                 left_on=['CUENTA_ID_ANONI'],
                 right_on = ['CUENTA'])

value_df.shape

#calculo de Valor del cliente
value_df["Value_x_Cli"] = value_df["Valor"] * value_df["SALARIO_BASE"]


value_df[['Valor','SALARIO_BASE','Value_x_Cli']].head(3)

final_cols = ['IDENTIFICADOR','CUENTA_ID_ANONI','REGION','GENERO','EDAD','IBC','Value_x_Cli']
value_df[final_cols].dtypes

#Write results
value_df[final_cols].shape
value_df[final_cols].to_parquet(fname = os.path.join(output_path, 'value_per_client' + '.snappy.parquet'), compression='snappy')