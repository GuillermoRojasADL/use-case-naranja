import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta

# folders names definitions
client_input_data_folder = '/mnt/s3-refined-porvenir/Clientes/h5/'
# files names definitions
client_input_data = \
    ['sbgr1_ps_aa_rd_person',
     'sbgr1_ps_bc',
     'sbgr1_ps_por_habdata_tbl',
     'sbgr1_ps_por_salotr_tbl',
     'sbgr1_ps_por_inf_com_tbl',
     'sbgr1_ps_rd_person',
     'sbgr1_ps_bo_role',
     'sbgr1_ps_bo_cm']
########################
client_df =\
    pd.read_hdf(client_input_data_folder+client_input_data[0]+'.h5', 'df')
print(client_df.shape)

print(client_df.columns)
print(len(client_df['IDENTIFICADOR'].unique()))
print(len(client_df['BO_ID'].unique()))
print(client_df['IDENTIFICADOR'].isna().sum())
print(client_df['BO_ID'].isna().sum())
print(client_df['DANE_NACIMIENTO'].isna().sum())
###############
client_df =\
    pd.read_hdf(client_input_data_folder+client_input_data[1]+'.h5', 'df')
print(client_df.shape)
print(client_df.groupby('SEGMENTO').count())


print(client_df.columns)
print(len(client_df['BO_ID'].unique()))
print(client_df['BO_ID'].duplicated().sum())
print(client_df['BO_ID'].isna().sum())

print(client_df.columns)
print(len(client_df['SEGMENTO'].unique()))
print(client_df['SEGMENTO'].duplicated().sum())
print(client_df['SEGMENTO'].isna().sum())

###############
client_df =\
    pd.read_hdf(client_input_data_folder+client_input_data[2]+'.h5', 'df')
print(client_df.shape)

print(client_df.columns)
print(len(client_df['BO_ID'].unique()))
print(client_df['BO_ID'].duplicated().sum())
print(client_df['BO_ID'].isna().sum())

print(len(client_df['AUDIT_STAMP'].unique()))
print(client_df['AUDIT_STAMP'].duplicated().sum())
print(client_df['AUDIT_STAMP'].isna().sum())

client_df['DATE'] =\
    pd.to_datetime(client_df['AUDIT_STAMP'].str[0:9].astype(str), format = '%d-%b-%y', errors='coerce')
print(client_df['DATE'].min())
print(client_df['DATE'].max())

print(client_df.columns)
print(len(client_df['POR_HABEASDATA_FLG'].unique()))
print(client_df['POR_HABEASDATA_FLG'].duplicated().sum())
print(client_df['POR_HABEASDATA_FLG'].isna().sum())

###############
client_df =\
    pd.read_hdf(client_input_data_folder+client_input_data[3]+'.h5', 'df')
print(client_df.shape)
print(client_df.columns)

print(len(client_df['BO_ID'].unique()))
print(client_df['BO_ID'].duplicated().sum())
print(client_df['BO_ID'].isna().sum())

print(len(client_df['EFFDT'].unique()))
print(client_df['EFFDT'].duplicated().sum())
print(client_df['EFFDT'].isna().sum())

print(len(client_df['AA_CODPROFESSION'].unique()))
print(client_df['AA_CODPROFESSION'].duplicated().sum())
print(client_df['AA_CODPROFESSION'].isna().sum())

print(len(client_df['DESCR100_1'].unique()))
print(client_df['DESCR100_1'].duplicated().sum())
print(client_df['DESCR100_1'].isna().sum())

###############
client_df =\
    pd.read_hdf(client_input_data_folder+client_input_data[4]+'.h5', 'df')
print(client_df.shape)
print(client_df.columns)

print(len(client_df['BO_ID'].unique()))
print(client_df['BO_ID'].duplicated().sum())
print(client_df['BO_ID'].isna().sum())

print(len(client_df['FECHA_NACIMIENTO'].unique()))
print(client_df['FECHA_NACIMIENTO'].dropna().duplicated().sum())
print(client_df['FECHA_NACIMIENTO'].isna().sum())

print(len(client_df['GENERO'].unique()))
print(client_df['GENERO'].dropna().duplicated().sum())
print(client_df['GENERO'].isna().sum())

client_df['DATE'] =\
    pd.to_datetime(client_df['FECHA_NACIMIENTO'], format = '%Y-%m-%d', errors='coerce')

print(pd.to_datetime(client_df['FECHA_NACIMIENTO'], format = '%Y-%m-%d', errors='coerce').min())
print(pd.to_datetime(client_df['FECHA_NACIMIENTO'], format = '%Y-%m-%d', errors='coerce').max())
###############
client_df =\
    pd.read_hdf(client_input_data_folder+client_input_data[5]+'.h5', 'df')
print(client_df.shape)
print(client_df.columns)

print(len(client_df['BO_ID'].unique()))
print(client_df['BO_ID'].duplicated().sum())
print(client_df['BO_ID'].isna().sum())

print(len(client_df['FECHA_NACIMIENTO'].unique()))
print(client_df['FECHA_NACIMIENTO'].dropna().duplicated().sum())
print(client_df['FECHA_NACIMIENTO'].isna().sum())

print(len(client_df['GENERO'].unique()))
print(client_df['GENERO'].dropna().duplicated().sum())
print(client_df['GENERO'].isna().sum())

###############
client_df =\
    pd.read_hdf(client_input_data_folder+client_input_data[6]+'.h5', 'df')
print(client_input_data[6])
print(client_df.shape)
print(client_df.columns)

print(len(client_df['BO_ID'].unique()))
print(client_df['BO_ID'].dropna().duplicated().sum())
print(client_df['BO_ID'].isna().sum())

print(len(client_df['TIPO_CLIENTE'].unique()))
print(client_df['TIPO_CLIENTE'].dropna().duplicated().sum())
print(client_df['TIPO_CLIENTE'].isna().sum())

print(len(client_df['FECHA_REGISTRO'].unique()))
print(client_df['FECHA_REGISTRO'].dropna().duplicated().sum())
print(client_df['FECHA_REGISTRO'].isna().sum())

print(client_df['FECHA_REGISTRO'].min())
print(client_df['FECHA_REGISTRO'].max())

###############
client_df =\
    pd.read_csv('/mnt/s3-refined-porvenir/Clientes/sbgr1_ps_cm.txt',
                sep = '\|\|',
                encoding = 'utf-8')
print(client_df.shape)
print(client_df.columns)


print(len(client_df['CM_ID'].unique()))
print(client_df['CM_ID'].dropna().duplicated().sum())
print(client_df['CM_ID'].isna().sum())

print(len(client_df['CITY'].unique()))
print(client_df['CITY'].dropna().duplicated().sum())
print(client_df['CITY'].isna().sum())

x = client_df[ ['CITY' , 'CM_ID']].groupby('CITY').count()
pd.DataFrame(x).to_csv('/mnt/s3-refined-porvenir/Clientes/CITY.csv', sep=';')
#####
for filename in client_input_data[1:]:
    data =\
        pd.read_hdf(client_input_data_folder+filename+'.h5', 'df')
    print(data.shape)
    client_df = pd.merge(client_df, data, how='left',
                       left_on=['BO_ID'],
                       right_on=['BO_ID'])
    print(client_df.shape)
client_df.to_hdf(client_input_data_folder + "client_dataset.h5",
                 key='df',
                 mode='w')
########################
