import os
import numpy as np
import pandas as pd
import time


def check_folder_to_create_and_write_partitions(df,periodos, path, folder_to_check):
    ini = time.time()
    if file not in os.listdir(os.path.join(path)):
        os.mkdir(os.path.join(path,folder_to_check))
        print('Se ha creado la carpeta en la ruta: {a}'.format(a=os.path.join(path,folder_to_check)))
    else:
        print('La carpeta ya existìa: {a}'.format(a=os.path.join(path,folder_to_check)))

    for fecha in periodos:
        print("particionando fecha:{a} \n en la carpeta: {b}".format(a=fecha, b=os.path.join(output_path, folder_to_check)))
        data_part = df.loc[df['YearMonth'] == fecha]
        print('Subset date {fec} data has: {a} rows and {b} columns'.format(fec=fecha, a=data_part.shape[0],b=data_part.shape[1]))
        print(">> writting parquet <<")
        data_part.to_parquet(fname=os.path.join(output_path, folder_to_check) + '//' + folder_to_check + '_' + str(fecha) + '.snappy.parquet',compression='snappy')
        del (data_part)
        print('DONE \n writed parquet: {c}. \n time elapsed: {a} \n'.format(a=(time.time() - ini) / 60, c=os.path.join(output_path,file) + '//' + file + '_' + str(fecha) + '.snappy.parquet'))
    print('Total time to write partitions: {a} \n for file {b} \n'.format(a=((time.time() - ini) / 60), b=file))
    return

input_path = '/mnt/s3-refined-porvenir/SG8/main//'
input_extension = '.txt'
output_path = '/mnt/work/datasets/SG8_partitions///'

txt_names = [x for x in os.listdir(input_path)]
file_names = [x[:-4] for x in os.listdir(input_path)]


fechas =[201705, 201706, 201707, 201708, 201709, 201710, 201711,
 201712, 201801, 201802, 201803, 201804, 201805, 201806, 201807,
 201808, 201809, 201810, 201811, 201812, 201901, 201902, 201903,
 201904, 201905, 201906]


txt_names
file_names






ini = time.time()

files_header_dict = {'header':['sbgr8_bon_bono_pensional','sbgr8_bon_caso_x_afiliado','sbgr8_bon_solicitud_obp'],
                     'no header':['None']}


file = 'sbgr8_bon_bono_pensional'



cesantias_saldo = pd.read_csv(input_path + file + input_extension, sep='\|\|', encoding='latin1',engine='python')
print('DONE \n Time elapsed reading file: {a}'.format(a=(time.time() - ini) / 60))
print('Loaded {a} rows, and {b} columns'.format(a=cesantias_saldo.shape[0], b=cesantias_saldo.shape[1]))
print(cesantias_saldo.dtypes)

cesantias_saldo['FECHA_CREACION'].head(2)

#Casteo de variables:

## Creación de columna de año-mes
print("Cast data types for {a}".format(a=file))
col_dates = ['FECHA_CREACION','FECHA_CAUSA_REDEN_ANTICIPADA',
             'FECHA_REDENCION_NORMAL', 'FECHA_CORTE', 'FECHA_EMISION_RECONOCIMIENTO']
for col in col_dates:
    cesantias_saldo[col] = pd.to_datetime(cesantias_saldo[col].apply(str), format='%d-%b-%y', errors='coerce')


cesantias_saldo['YearMonth'] = 100*cesantias_saldo['FECHA_CREACION'].dt.year +cesantias_saldo['FECHA_CREACION'].dt.month
data = cesantias_saldo.drop_duplicates()

data.shape[0] == cesantias_saldo.shape[0]
del(cesantias_saldo)

col_cats = ['BONO_PENSIONAL_ID', 'CASO_AFILIADO_ID', 'TIPO_BONO',
            'VERSION_BONO']
cols_num = ['VALOR_FECHA_EMISION', 'VALOR_FECHA_REDENCION','VALOR_BONO_FECHA_CORTE']


data['YearMonth'] = data['YearMonth'].astype('category')

for col in col_cats:
    data[col] = data[col].astype('category')


for col in cols_num:
    data[col] = data[col].astype('float')


check_folder_to_create_and_write_partitions(df=data, periodos=fechas, path=output_path, folder_to_check=file)

## check parquet

os.listdir(output_path+file)
aa = pd.read_parquet(output_path+file)
aa.dtypes
aa.shape == data.loc[data['YearMonth'].isin(fechas)].shape
#data.shape



#
#
#
#
#
#


file = 'sbgr8_bon_solicitud_obp'

cesantias_saldo = pd.read_csv(input_path + file + input_extension, sep='\|\|', encoding='latin1',engine='python')
print('DONE \n Time elapsed reading file: {a}'.format(a=(time.time() - ini) / 60))
print('Loaded {a} rows, and {b} columns'.format(a=cesantias_saldo.shape[0], b=cesantias_saldo.shape[1]))
print(cesantias_saldo.dtypes)

cesantias_saldo['FECHA_TRASLADO'].head(2)

#Casteo de variables:

## Creación de columna de año-mes
print("Cast data types for {a}".format(a=file))
col_dates = ['FECHA_TRASLADO','FECHA_MUERTE_INVALIDEZ']
for col in col_dates:
    cesantias_saldo[col] = pd.to_datetime(cesantias_saldo[col].apply(str), format='%d-%b-%y', errors='coerce')

cesantias_saldo['YearMonth'] = 100*cesantias_saldo[col_dates[0]].dt.year +cesantias_saldo[col_dates[0]].dt.month
data = cesantias_saldo.drop_duplicates()

data.shape[0] == cesantias_saldo.shape[0]

del(cesantias_saldo)

col_cats = ['SOLICITUD_ID', 'CASO_AFILIADO_ID', 'CONSECUTIVO_SOLICITUD_OBP',
            'CONSECUTIVO_LIQUIDACION','VERSION_BONO','CONSECUTIVO_SOLICITUD_ENTIDAD']
cols_num = ['VALOR_BONO']

data['YearMonth'] = data['YearMonth'].astype('category')

for col in col_cats:
    data[col] = data[col].astype('category')


for col in cols_num:
    data[col] = data[col].replace('"', np.nan)
    data[col] = data[col].astype('float')

data[data['VALOR_BONO']==np.nan].shape

data.dtypes
check_folder_to_create_and_write_partitions(df=data, periodos=fechas, path=output_path, folder_to_check=file)

## check parquet

os.listdir(output_path+file)
aa = pd.read_parquet(output_path+file)
aa.dtypes
aa.shape == data.loc[data['YearMonth'].isin(fechas)].shape
#data.shape

## check parquet

os.listdir(output_path+file)
aa = pd.read_parquet(output_path+file)
aa.dtypes
aa.shape == data.loc[data['YearMonth'].isin(fechas)].shape

