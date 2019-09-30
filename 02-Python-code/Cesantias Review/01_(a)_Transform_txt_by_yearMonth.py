import os
import pandas as pd

input_path = '/mnt/s3-refined-porvenir/SG5/main//'
input_extension = '.txt'
output_path = '/mnt/work/datasets/SG5_partitions//'

os.listdir(input_path)
os.listdir(output_path)
import time
ini = time.time()


fechas =[201705, 201706, 201707, 201708, 201709, 201710, 201711,
 201712, 201801, 201802, 201803, 201804, 201805, 201806, 201807,
 201808, 201809, 201810, 201811, 201812, 201901, 201902, 201903,
 201904, 201905, 201906]

def check_folder_to_create(path, folder_to_check):
    if file not in os.listdir(os.path.join(path)):
        os.mkdir(os.path.join(path,folder_to_check))
        print('Se ha creado la carpeta en la ruta: {a}'.format(a=os.path.join(path,folder_to_check)))
    else:
        print('La carpeta ya existìa: {a}'.format(a=os.path.join(path,folder_to_check)))
    return;


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
    return;





files_header_dict = {'header':['sbgr5_cta_saldo_cuenta','sbgr5_ret_pago_solicitud_retiro',
                               'sbgr5_ret_solicitud_retiro','sbgr5_cta_cuenta_aporte',
                               'sbgr5_cta_cuenta','sbgr5_cta_cuenta_movimiento'],
                     'no header':['sbgr5_gen_valor_unidad','sbgr5_ret_parametrizacion_retiro']}
type(files_header_dict['no header'])

#file_names = ['sbgr5_ret_pago_solicitud_retiro','d']
for file in file_names:
    if file in files_header_dict['header']:
        print('Loading {a} with header'.format(a=file))
        cesantias_saldo = pd.read_csv(input_path + file + input_extension, sep='\|\|', encoding='latin1',engine='python')
        print('DONE \n Time elapsed reading file: {a}'.format(a=(time.time() - ini) / 60))
        print('Loaded {a} rows, and {b} columns'.format(a=cesantias_saldo.shape[0], b=cesantias_saldo.shape[1]))
        print(cesantias_saldo.dtypes)

        #Casteo de variables:
        if file == 'sbgr5_ret_pago_solicitud_retiro':
        ## Creación de columna de año-mes
            print("Cast data types for {a}".format(a=file))
            cesantias_saldo['FECHA_CREACION'] = pd.to_datetime(cesantias_saldo['FECHA_CREACION'].apply(str), format='%d-%b-%y', errors='coerce')
            cesantias_saldo['YearMonth'] = 100*cesantias_saldo['FECHA_CREACION'].dt.year +cesantias_saldo['FECHA_CREACION'].dt.month
            selected_columns=['CUENTA_ID_HASH', 'BANCO_ID_ORIGEN', 'ESTADO_PAGO_ID','FORMA_PAGO_ID','VALOR_SOLICITADO','VALOR_PAGO_NETO','VALOR_RENDIMIENTOS','VALOR_RET_FUENTE','VALOR_COMISIONES','VALOR_GRAVAMEN_FINANCIERO','FECHA_CREACION','NUM_SOLICITUD_RETIRO_ID','YearMonth',]
            data = cesantias_saldo.loc[:, selected_columns].drop_duplicates()
            del(cesantias_saldo)

            data['YearMonth'] = data['YearMonth'].astype('category')
            data['BANCO_ID_ORIGEN'] = data['BANCO_ID_ORIGEN'].astype('category')
            data['ESTADO_PAGO_ID'] = data['ESTADO_PAGO_ID'].astype('category')
            data['FORMA_PAGO_ID'] = data['FORMA_PAGO_ID'].astype('category')
            data['NUM_SOLICITUD_RETIRO_ID'] = data['NUM_SOLICITUD_RETIRO_ID'].astype('category')

            data['VALOR_SOLICITADO'] = data['VALOR_SOLICITADO'].astype('float')
            data['VALOR_PAGO_NETO'] = data['VALOR_PAGO_NETO'].astype('float')
            data['VALOR_RENDIMIENTOS'] = data['VALOR_RENDIMIENTOS'].astype('float')
            data['VALOR_RET_FUENTE'] = data['VALOR_RET_FUENTE'].astype('float')
            data['VALOR_COMISIONES'] = data['VALOR_COMISIONES'].astype('float')
            data['VALOR_GRAVAMEN_FINANCIERO'] = data['VALOR_GRAVAMEN_FINANCIERO'].astype('float64')

            check_folder_to_create_and_write_partitions(df=data, periodos=fechas, path=output_path, folder_to_check=file)
        elif file =='sbgr5_cta_saldo_cuenta':
            print("Cast data types for {a}".format(a=file))
            cesantias_saldo['FECHA_CREACION_format'] = pd.to_datetime(cesantias_saldo['FECHA_CREACION'].apply(str),format='%d-%b-%y', errors='coerce')
            cesantias_saldo['YearMonth'] = 100 * cesantias_saldo['FECHA_CREACION_format'].dt.year + cesantias_saldo['FECHA_CREACION_format'].dt.month

            selected_columns = ['CUENTA_ID_HASH', 'INVERSION_ID', 'SALDO_UNIDADES', 'SALDO_PESOS', 'FONDO_ID','FECHA_CREACION_format', 'YearMonth']
            data = cesantias_saldo.loc[:, selected_columns].drop_duplicates()

            del(cesantias_saldo)

            data['YearMonth'] = data['YearMonth'].astype('category')
            data['INVERSION_ID'] = data['INVERSION_ID'].astype('category')

            data['SALDO_UNIDADES'] = data['SALDO_UNIDADES'].astype('float64')
            data['SALDO_PESOS'] = data['SALDO_PESOS'].astype('float64')

            data['FONDO_ID'] = data['FONDO_ID'].astype('category')


            check_folder_to_create_and_write_partitions(df=data, periodos=fechas, path=output_path,folder_to_check=file)
        elif file == 'sbgr5_ret_solicitud_retiro':
            print("-- Cast data types for {a} \n".format(a=file))
            cesantias_saldo['FECHA_SOLICITUD'] = pd.to_datetime(cesantias_saldo['FECHA_SOLICITUD'].apply(str),format='%d-%b-%y', errors='coerce')
            cesantias_saldo['FECHA_AUTORIZACION'] = pd.to_datetime(cesantias_saldo['FECHA_AUTORIZACION'].apply(str),format='%d-%b-%y', errors='coerce')
            cesantias_saldo['YearMonth'] = 100 * cesantias_saldo['FECHA_AUTORIZACION'].dt.year + cesantias_saldo['FECHA_AUTORIZACION'].dt.month

            data = cesantias_saldo.drop_duplicates()
            del (cesantias_saldo)

            data['YearMonth'] = data['YearMonth'].astype('category')
            data['ESTADO_RETIRO_ID'] = data['ESTADO_RETIRO_ID'].astype('category')
            data['PARAMETRIZACION_RETIRO_ID'] = data['PARAMETRIZACION_RETIRO_ID'].astype('category')
            data['NUM_SOLICITUD_RETIRO_ID'] = data['NUM_SOLICITUD_RETIRO_ID'].astype('category')

            data['VALOR_RETIRO_NETO'] = data['VALOR_RETIRO_NETO'].astype('float64')
            data['VALOR_RETIRO_SOLICITADO'] = data['VALOR_RETIRO_SOLICITADO'].astype('float64')
            data['VALOR_RETIRO_APLICADO'] = data['VALOR_RETIRO_APLICADO'].astype('float64')
            data['VALOR_DESCUENTOS'] = data['VALOR_DESCUENTOS'].astype('float64')

            check_folder_to_create_and_write_partitions(df=data, periodos=fechas, path=output_path, folder_to_check=file)
            ## end

        elif file == 'sbgr5_cta_cuenta_aporte':
            print("-- Cast data types for {a} \n".format(a=file))
            cesantias_saldo['FECHA_PAGO'] = pd.to_datetime(cesantias_saldo['FECHA_PAGO'].apply(str), format='%d-%b-%y',errors='coerce')
            cesantias_saldo['YearMonth'] = 100 * cesantias_saldo['FECHA_PAGO'].dt.year + cesantias_saldo['FECHA_PAGO'].dt.month

            data = cesantias_saldo.drop_duplicates()
            del (cesantias_saldo)

            data['YearMonth'] = data['YearMonth'].astype('category')
            data['INDICADOR_DISPONIBLE'] = data['INDICADOR_DISPONIBLE'].astype('category')
            data['CUENTA_APORTE_ID'] = data['CUENTA_APORTE_ID'].astype('category')
            data['VALOR_PESOS'] = data['VALOR_PESOS'].astype('float64')
            data['VALOR_UNIDADES'] = data['VALOR_UNIDADES'].astype('float64')

            check_folder_to_create_and_write_partitions(df=data, periodos=fechas, path=output_path, folder_to_check=file)
            ## end

        elif file == 'sbgr5_cta_cuenta_movimiento':
            print("-- Cast data types for {a} \n".format(a=file))
            #Tabla sin fecha
            data = cesantias_saldo.loc[:,].drop_duplicates()
            del (cesantias_saldo)

            data['CUENTA_ID_HASH'] = data['CUENTA_ID_HASH'].astype('category')
            data['CUENTA_APORTE_ID'] = data['CUENTA_APORTE_ID'].astype('category')
            data['CUENTA_MOVIMIENTO_ID'] = data['CUENTA_MOVIMIENTO_ID'].astype('category')
            data['CONCEPTO_MOVIMIENTO_ID'] = data['CONCEPTO_MOVIMIENTO_ID'].astype('category')

            check_folder_to_create(output_path,file)
            data.to_parquet(fname=os.path.join(output_path, file) + '//' + file + '_' + '.snappy.parquet', compression='snappy')
        elif file == 'sbgr5_cta_cuenta':
            #print("-- WARNING: CHANGE CTA_CUENTA.TXT FILE FOR NEW ONE\n")
            print("-- Cast data types for {a} \n".format(a=file))

            cesantias_saldo['FECHA_INACTIVACION'] = pd.to_datetime(cesantias_saldo['FECHA_INACTIVACION'].apply(str), format='%d-%b-%y',errors='coerce')
            cesantias_saldo['FECHA_ACTIVACION'] = pd.to_datetime(cesantias_saldo['FECHA_ACTIVACION'].apply(str), format='%d-%b-%y',errors='coerce')
            cesantias_saldo['YearMonth'] = 100 * cesantias_saldo['FECHA_ACTIVACION'].dt.year + cesantias_saldo['FECHA_ACTIVACION'].dt.month

            data = cesantias_saldo.loc[:, ].drop_duplicates()
            del (cesantias_saldo)

            data['YearMonth'] = data['YearMonth'].astype('category')

            data['NRO_ID_EMPLEADOR_HASH'] = data['NRO_ID_EMPLEADOR_HASH'].astype('category')
            data['IDENTIFICADOR_HASH'] = data['IDENTIFICADOR_HASH'].astype('category')
            data['ESTADO_CUENTA_ID'] = data['ESTADO_CUENTA_ID'].astype('category')

            check_folder_to_create_and_write_partitions(df=data, periodos=fechas, path=output_path,
                                                        folder_to_check=file)



    elif file in files_header_dict['no header']:
        #Cargar archivos sin header
        print('Loading {a} without header'.format(a=file))

        if file == 'sbgr5_gen_valor_unidad':
        ## Creación de columna de año-mes
            cesantias_saldo = pd.read_csv(input_path + file + input_extension, sep='\|\|', encoding='latin1',engine='python', header=None)
            print('DONE \n Time elapsed reading file: {a}.'.format(a=(time.time() - ini) / 60))
            print('Loaded {a} rows, and {b} columns'.format(a=cesantias_saldo.shape[0], b=cesantias_saldo.shape[1]))
            print(cesantias_saldo.dtypes)
            print("WARNING: ARCHIVO SIN FECHAS NI ENCABEZADO{a}".format(a=file))
            #Cast vars here
            #check_folder_to_create_and_write_partitions(df=data, periodos=fechas, path=output_path, folder_to_check=file)
        elif file == 'sbgr5_ret_parametrizacion_retiro':
            cesantias_saldo = pd.read_csv(input_path + file + input_extension, sep='\|\|', encoding='latin1',engine='python',header=None, skipinitialspace=True, quotechar='"')
            print('DONE \n Time elapsed reading file: {a}.'.format(a=(time.time() - ini) / 60))
            print('Loaded {a} rows, and {b} columns'.format(a=cesantias_saldo.shape[0], b=cesantias_saldo.shape[1]))
            print(cesantias_saldo.dtypes)
            print("WARNING: ARCHIVO SIN FECHAS NI ENCABEZADO{a}".format(a=file))
            #check_folder_to_create_and_write_partitions(df=data, periodos=fechas, path=output_path, folder_to_check=file)
    else:
        print("ºººº Archivo por fuera de diccionario: {a}".format(a=file))


os.listdir(output_path+'/sbgr5_ret_pago_solicitud_retiro')

## test
#aa_fecha = '201806'
#aa_path = os.path.join(output_path, file) + '//' +file+ '_' + str(aa_fecha ) + '.snappy.parquet'

#aa = pd.read_parquet(aa_path)
#aa.shape
