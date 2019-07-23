import sys
import pandas as pd
import os
import numpy as np
pd.options.mode.chained_assignment = None  # Disable annoying pandas warning
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")
###################
# transform text files to h5 files
INPUT_EXTENSION = '.txt'
OUT_EXTENSION = 'h5'

INPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Sg4/'
FILE_NAME = 'SF_SUPPORT'
OUTPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Sg4/results/'
                                                          
import pandas as pd
import numpy as np
import unidecode
import re
import datetime

######################
def full_describe_categorical(dataframe, variables="all", variability=20, completeness=10, h_cardinality=50):
    """
    Parameters description:
    dataframe: Table to be used in DataFrame format
    variables: Variables to perform the describe method. Available values: "all", type sequence (ie. '1:8'),
    type lis of numbers or variables labels (ie. '1,4,9' o 'var1,var2,var3')
    completeness: Minimum completeness accepted for variables (value betwen 0 y 100)
    variability: Minimum variablility accepted for variables (ie. 0,2,10,20,50,100)
    h_cardinality: Value to establish when a variable has high cardinality problems
    """
    print("Describe procedure starts, Time: " + datetime.datetime.now().strftime("%H:%M:%S"))
    start1 = datetime.datetime.now()
    inicial_varnames = dataframe.columns
    dataframe = dataframe.select_dtypes(include='object')
    if variables == "all":  # If desired for all the variables
        # Calculate describe adding (concatenar,concatenate) the missing count
        summary = pd.concat([dataframe.describe(include="all"), dataframe.isnull().sum().to_frame(name='missing').T],
                            sort=False)
        variables_serie = dataframe.columns
    elif ":" in variables:  # If desired for sequence of variables n1:n2
        # Calculate describe adding (concatenar,concatenate) the missing count
        variables_serie = list(pd.to_numeric((variables).split(':')))
        variables_serie = list(pd.Series(range(variables_serie[0], variables_serie[1])))
        summary = pd.concat([dataframe.iloc[:, variables_serie].describe(include="all"),
                             dataframe.iloc[:, variables_serie].isnull().sum().to_frame(name='missing').T], sort=False)
    else:
        try:  # If desired for numeric array (posiciones,positions) of variables [n1,n2,n3,...]
            # Dividir valor de variables por coma, creando lista numerica
            variables_serie = list(pd.to_numeric((variables).split(',')))
            # Calculate describe adding (concatenar,concatenate) the missing count
            summary = pd.concat([dataframe.iloc[:, variables_serie].describe(include="all"),
                                 dataframe.iloc[:, variables_serie].isnull().sum().to_frame(name='missing').T],
                                sort=False)
        except:
            # Create list from string separated for comma
            variables_serie = list(variables.split(","))
            # Check if exists any column name which doesn't exist in the dataframe (in,contains,contiene,valor en)
            if set(list(variables_serie)).issubset(list(inicial_varnames)):
                # Leave only the column names that exist in the dataframe (intersection,interseccion,valores comunes,common values)
                variables_serie = list(set(variables_serie) & set(dataframe.columns))
            if set(list(variables_serie)).issubset(list(
                    dataframe.columns)):  # If desired for string array (colomn names) of variables ['nam1','nam2','nam3',...]
                # Calculate describe adding (concatenar,concatenate) the missing count
                summary = pd.concat([dataframe.loc[:, variables_serie].describe(include="all"),
                                     dataframe.loc[:, variables_serie].isnull().sum().to_frame(name='missing').T],
                                    sort=False)
            else:  # Garbage Collector in other case
                summary = "Invalid entry for 'variables' parameter"

    print("Describe procedure finished. Elapsed time: " + str((datetime.datetime.now() - start1).seconds) + " secs")

    start2 = datetime.datetime.now()
    print("Starts completeness and variability calculation, Time: " + datetime.datetime.now().strftime("%H:%M:%S"))
    if isinstance(summary,
                  pd.DataFrame) == True:  # Verifiying if there is any error in the parameters and checking ig summary was created
        # Calculate total count
        summary.loc['total count'] = summary.loc[['count', 'missing']].sum()
        # Calculation of percentage of missing
        summary.loc['% missing'] = np.around(
            (summary.loc['missing'] * 100 / summary.loc['total count']).astype(np.double), 4)

        # Transpose data
        summary = summary.T

        # Include ID variables
        summary.insert(loc=0, column='variable', value=list(summary.index))

        # Calculation of variability
        # Fill NaN values with "missing" category (rellenar misssing, fill missing, imputar missing)
        dataframe = dataframe.fillna("missing")
        # Calculate frequency table for all variables
        frequences = dataframe.apply(pd.value_counts)

        extended_freq_table = []
        frequences_reduced = []
        for column in variables_serie:  # Iterate over dataframe to calculate frequencies per variable
            extended_freq_table.append(frequences_table(column, frequences)['frequences'])
            frequences_reduced.append(frequences_table(column, frequences)['frequences_reduced'])

        # Concatenate values from the list in a Dataframe
        extended_freq_table = pd.concat(extended_freq_table, axis=0)
        frequences_reduced = pd.concat(frequences_reduced, axis=0).loc[:, ["variable", "max_particip"]]
        frequences_reduced['variability'] = np.where(frequences_reduced['max_particip'] >= 100, '00_variation',
                                                     np.where(frequences_reduced['max_particip'] >= 98, '02_variation',
                                                              np.where(frequences_reduced['max_particip'] >= 90,
                                                                       '10_variation',
                                                                       np.where(
                                                                           frequences_reduced['max_particip'] >= 80,
                                                                           '20_variation',
                                                                           np.where(
                                                                               frequences_reduced['max_particip'] >= 50,
                                                                               '50_variation',
                                                                               'high_variability')))))

        if variability == 0:
            frequences_reduced['decision_variability'] = "accept"
        elif variability == 2:
            frequences_reduced['decision_variability'] = np.where(frequences_reduced['variability'] == '00_variation',
                                                                  'reject',
                                                                  'accept')
        elif variability == 10:
            frequences_reduced['decision_variability'] = np.where(
                (frequences_reduced['variability'] == '00_variation') | (
                            frequences_reduced['variability'] == '02_variation'), 'reject',
                'accept')
        elif variability == 20:
            frequences_reduced['decision_variability'] = np.where(
                (frequences_reduced['variability'] == '00_variation') | (
                            frequences_reduced['variability'] == '02_variation') | (
                            frequences_reduced['variability'] == '10_variation'), 'reject',
                'accept')
        elif variability == 50:
            frequences_reduced['decision_variability'] = np.where(
                (frequences_reduced['variability'] == '00_variation') | (
                            frequences_reduced['variability'] == '02_variation') | (
                            frequences_reduced['variability'] == '10_variation') | (
                            frequences_reduced['variability'] == '20_variation'), 'reject',
                'accept')
        elif variability == 100:
            frequences_reduced['decision_variability'] = np.where(
                (frequences_reduced['variability'] == '00_variation') | (
                            frequences_reduced['variability'] == '02_variation') | (
                            frequences_reduced['variability'] == '10_variation') | (
                            frequences_reduced['variability'] == '20_variation') | (
                            frequences_reduced['variability'] == '50_variation'), 'reject',
                'accept')
        else:
            frequences_reduced['decision_variability'] = "INVALID VALUE"
            print("Invalid entry for parameter 'variability'")

        # Join summary and frequences_reduces in one final table
        summary = summary.set_index('variable').join(frequences_reduced.set_index('variable'))

        # Calculation of completeness
        summary['decision_completeness'] = np.where(summary['% missing'] == 0, 'accept_100',
                                                    np.where(summary['% missing'] > completeness, 'reject', 'accept'))
        # Calculation of high cardinality
        summary['decision_high_cardin'] = np.where(summary['unique'] > h_cardinality, 'high_cardinality',
                                                   'accept')

    else:
        summary = "Invalid entry for parameter 'variables'"

    print("Completeness and variability calculation finished. Elapsed time: " + str(
        (datetime.datetime.now() - start2).seconds) + " secs")
    print("Calculation of whole process finished. Total elapsed time: " + str(
        (datetime.datetime.now() - start1).seconds) + " secs")
    print("Input parameters: variables='" + str(variables) + "', variability= " + str(
        variability) + ", completeness=" + str(completeness) + ", h_cardinality=" + str(h_cardinality))
    # 'summary':
    # 'extended_freq_table':
    ret = [
        summary,
        extended_freq_table
    ]
    return ret
def full_describe_numerical(dataframe, variables="all", variability=20, completeness=10):
    """
    Parameters description:
    dataframe: Table to be used in DataFrame format
    variables: Variables to perform the describe method. Available values: "all", type sequence (ie. '1:8'),
    type lis of numbers or variables labels (ie. '1,4,9' o 'var1,var2,var3')
    completeness: Minimum completeness accepted for variables (value betwen 0 y 100)
    variability: Minimum variablility accepted for variables (ie. 0,2,10,20,50,100)
    """
    print("Describe procedure starts, Time: " + datetime.datetime.now().strftime("%H:%M:%S"))
    start1 = datetime.datetime.now()
    inicial_varnames = dataframe.columns
    dataframe = dataframe.select_dtypes(include='number')
    if variables == "all":  # If desired for all the variables
        # Calculate describe adding (concatenar,concatenate) the missing count
        summary = pd.concat(
            [dataframe.describe(include="all", percentiles=[0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1]),
             dataframe.isnull().sum().to_frame(name='missing').T], sort=False)
    elif ":" in variables:  # If desired for sequence of variables n1:n2
        # Calculate describe adding (concatenar,concatenate) the missing count
        variables_serie = list(pd.to_numeric((variables).split(':')))
        variables_serie = list(pd.Series(range(variables_serie[0], variables_serie[1])))
        summary = pd.concat([dataframe.iloc[:, variables_serie].describe(include="all",
                                                                         percentiles=[0, 0.01, 0.05, 0.1, 0.25, 0.5,
                                                                                      0.75, 0.9, 0.95, 0.99, 1]),
                             dataframe.iloc[:, variables_serie].isnull().sum().to_frame(name='missing').T], sort=False)
    else:
        try:  # If desired for numeric array (posiciones,positions) of variables [n1,n2,n3,...]
            # Dividir valor de variables por coma, creando lista numerica
            variables_serie = list(pd.to_numeric((variables).split(',')))
            # Calculate describe adding (concatenar,concatenate) the missing count
            summary = pd.concat([dataframe.iloc[:, variables_serie].describe(include="all",
                                                                             percentiles=[0, 0.01, 0.05, 0.1, 0.25, 0.5,
                                                                                          0.75, 0.9, 0.95, 0.99, 1]),
                                 dataframe.iloc[:, variables_serie].isnull().sum().to_frame(name='missing').T],
                                sort=False)
        except:
            # Create list from string separated for comma
            variables_serie = list(variables.split(","))
            # Check if exists any column name which doesn't exist in the dataframe (in,contains,contiene,valor en)
            if set(list(variables_serie)).issubset(list(inicial_varnames)):
                # Leave only the column names that exist in the dataframe (intersection,interseccion,valores comunes,common values)
                variables_serie = list(set(variables_serie) & set(dataframe.columns))
            if set(list(variables_serie)).issubset(list(
                    dataframe.columns)):  # If desired for string array (colomn names) of variables ['nam1','nam2','nam3',...]
                # Calculate describe adding (concatenar,concatenate) the missing count
                summary = pd.concat([dataframe.loc[:, variables_serie].describe(include="all",
                                                                                percentiles=[0, 0.01, 0.05, 0.1, 0.25,
                                                                                             0.5, 0.75, 0.9, 0.95, 0.99,
                                                                                             1]),
                                     dataframe.loc[:, variables_serie].isnull().sum().to_frame(name='missing').T],
                                    sort=False)
            else:  # Garbage Collector in other case
                summary = "Invalid entry for 'variables' parameter"

    print("Describe procedure finished. Elapsed time: " + str((datetime.datetime.now() - start1).seconds) + " secs")

    start2 = datetime.datetime.now()
    print("Starts completeness and variability calculation, Time: " + datetime.datetime.now().strftime("%H:%M:%S"))
    if isinstance(summary,
                  pd.DataFrame) == True:  # Verifiying if there is any error in the parameters and checking ig summary was created
        # Calculate total count
        summary.loc['total count'] = summary.loc[['count', 'missing']].sum()
        # Calculation of percentage of missing
        summary.loc['% missing'] = np.around(
            (summary.loc['missing'] * 100 / summary.loc['total count']).astype(np.double), 4)
        # Transpose data
        summary = summary.T
        # Calulation of variability
        summary['variability'] = np.where(summary['min'] == summary['max'], '00_variation',
                                          np.where(summary['1%'] == summary['99%'], '02_variation',
                                                   np.where(summary['5%'] == summary['95%'], '10_variation',
                                                            np.where(summary['10%'] == summary['90%'], '20_variation',
                                                                     np.where(summary['25%'] == summary['75%'],
                                                                              '50_variation',
                                                                              'high_variability')))))
        if variability == 0:
            summary['decision_variability'] = "accept"
        elif variability == 2:
            summary['decision_variability'] = np.where(summary['variability'] == '00_variation', 'reject',
                                                       'accept')
        elif variability == 10:
            summary['decision_variability'] = np.where(
                (summary['variability'] == '00_variation') | (summary['variability'] == '02_variation'), 'reject',
                'accept')
        elif variability == 20:
            summary['decision_variability'] = np.where(
                (summary['variability'] == '00_variation') | (summary['variability'] == '02_variation') | (
                            summary['variability'] == '10_variation'), 'reject',
                'accept')
        elif variability == 50:
            summary['decision_variability'] = np.where(
                (summary['variability'] == '00_variation') | (summary['variability'] == '02_variation') | (
                            summary['variability'] == '10_variation') | (summary['variability'] == '20_variation'),
                'reject',
                'accept')
        elif variability == 100:
            summary['decision_variability'] = np.where(
                (summary['variability'] == '00_variation') | (summary['variability'] == '02_variation') | (
                            summary['variability'] == '10_variation') | (summary['variability'] == '20_variation') | (
                            summary['variability'] == '50_variation'), 'reject',
                'accept')
        else:
            summary['decision_variability'] = "INVALID VALUE"
            print("Invalid entry for parameter 'variability'")
        # Calculation of completeness
        summary['decision_completeness'] = np.where(summary['% missing'] == 0, 'accept_100',
                                                    np.where(summary['% missing'] > completeness, 'reject', 'accept'))
    else:
        summary = "Invalid entry for parameter 'variables'"

    print("Completeness and variability calculation finished. Elapsed time: " + str(
        (datetime.datetime.now() - start2).seconds) + " secs")

    print("Calculation of whole process finished. Total elapsed time: " + str(
        (datetime.datetime.now() - start1).seconds) + " secs")
    print("Input parameters: variables='" + str(variables) + "', variability= " + str(
        variability) + ", completeness=" + str(completeness))

    return summary
def frequences_table(col, dataframe):
    """
    Parameters description:
    dataframe: Table of frequences to be used in DataFrame format
    col: Variable name to perform the frequency table method

    This function drop missing values from variable (remove missing, eliminar missing, eliminar NaN, eliminar datos faltantes)
    """
    frequences = dataframe[col].dropna()
    # Calculate percentages of participation
    percentages = np.around(frequences * 100 / frequences.sum(), 2)
    # Calculate cumulative percentages
    cum_percentajes = np.around(100 * frequences.cumsum() / frequences.sum(), 2)
    # Concatenate variables calculed previously
    frequences = pd.concat([frequences, percentages, cum_percentajes], sort=False, axis=1)
    # Include ID variables
    frequences.insert(loc=0, column='variable', value=col)
    frequences.insert(loc=1, column='values', value=list(frequences.index))
    # Change columns names
    frequences.columns = ['variable', 'values', 'count', '%', '% cum']
    # Calculate maximum participation from categories
    frequences['max_particip'] = frequences['%'].max()
    frequences = pd.DataFrame(frequences)

    # Drop duplicates from frecuency table (unique, distinct, duplicados, duplicate, distintos, unicos)
    frequences_reduced = frequences.drop_duplicates(["variable", "max_particip"])

    # Include "missing" value if neccesary
    if "missing" not in frequences["values"]:
        frequences = frequences.append(
            pd.Series([col, "missing", 0, 0, 100, np.asscalar(frequences_reduced.iloc[[0], [5]].values)],
                      index=frequences.columns), ignore_index=True)

    ret = {
        'frequences': frequences,
        'frequences_reduced': frequences_reduced
    }
    return ret
def missing_values_table(df):
    mis_val = df.isnull().sum()
    mis_val_percent = 100 * df.isnull().sum() / len(df)
    mis_val_table = pd.concat([mis_val, mis_val_percent], axis=1)
    mis_val_table_ren_columns = mis_val_table.rename(
        columns={0: 'Missing Values', 1: '% of Total Values'})
    mis_val_table_ren_columns = mis_val_table_ren_columns[
        mis_val_table_ren_columns.iloc[:, 1] != 0].sort_values(
        '% of Total Values', ascending=False).round(1)
    print("Your selected dataframe has " + str(df.shape[1]) + " columns.\n"
                                                              "There are " + str(
        mis_val_table_ren_columns.shape[0]) +
          " columns that have missing values.")
    return mis_val_table_ren_columns


syq = pd.read_csv(INPUT_DATA_PATH + FILE_NAME + INPUT_EXTENSION, sep='"\|\|"', encoding='latin1')


syq.head()
syq.shape
os.listdir(INPUT_DATA_PATH)

#syq.dtypes.to_csv(os.path.join(OUTPUT_DATA_PATH, 'syqDtypes_def.csv'), sep='|', header = True)

#dict_types = pd.read_csv('/mnt/s3-refined-porvenir/test/results/syqDtypes_def.csv', header=None, sep=";",names=None)
#dict_types.drop(axis='index')
#dict_types.apply(lambda x: print(x.name))

df = syq.drop_duplicates()
df.shape



### cast data types

df['Tarea'] = df['"Tarea'].str[1:]

df['Radicado_Entrada'] = df['Radicado_Entrada'].astype(str)
df['Cod_Asunto'] = df['Cod_Asunto'].astype(str)
df['Descripcion_Casual'] = df['Descripcion_Casual'].astype(str)
df['Solucion'] = df['Solucion'].astype(str)

df['Nro_Anexos'] = df['Nro_Anexos'].astype(str)
df['Centro_Costo_Delegado'] = df['Centro_Costo_Delegado'].astype(str)
df['Centro_Costo_Destino'] = df['Centro_Costo_Destino'].astype(str)

#Fechas
#Transformaciones de fechas
#products_pd['Fecha_Apertura_Producto'] = pd.to_datetime(products_pd['Fecha_Apertura_Producto'].apply(str), yearfirst=True, format='%Y%m%d', errors='coerce')
#products_pd['FECHA_BLOQ'] = pd.to_datetime(products_pd['FECHA_BLOQ'].apply(str), yearfirst=True, format='%Y%m%d', errors='coerce')

df['Fecha_Radicacion'] = df['Fecha_Radicacion'].astype(str)
df['Fecha_Asignacion'] = df['Fecha_Asignacion'].astype(str)
df['Fecha_Activacion'] = df['Fecha_Activacion'].astype(str)
df['Fecha_Terminacion'] = df['Fecha_Terminacion'].astype(str)
df['Fecha_Vencimiento_Aprox'] = df['Fecha_Vencimiento_Aprox'].astype(str)

df['Fecha_Vencimiento'] = df['Fecha_Vencimiento'].astype(str)
df['Fecha_Final'] = df['Fecha_Final'].astype(str)
df['Fecha_Asignacion'] = df['Fecha_Asignacion'].astype(str)
df['Fecha_Digitalizacion'] = df['Fecha_Digitalizacion'].astype(str)

df['FechaCreacionArchivo'] = df['FechaCreacionArchivo'].astype(str)
df['Fecha_Ultima_Modificacion'] = df['Fecha_Ultima_Modificacion'].astype(str)
df['Fecha_RtaTemporal'] = df['Fecha_RtaTemporal'].astype(str)
df['Fecha_Envio'] = df['Fecha_Envio'].astype(str)

df['Fecha_recibo_env'] = df['Fecha_recibo_env'].astype(str)
df['Fecha_gen_ruta'] = df['Fecha_gen_ruta'].astype(str)
df['Fecha_recib_ruta'] = df['Fecha_recib_ruta'].astype(str)


columnas = ['NIT_Cust','Tarea', 'Radicado_Entrada', 'Fecha_Radicacion', 'Producto',
       'Categoria', 'Cod_Asunto', 'Descripcion_Asunto', 'Clasificacion_Asunto',
       'Modo_Activacion', 'Prioridad', 'Duracion_Dias', 'Requiere_Respuesta',
       'Tipo_De_Relacion', 'Cod_Condicion', 'Detalle_Condicion', 'Estado',
       'Descripcion_Casual', 'Solucion', 'Descripcion_Solucion', 'Referencia',
       'Nro_Anexos', 'Canal', 'Tipo_Cliente', 'Nombre',
       'ID_Usuario_Delegado', 'Usuario_Delegado', 'Centro_Costo_Delegado',
       'RZDA_Delegado', 'Regional_Delegado', 'Fecha_Asignacion',
       'Hora_Asignacion', 'Mes', 'Fecha_Activacion', 'Fecha_Terminacion',
       'Fecha_Vencimiento_Aprox', 'Fecha_Vencimiento', 'Fecha_Final',
       'ID_Usuario_Asignador', 'Usuario_Asignador', 'Centro_Costo_Asignador',
       'Desc_Centro_Asignador', 'Regional_Asignador', 'ID_Usuario_Responsable',
       'Usuario_Responsable', 'Centro_Costo_Responsable',
       'Desc_Centro_Responsable', 'Regional_Responsable',
       'ID_Usuario_Supervisor', 'Usuario_Supervisor',
       'Centro_Costo_Supervisor', 'Desc_Centro_Supervisor',
       'Regional_Supervisor', 'Fecha_Digitalizacion', 'Radicado_Salida',
       'Desc_Motivo', 'Segmento', 'ArchivoAnexo', 'FechaCreacionArchivo',
       'UsuarioCreoArchivo', 'Estado_Tramite', 'Direccion', 'Barrio', 'Ciudad',
       'Dpto', 'Telefono', 'RZDA_Asignador', 'Parametros', 'Sucursal',
       'Dir_sucursal', 'Ciudad_sucursal', 'Depto_sucursal', 'Suc_porvenir',
       'Otra_Localizacion', 'Fecha_Ultima_Modificacion',
       'ID_Ultima_Modificacion', 'Usuario_Ultima_Modificacion',
       'Centro_Costo_Ultima_Modificacion', 'Desc_Centro_Ultima_Modificacion',
       'Objetivo_Requerimiento', 'RadiSalida_RtaTemporal', 'Fecha_RtaTemporal',
       'Requiere_documento', 'Numero_Envio', 'Fecha_Envio',
       'Usuario_generador', 'Centro_Costo_Remitente', 'Nombre_CC_Remitente',
       'Fecha_recibo_env', 'Usuario_recp_env', 'Numero_Ruta', 'Fecha_gen_ruta',
       'Fecha_recib_ruta', 'Centro_Costo_Destino', 'Nombre_CC_Destino',
       'Recibido_centro_costo', 'Justificacion_Vencimiento"']

columns_crts =['Producto','Categoria','Cod_Asunto','Clasificacion_Asunto',
               'Modo_Activacion','Prioridad','Duracion_Dias','Requiere_Respuesta',
               'Cod_Condicion','Detalle_Condicion','Estado','Canal','Tipo_Cliente',
               'Desc_Motivo','Segmento']

df.shape
df = df[columnas]
df.shape

describe_num = full_describe_numerical(df,variables="all",variability=20,completeness=10)
type(describe_num)


describe_cat = full_describe_categorical(df,variables="all",variability=20,completeness=10,h_cardinality=50)
type(describe_cat[1])


import os


csv_path = os.path.join(INPUT_DATA_PATH, "results")
os.listdir(csv_path)
describe_num.to_csv(os.path.join(csv_path, "descriptive_num_vars_v2.csv"), sep='|', encoding='latin1')


describe_cat[0].to_csv(os.path.join(csv_path,"descriptive_cat1_vars_v2.csv"), sep='|',encoding='latin1')
describe_cat[1].to_csv(os.path.join(csv_path,"descriptive_cat2_vars_v2.csv"), sep='|', encoding='latin1')


?describe_num.to_csv

##################
## na description in overall dataset

missi = missing_values_table(df)
missi.to_csv(os.path.join(csv_path, "missing_report.csv"), sep='|')


## Save as HDF
df.to_hdf(OUTPUT_DATA_PATH + FILE_NAME + "." + OUT_EXTENSION,
                 key='df',
                 mode='w')



### Cruce con el archivo de muestra de clientes
SAMPLE_FILE = '/mnt/s3-refined-porvenir/Clientes/h5/temp_files/persona_natural_IDENT_client_PO.h5'
sample = pd.read_hdf(SAMPLE_FILE , key='df', mode = 'r')
sample.shape
sample.head(2)




clientes_syq = pd.merge(left=sample,
                        right=df,
                        how='inner',
                        left_on=['IDENTIFICADOR'],
                        right_on=['NIT_Cust'])

clientes_syq.shape

OUTPUT_MERGED_FILE = 'clientes_syq_PO'
clientes_syq.to_hdf(OUTPUT_DATA_PATH + OUTPUT_MERGED_FILE + "." + OUT_EXTENSION,
                 key='df',
                 mode='w')


