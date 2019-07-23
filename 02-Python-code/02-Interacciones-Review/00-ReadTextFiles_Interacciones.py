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
archivos_interacciones = [
        "sbgr_03_05_interacciones",
        "sbgr_03_106_interacciones",
        "sbgr_03_110_interacciones",
        "sbgr_03_14_interacciones",
        "sbgr_03_01_interacciones",
        "sbgr_03_06_interacciones",
        "sbgr_03_107_interacciones",
        "sbgr_03_111_interacciones",
        "sbgr_03_15_interacciones",
        "sbgr_03_02_interacciones",
        "sbgr_03_07_interacciones",
        "sbgr_03_108_interacciones",
        "sbgr_03_112_interacciones",
        "sbgr_03_16_interacciones",
        "sbgr_03_03_interacciones",
        "sbgr_03_08_interacciones",
        "sbgr_03_109_interacciones",
        "sbgr_03_11_interacciones",
        "sbgr_03_17_interacciones",
        "sbgr_03_04_interacciones",
        "sbgr_03_09_interacciones",
        "sbgr_03_10_interacciones",
        "sbgr_03_13_interacciones"
    ]

INPUT_EXTENSION = ''
OUT_EXTENSION = 'h5'

INPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Interacciones/'
OUTPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Interacciones/h5/'

import pandas as pd
import numpy as np
import unidecode
import re
import datetime

li = []
for file in archivos_interacciones[-8:-7]:
    df1 = pd.read_csv(INPUT_DATA_PATH + file + INPUT_EXTENSION, sep='"\|\|"', encoding='latin1')
    print(" ---{indice}: Archivo: {fname} ----- \n Número de filas: {a}, Número de columnas: {b}".format(indice=len(li) ,fname= file,a=df1.shape[0], b=df1.shape[1]))
    df = df1.drop_duplicates()
    df['DATE'] = df['Fecha'].astype(str).str[0:7]
    shape1, shape2 = (df1.shape, df.shape)
    print("""Número de registros en archivo: {a} \n 
            Número de registros sin duplicados: {b} \n
            registros eliminados {c} \n
            ------------------------- \n""".format(a=shape1[0], b=shape2[0], c=int(shape1[0]-shape2[0])))
    li.append(df)

data = pd.concat(li, axis=0, ignore_index=True)

data.columns
data1==data
#['"Interaccion', 'Subinteraccion', 'Producto', 'Descripcion_Producto',
#       'Categoria', 'Descripcion_Categoria', 'Asunto', 'Descripcion_Asunto',
#       'Motivo', 'Tipo_Subinter', 'Usuario', 'Centro_Costo', 'Fecha', 'Hora',
#       'Canales', 'Segmento', 'Numero_ID', 'Rol_Emp_PJ', 'Rol_Emp_PN',
#       'Observaciones', 'Area', 'Regional', 'Canal', 'PN', 'PJ', 'CC_Usuario"',
#       'DATE']

data.shape

data['DATE'].value_count

df = data
df.dtypes

df['Subinteraccion'] = df['Subinteraccion'].astype(str)
df['Categoria'] = df['Categoria'].astype(str)
df['Asunto'] = df['Asunto'].astype(str)
df['Rol_Emp_PN'] = df['Rol_Emp_PN'].astype(str)

##Write csv
df.to_csv('/home/jupyter/notebooks/s3-refined/Interacciones/results/review/interactions.csv','|')


#for filename in archivos_interacciones:
#    print('reading file: ' + INPUT_DATA_PATH + filename + INPUT_EXTENSION)
#    interactions_file = pd.read_csv(
#        INPUT_DATA_PATH + filename + INPUT_EXTENSION,
#        sep='\|\|',
#        encoding='latin1')
#    print(interactions_file.shape)
#    interactions_file.to_hdf(OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION,key='df',mode='w')
#    print('Writed file: ' + OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION)
#    del interactions_file

# For testing
#filename_path_1 = INPUT_DATA_PATH + archivos_interacciones[0] + INPUT_EXTENSION
#filename_path_2 = INPUT_DATA_PATH + archivos_interacciones[0] + INPUT_EXTENSION
#print(filename_path)
#print('reading file: ' + INPUT_DATA_PATH + filename_path_1+ '.' + INPUT_EXTENSION)
#interactions_file_1 = pd.read_csv(filename_path_1, sep='\|\|', encoding='latin1')
#interactions_file_2 = pd.read_csv(filename_path_2, sep='\|\|', encoding='latin1')

#print(interactions_file_1.shape)
#print(interactions_file_2.shape)

#print(interactions_file_1.dtypes)
#print(interactions_file_2.dtypes)


#df = interactions_file_2
#{df.columns.get_loc(c):c for idx, c in enumerate(df.columns)}
#df.iloc[:,[12,13,14,15,16]]

#interactions_file_2['Fecha']
#interactions_file_1.dtypes == interactions_file_2.dtypes

#interactions_file_1.head(10)
#print('writing file: ' + OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION)
#interactions_file.to_hdf(OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION,
#                   key='df',
#                   mode='w')


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


data = pd.read_csv('/home/jupyter/notebooks/s3-refined/Interacciones/results/review/interactions.csv','|')
data.dtypes
vars_cat = ['Subinteraccion', 'Producto',
       'Categoria', 'Descripcion_Categoria', 'Asunto', 'Descripcion_Asunto',
       'Motivo', 'Tipo_Subinter', 'Usuario','Centro_Costo', 'Fecha', 'Hora',
       'Canales', 'Segmento', 'Numero_ID', 'Rol_Emp_PJ', 'Rol_Emp_PN',
       'Observaciones', 'Area', 'Regional', 'Canal', 'PN', 'PJ']


data['DATE'].astype(str).value_counts(ascending =True, dropna=False)
data.dtypes
##%%%% Describe
describe_num = full_describe_numerical(data,variables="all",variability=20,completeness=10)
type(describe_num)


###


data.dtypes
data['Subinteraccion'] = data['Subinteraccion'].astype(str)
data['Categoria'] = data['Categoria'].astype(str)
data['Asunto'] = data['Asunto'].astype(str)
data['Rol_Emp_PN'] = data['Rol_Emp_PN'].astype(str)
data['Centro_Costo'] = data['Centro_Costo'].astype(str)
describe_cat = full_describe_categorical(data,variables="all",variability=20,completeness=10,h_cardinality=50)
type(describe_cat[1])


import os



csv_path = os.path.join(INPUT_DATA_PATH, "results")
os.listdir(csv_path)
describe_num.to_csv(os.path.join(csv_path, "descriptive_num_vars_v1.csv"), sep='\t')


describe_cat[0].to_csv(os.path.join(csv_path,"descriptive_cat1_vars_v1.csv"), sep='\t')
describe_cat[1].to_csv(os.path.join(csv_path,"descriptive_cat2_vars_v1.csv"), sep='\t')z


describe_cat[1].columns
#
describe_cat[1].shape

describe_cat[1].iloc[1082128:,:].head(2)

##################
## na description in overall dataset

missi = missing_values_table(df)
missi.to_csv(os.path.join(csv_path, "missing_report.csv"), sep='|')
