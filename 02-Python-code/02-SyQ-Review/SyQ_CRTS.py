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

OUTPUT_MERGED_FILE = 'clientes_syq_PO'


import pandas as pd

clisyq = pd.read_hdf(OUTPUT_DATA_PATH + OUTPUT_MERGED_FILE + "." + OUT_EXTENSION , key='df', mode = 'r')
clisyq['Tarea'] = clisyq['"Tarea'].str[1:]
clisyq.drop(columns=['"Tarea'], inplace=True)

#creacion de columna de YearMonth
clisyq['YearMonth'] = pd.to_datetime(clisyq['Fecha_Radicacion'],format="%d/%m/%Y", errors="coerce")
clisyq['YearMonth'] = clisyq['YearMonth'].astype(str).str[0:4] + clisyq['YearMonth'].astype(str).str[5:7]



clisyq.shape

print('year_month')
year_month =\
    pd.DataFrame(pd.date_range(start = '2017-06-01', end='2019-05-01', freq='MS').map(lambda x: np.str(x.year) + "" + np.str(x.month).rjust(2,'0')))
year_month.columns = ['YearMonth']



def NUMERO_DE_SyQ(x):
    values = x.dropna()
    if len(values) >0 :
        return(values.nunique())
    else:
        return 0

def NUMERO_DE_SyQ_x_OBLIGATORIAS(x):
    values = x.dropna()
    if len(values) >0:
        return (values =='PENSIONES OBLIGATORIAS').sum()
    else:
        return 0

def NUMERO_DE_SyQ_x_VOLUNTARIAS(x):
    values = x.dropna()
    if len(values) >0:
        return (values =='PENSIONES VOLUNTARIAS').sum()
    else:
        return 0


def NUMERO_DE_SyQ_x_CESANTIAS(x):
    values = x.dropna()
    if len(values) > 0:
        return (values == 'CESANTÍAS').sum()
    else:
        return 0

def NUMERO_DE_SyQ_x_SOCIEDAD(x):
    values = x.dropna()
    if len(values) > 0:
        return (values == 'SOCIEDAD').sum()
    else:
        return 0



def NUMERO_DE_ASUNTOS_QUEJA(x):
    values = x.dropna()
    if len(values) > 0:
        return (values == 'Queja').sum()
    else:
        return 0

def NUMERO_DE_ASUNTOS_REQUERIMIENTO_INTERNO(x):
    values = x.dropna()
    if len(values) > 0:
        return (values == 'Requerimiento Interno').sum()
    else:
        return 0

def NUMERO_DE_ASUNTOS_REQUERIMIENTO_LEGAL(x):
    values = x.dropna()
    if len(values) > 0:
        return (values == 'Requerimiento Legal').sum()
    else:
        return 0



def NUMERO_DE_SyQ_REQUIEREN_RESPUESTA(x):
    values = x.dropna()
    if len(values) > 0:
        return (values == 'Y').sum()
    else:
        return 0


def NUMERO_DE_SyQ_PRIORIDAD_ALTA(x):
    values = x.dropna()
    if len(values) > 0:
        return (values == 'ALT').sum()
    else:
        return 0

def NUMERO_DE_SyQ_PRIORIDAD_NORMAL(x):
    values = x.dropna()
    if len(values) > 0:
        return (values == 'NOR').sum()
    else:
        return 0

def NUMERO_DE_SyQ_PRIORIDAD_BAJA(x):
    values = x.dropna()
    if len(values) > 0:
        return (values == 'BAJ').sum()
    else:
        return 0


aggregations = {
    'YearMonth':['first'],
    'Tarea':[NUMERO_DE_SyQ],
    "Producto" : [NUMERO_DE_SyQ_x_OBLIGATORIAS, NUMERO_DE_SyQ_x_VOLUNTARIAS,
                  NUMERO_DE_SyQ_x_CESANTIAS, NUMERO_DE_SyQ_x_SOCIEDAD],
    "Clasificacion_Asunto" : [NUMERO_DE_ASUNTOS_QUEJA, NUMERO_DE_ASUNTOS_REQUERIMIENTO_INTERNO,
                              NUMERO_DE_ASUNTOS_REQUERIMIENTO_LEGAL],
    "Requiere_Respuesta" : [NUMERO_DE_SyQ_REQUIEREN_RESPUESTA],
    "Prioridad" : [NUMERO_DE_SyQ_PRIORIDAD_ALTA,
                   NUMERO_DE_SyQ_PRIORIDAD_NORMAL,NUMERO_DE_SyQ_PRIORIDAD_BAJA]
    }
syq_ym_grouped = \
    clisyq.groupby(['IDENTIFICADOR']).agg(aggregations)

syq_ym_grouped.columns = \
        syq_ym_grouped.columns.droplevel(level=0)

syq_ym_grouped.reset_index(inplace=True)

syq_ym_grouped.head(2)
syq_ym_grouped.columns
syq_ym_grouped.shape

syq_ym_grouped.to_hdf(OUTPUT_DATA_PATH + 'clientes_SyQ_CRTS.h5', key='df')


#client_syq_ids = clisyq[['IDENTIFICADOR', 'YearMonth']].drop_duplicates().count()