import pandas as pd
import sys

pd.options.mode.chained_assignment = None  # Disable annoying pandas warning
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

archivos_clientes = \
    ['sbgr1_ps_bc',
     'sbgr1_ps_por_habdata_tbl',
     'sbgr1_ps_por_salotr_tbl',
     'sbgr1_ps_aa_rd_person',
     'sbgr1_ps_por_inf_com_tbl',
     'sbgr1_ps_rd_person',
     'sbgr1_ps_bo_role',
     'sbgr1_ps_bo_cm',
     'sbgr1_ps_cm']

INPUT_EXTENSION = 'txt'
OUT_EXTENSION = 'h5'

INPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Clientes/'
OUTPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Clientes/h5/'

for filename in archivos_clientes:
    print('filename: ' + filename)
    client_file = pd.read_csv(
        INPUT_DATA_PATH + filename + '.' + INPUT_EXTENSION,
        sep='\|\|',
        encoding='utf-8')
    print(client_file.shape)
    client_file.to_hdf(OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION,
                 key='df',
                 mode='w')
