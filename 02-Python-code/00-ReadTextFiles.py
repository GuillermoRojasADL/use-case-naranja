import pandas as pd

archivos_clientes = \
    ['sbgr1_ps_bc',
    'sbgr1_ps_bo_role',
    'sbgr1_ps_por_habdata_tbl',
    'sbgr1_ps_por_salotr_tbl',
    'sbgr1_ps_aa_rd_person',
    'sbgr1_ps_bo_cm',
    'sbgr1_ps_cm',
    'sbgr1_ps_por_inf_com_tbl',
    'sbgr1_ps_rd_person']

INPUT_EXTENSION = 'txt'
OUT_EXTENSION = 'h5'

INPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Clientes/'
OUTPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Clientes/h5/'

for filename in archivos_clientes:
    print('filename: ' + filename)
    client_file = pd.read_csv(
        INPUT_DATA_PATH + filename + '.' + INPUT_EXTENSION,
        sep='\|\|',
        encoding='latin-1')
    print(client_file.shape)
    client_file.to_hdf(OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION,
                 key='df',
                 mode='w')

