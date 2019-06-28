import pandas as pd
pd.options.mode.chained_assignment = None  # Disable annoying pandas warning

# archivos_clientes = \
#     [
#     # 'sbgr1_ps_bc',
#     'sbgr1_ps_bo_role',
#     # 'sbgr1_ps_por_habdata_tbl',
#     # 'sbgr1_ps_por_salotr_tbl',
#     # 'sbgr1_ps_aa_rd_person',
#     'sbgr1_ps_bo_cm',
#     'sbgr1_ps_cm',
#     #'sbgr1_ps_por_inf_com_tbl',
#     #'sbgr1_ps_rd_person']

archivos_clientes = \
    ['sbgr1_ps_bo_role','sbgr1_ps_bo_cm','sbgr1_ps_cm']

INPUT_EXTENSION = 'txt'
OUT_EXTENSION = 'h5'

INPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Clientes/'
OUTPUT_DATA_PATH = '/mnt/s3-refined-porvenir/Clientes/h5/'

filename = archivos_clientes[0]
print('filename: ' + filename)
client_file = pd.read_csv(
    INPUT_DATA_PATH + filename + '.' + INPUT_EXTENSION,
    sep='\|\|')
    # ,
    # encoding='latin-1')

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


# data_cupo_vigente_raw = pd.read_hdf('/home/pandrade/use-case-azul/03-Data/data_cupo_vigente_raw.h5', 'df')
