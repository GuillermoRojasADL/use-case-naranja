import sys
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None  # Disable annoying pandas warning
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")
###################
# transform text files to h5 files
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

for filename in archivos_clientes[0:8]:
    print('reading file: ' + INPUT_DATA_PATH + filename + '.' + INPUT_EXTENSION)
    client_file = pd.read_csv(
        INPUT_DATA_PATH + filename + '.' + INPUT_EXTENSION,
        sep='\|\|',
        encoding='utf-8')
    print(client_file.shape)
    print('writing file: ' + OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION)
    client_file['CITY'] = client_file['CITY'].astype('str')
    client_file.to_hdf(OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION,
                 key='df',
                 mode='w')
    del client_file

filename = archivos_clientes[8]
print('reading file: ' + INPUT_DATA_PATH + filename + '.' + INPUT_EXTENSION)
client_file = pd.read_csv(
    INPUT_DATA_PATH + filename + '.' + INPUT_EXTENSION,
    sep='\|\|',
    encoding='utf-8')
print(client_file.shape)
print('writing file: ' + OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION)
client_file['CITY'] =\
    pd.to_numeric(client_file['CITY'], errors='coerce')
client_file.to_hdf(OUTPUT_DATA_PATH + filename + "." + OUT_EXTENSION,
                   key='df',
                   mode='w')
# ###################
# # read h5 files and join tables
#
# # folders names definitions
# client_input_data_folder = '/mnt/s3-refined-porvenir/Clientes/h5/'
# # files names definitions
# client_input_data = \
#     ['sbgr1_ps_aa_rd_person',
#      'sbgr1_ps_bc',
#      'sbgr1_ps_por_habdata_tbl',
#      'sbgr1_ps_por_salotr_tbl',
#      'sbgr1_ps_por_inf_com_tbl',
#      'sbgr1_ps_rd_person',
#      'sbgr1_ps_bo_role',
#      'sbgr1_ps_bo_cm',
#      'sbgr1_ps_cm']
# ########################
# client_df =\
#     pd.read_hdf(client_input_data_folder+client_input_data[0]+'.h5', 'df')
# print(client_df.shape)
# for filename in client_input_data[1:]:
#     print('reading file: ' + client_input_data_folder+filename+'.h5')
#     data =\
#         pd.read_hdf(client_input_data_folder+filename+'.h5', 'df')
#     print(data.shape)
#     client_df = pd.merge(client_df, data, how='left',
#                        left_on=['BO_ID'],
#                        right_on=['BO_ID'])
#     del data
#     print(client_df.shape)
# client_df.to_hdf(client_input_data_folder + "client_dataset.h5",
#                  key='df',
#                  mode='w')
# ####
