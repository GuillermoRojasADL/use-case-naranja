import pandas as pd

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
print(client_input_data[0])
all_clients =\
     pd.read_hdf(client_input_data_folder + client_input_data[0] + '.h5', 'df')
all_clients['key'] = 0
year_month = pd.DataFrame(pd.date_range(start = '2017-07-01', end='2019-06-01', freq='MS'))
year_month['key'] = 0
year_month.columns = ['year_month','key']
year_month['key'] = year_month['key'].astype(int)
all_clients = all_clients.merge(year_month, how='outer', on = 'key')
all_clients.drop(columns = ['key'], inplace = True)
########################
print(client_input_data[1])
sbgr1_ps_bc =\
     pd.read_hdf(client_input_data_folder + client_input_data[1] + '.h5', 'df')
sbgr1_ps_bc.groupby('BO_ID')
all_clients = pd.merge(all_clients, sbgr1_ps_bc, how='left',
                     left_on=['BO_ID'],
                     right_on=['BO_ID'])
print(sbgr1_ps_bc.columns)
print(all_clients.shape)
print(client_input_data[2])
sbgr1_ps_por_habdata_tbl =\
     pd.read_hdf(client_input_data_folder + client_input_data[2] + '.h5', 'df')
all_clients = pd.merge(all_clients, sbgr1_ps_por_habdata_tbl, how='left',
                     left_on=['BO_ID'],
                     right_on=['BO_ID'])
print(sbgr1_ps_por_habdata_tbl.columns)
print(all_clients.shape)
print(client_input_data[3])
sbgr1_ps_por_salotr_tbl =\
     pd.read_hdf(client_input_data_folder + client_input_data[3] + '.h5', 'df')
all_clients = pd.merge(all_clients, sbgr1_ps_por_salotr_tbl, how='left',
                     left_on=['BO_ID'],
                     right_on=['BO_ID'])
print(sbgr1_ps_por_salotr_tbl.columns)
print(all_clients.shape)
print(client_input_data[4])
sbgr1_ps_por_inf_com_tbl =\
     pd.read_hdf(client_input_data_folder + client_input_data[4] + '.h5', 'df')
all_clients = pd.merge(all_clients, sbgr1_ps_por_inf_com_tbl, how='left',
                     left_on=['BO_ID'],
                     right_on=['BO_ID'])
print(sbgr1_ps_por_inf_com_tbl.columns)
print(all_clients.shape)
print(client_input_data[5])
sbgr1_ps_rd_person =\
     pd.read_hdf(client_input_data_folder + client_input_data[5] + '.h5', 'df')
all_clients = pd.merge(all_clients, sbgr1_ps_rd_person, how='left',
                     left_on=['BO_ID'],
                     right_on=['BO_ID'])
print(sbgr1_ps_rd_person.columns)
print(all_clients.shape)
print(client_input_data[6])
sbgr1_ps_bo_role =\
     pd.read_hdf(client_input_data_folder + client_input_data[6] + '.h5', 'df')
all_clients = pd.merge(all_clients, sbgr1_ps_bo_role, how='left',
                     left_on=['BO_ID'],
                     right_on=['BO_ID'])
print(sbgr1_ps_bo_role.columns)
print(all_clients.shape)
print(client_input_data[7])
sbgr1_ps_bo_cm =\
     pd.read_hdf(client_input_data_folder + client_input_data[7] + '.h5', 'df')
all_clients = pd.merge(all_clients, sbgr1_ps_bo_cm, how='left',
                     left_on=['BO_ID'],
                     right_on=['BO_ID'])
print(all_clients.shape)
######################
