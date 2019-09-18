import os
import pandas as pd

#########################################################
input_path = '/mnt/work/datasets/SG5_partitions//'
input_extension = '.snappy.parquet'
output_path = '/mnt/work/datasets/preMDT_cesantias_pi_def//'

file = 'sbgr5_cta_cuenta'
cta_cuenta = pd.read_parquet(os.path.join(input_path, file))

# Fecha de activación , ID para clientes activos.
cta_cuenta['ESTADO_CUENTA_ID'].value_counts()
sel_cols = ['CUENTA_ID_HASH','IDENTIFICADOR_HASH','FECHA_ACTIVACION','FECHA_INACTIVACION']
cond = (cta_cuenta['ESTADO_CUENTA_ID']=='ACTIVA')
df = cta_cuenta[cond][sel_cols]

df.to_parquet(os.path.join(output_path, 'cesantias_preMDT' + '.snappy.parquet'), compression='snappy')


#### PI pre MDT


# cuenta: sbgr6_fo_cuentas.txt
file = 'sbgr6_fo_cuentas.txt'
pi_cta = pd.read_parquet(os.path.join('/mnt/work/datasets/SG6_parquet', file + '.snappy.parquet'))

file = 'sbgr6_af_datos_basicos.txt'
pi_id = pd.read_csv(os.path.join('/mnt/s3-refined-porvenir/SG6/main', file),  sep='\|\|', encoding='latin1',engine='python')

file = 'sbgr6_enca_solicitud.txt'
connect = pd.read_parquet(os.path.join('/mnt/work/datasets/SG6_parquet', file + '.snappy.parquet'))

file = 'sbgr6_fo_aportes.txt'
fo_aportes = pd.read_parquet(os.path.join('/mnt/work/datasets/SG6_parquet', file+ '.snappy.parquet'))

#fo_aportes.shape # (42932448, 27)
#connect.shape # (271050, 6)
#pi_cta.shape # (304907, 12)
#pi_id.shape # (266264, 9)

connect['NUM_FORMULARIO'] = connect['NUM_FORMULARIO'].astype(str)

df_join_ini = pd.merge(pi_id,
                     connect,
                     how='inner',
                     left_on=['NUM_FORMULARIO'],
                     right_on = ['NUM_FORMULARIO'])

#Filtro de activos en PI
# sel_cols = ['COD_CUENTA_ANONI','IDENTIFICADOR_HASH','FECHA_ACTIVACION']
cond = (pi_cta['EST_CUENTA']=='A')
pi_cta_active = pi_cta[cond]

df_final = pd.merge(pi_cta_active ,
                     df_join_ini,
                     how='inner',
                     left_on=['COD_CUENTA_ANONI'],
                     right_on = ['COD_CUENTA'])


#creación yearmonth
df_final['FEC_ULT_APORTE'] = pd.to_datetime(df_final['FEC_ULT_APORTE'].apply(str), format='%d-%b-%y', errors='coerce')
df_final['YearMonth'] = 100*df_final['FEC_ULT_APORTE'].dt.year + df_final['FEC_ULT_APORTE'].dt.month

#CREACIÓN DE VARIABLE DE SALDO TOTAL
fo_aportes['FECHA_CONSIGNACION'] = pd.to_datetime(fo_aportes['FECHA_CONSIGNACION'].apply(str), format='%d-%b-%y', errors='coerce')
fo_aportes['YearMonth'] = 100*fo_aportes['FECHA_CONSIGNACION'].dt.year + fo_aportes['FECHA_CONSIGNACION'].dt.month

fo_aportes['SALDO_TOTAL'] = fo_aportes['SALDO_CAPITAL_AFL'] +fo_aportes['SALDO_CAPITAL_PAT'] + \
                fo_aportes['SALDO_RET_CONTINGENTE_AFL'] + fo_aportes['SALDO_RET_CONTINGENTE_PAT'] + \
                 fo_aportes['SALDO_RENDIMIENTOS_AFL'] + fo_aportes['SALDO_RENDIMIENTOS_PAT']

#ultimos 7 años de aportes y cuentas activas
fo_aportes_filter = fo_aportes[fo_aportes['YearMonth'] > 201204]

aportes_grouped = fo_aportes_filter[['COD_CUENTA','YearMonth','SALDO_TOTAL']].\
                groupby(['COD_CUENTA','YearMonth'], as_index=False, sort = False).max()



df_final_saldo = pd.merge(df_final ,
                     aportes_grouped,
                     how='inner',
                     left_on=['COD_CUENTA_ANONI','YearMonth'],
                     right_on = ['COD_CUENTA','YearMonth'])

sel_cols = ['COD_CUENTA_ANONI','IDENTIFICADOR','FEC_ULT_APORTE','FEC_APERTURA','SALDO_TOTAL']
df_to_write = df_final_saldo[sel_cols]


name_file = 'pv_preMDT'
df_to_write.to_parquet(os.path.join(output_path, name_file + '.snappy.parquet'), compression='snappy')