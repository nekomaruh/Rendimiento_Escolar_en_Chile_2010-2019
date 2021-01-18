import queries as q
import static_tables as st
import os, psutil
import pandas as pd
import fnmatch
import time

def get_ram(info='unknown'):
    # Obtiene la información de la ram utilizada durante la ejecución del programa
    process = psutil.Process(os.getpid())
    print('RAM:', process.memory_info()[0]/8000000, 'mb ('+info+')')

def drop_dimensions():
    # Elimina todas las tablas que existen en la base de datos
    q.drop_tables()
    get_ram(info='Dropped all tables')

def create_dimensions():
    # Crea las tablas estáticas
    q.create_static_tables()
    q.create_tables()
    get_ram(info='Created all tables')

def insert_static_dimensions():
    # Inserta los valores a las tablas estáticas
    q.insert_dim_depe(st.data_depe)
    q.insert_dim_region(st.data_region)
    q.insert_dim_provincia(st.data_provincia)
    q.insert_dim_rural(st.data_rural_rbd)
    q.insert_dim_ense(st.data_ense)
    q.insert_dim_grado(st.data_grado)
    q.insert_dim_genero(st.data_genero)
    q.insert_dim_sit_fin(st.data_sit_fin)
    q.insert_dim_jornada(st.data_jornada)
    q.insert_dim_int_alu(st.data_int_alu)
    q.insert_dim_sec(st.data_sec)
    q.insert_dim_espe(st.data_espe)
    q.insert_dim_ense2(st.data_ense2)
    get_ram(info='Static tables inserted')

def df_to_html(dataframe, year, num_rows):
    # Nota: No se asegura que la cantidad de columnas a exportar sea mayor al dataframe
    # Esta función solo la utilizamos para testing
    header = dataframe.head(num_rows)
    header.to_html("datasets_headers_pdf/df_"+year+".html")
    get_ram(info='HTML exported '+str(year))

def df_to_sql(table_name, engine, data, headers, remove_duplicates):
    # Sube los datos del dataframe a la base de datos
    get_ram(info='Creating dataframe "'+table_name+'"')
    df = pd.concat(data, axis=1, keys=headers)
    #new_df = new_df.drop_duplicates(subset=remove_duplicates)
    print(df.head())
    print(df.dtypes)
    get_ram(info='Dropping duplicates from "'+table_name+'"')
    df.drop_duplicates(subset=remove_duplicates, keep="first", inplace=True)
    #new_df = new_df.reset_index(drop=True)
    df.reset_index(drop=True, inplace=True)
    get_ram(info='Uploading to database table "'+table_name+'"...')
    df.to_sql(table_name,engine, method='multi', if_exists='append',index=False, chunksize=100000)
    get_ram(info='Data "'+table_name+'" inserted to database')

def get_amount_of_csv():
    # Obtiene la cantidad de archivos .csv
    return len(fnmatch.filter(os.listdir('datasets/'), '*.csv'))

def get_time(start_time):
    end_time = time.time()
    difference = round(end_time-start_time,4)
    return print('Time: '+str(difference)+' seconds')

def insert_dim_comuna(list):
    get_ram(info='Uploading to database table "comuna"...')
    q.insert_dim_com(list)
    get_ram(info='Data "comuna" inserted to database')

def get_columns_to_drop():
    return ['FEC_ING_ALU',
    'NOM_REG_RBD_A', 
    'COD_DEPROV_RBD', 
    'NOM_DEPROV_RBD', 
    'GD_ALU',
    'COD_DEPE2',
    'ESTADO_ESTAB', 
    'COD_TIP_CUR', 
    'COD_DES_CUR', 
    'COD_REG_ALU', 
    'COD_RAMA', 
    'COD_MEN', 
    'SIT_FIN_R',
    'SIT_FINAL_R',
    'EDAD_ALU']