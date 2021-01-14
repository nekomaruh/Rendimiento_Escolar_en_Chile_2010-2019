# Libraries
import pandas as pd
import os, psutil
import time
import sqlalchemy
import numpy as np


# Project functions
import static_tables as st
import queries as q

from sqlalchemy import event

# Exportamos hacia la base de datos en postgres
engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5432/rendimiento_escolar')

@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True
        cursor.commit()



start_time = time.time()

def export_table_to_sql(table_name, data, headers, remove_duplicates):
    new_df = pd.concat(data, axis=1, keys=headers)
    new_df = new_df.drop_duplicates(subset=remove_duplicates)
    new_df = new_df.reset_index(drop=True)
    new_df.to_sql(table_name,engine, method='multi', if_exists='append',index=False, chunksize=1000)

def read_file(year, drops):
    print('Reading dataset', year)
    # Leemos los datos y separamos por ; porque algunos nombres de establecimientos poseen comas y dan error
    df = pd.read_csv("datasets/Rendimiento por estudiante "+str(year)+".csv", sep=';', low_memory=False, encoding='latin-1')
    print('Cantidad de datos:',len(df))

    # Eliminamos las columnas
    if drops!=None:
        for col in drops:
            if col in df:
                df = df.drop(columns=col)
        
    # Están en todos los años
    df.fillna({'SIT_FIN' : '-'}, inplace = True) 
    df['COD_SEC'] = df['COD_SEC'].replace([' '],0) 
    df['COD_ESPE'] = df['COD_ESPE'].replace([' '],0) 

    if year <= 2013: # Esta solo en los años 2010-2013
        df['INT_ALU'] = df['INT_ALU'].replace(['.'],2) 
    
    """
    if year == 2010:
        # Crear comunas de establecimiento y alumno, estan en todos los años
        headers_com = ["COD_COM", "NOM_COM"]

        data_com_rbd = [df["COD_COM_RBD"], df["NOM_COM_RBD"]]
        df_com_rbd = pd.concat(data_com_rbd, axis=1, keys=headers_com)

        data_com_alu = [df["COD_COM_ALU"], df["NOM_COM_ALU"]]
        df_com_alu = pd.concat(data_com_alu, axis=1, keys=headers_com)

        df_com = pd.concat([df_com_rbd,df_com_alu])

        df_com = df_com.drop_duplicates(subset=['COD_COM'])
        df_com = df_com.reset_index(drop=True)

        q.insert_dim_com(df_com.values.tolist())

        del headers_com, data_com_rbd, df_com_rbd, data_com_alu, df_com_alu, df_com
    """
    """
    data_alu = [df["MRUN"], df["FEC_NAC_ALU"],df["GEN_ALU"], df["COD_COM_ALU"], df["INT_ALU"]]
    headers_alu = ["mrun", "fec_nac_alu", "gen_alu", "cod_com", "int_alu"]
    df_alu = pd.concat(data_alu, axis=1, keys=headers_alu)
    df_alu = df_alu.drop_duplicates(subset=['mrun'])
    df_alu = df_alu.reset_index(drop=True)

    df_alu.to_sql('alumno',engine, method='multi', if_exists='append',index=False)

    print(df_alu)
    """

    df["COD_PRO_RBD"] = np.nan
    df["COD_ENSE2"] = np.nan
    df["COD_JOR"] = np.nan
    df["PROM_GRAL"] = df["PROM_GRAL"].str.replace(',', ".").astype(float)

    # Agregar establecimientos
    """
    data_establecimiento = [df["RBD"], df["DGV_RBD"], df["NOM_RBD"], df["RURAL_RBD"], df["COD_DEPE"], df["COD_ESPE"], df["COD_REG_RBD"], df["COD_PRO_RBD"], df["COD_SEC"], df["COD_COM_RBD"]]
    headers_establecimiento = ['rbd', 'dgv_rbd', 'nom_rbd', 'rural_rbd', 'cod_depe', 'cod_espe', 'cod_reg_rbd', 'cod_pro_rbd', 'cod_sec', 'cod_com']
    export_table_to_sql(table_name='establecimiento', data=data_establecimiento, headers=headers_establecimiento, remove_duplicates=['rbd','dgv_rbd'])
    """
    print(df.head())
    data_notas = [df["AGNO"], df["MRUN"], df["RBD"], df["DGV_RBD"], df["PROM_GRAL"], df["SIT_FIN"], df["COD_ENSE"], df["COD_ENSE2"], df["COD_JOR"], df["COD_GRADO"]]
    print(data_notas)
    head_notas = ['agno', 'mrun', 'rbd', 'dgv_rbd', 'prom_gral', 'sit_fin', 'cod_ense', 'cod_ense2', 'cod_jor', 'cod_grado']
    export_table_to_sql(table_name='notas', data=data_notas, headers=head_notas, remove_duplicates=['agno','mrun'])

    #df_2010_head = df_2010.head()
    #html_2010 = df_2010_head.to_html("datasets_headers_pdf/df_"+year+".html")
    ram(info='Dataframe ' + str(year))
    return df

def insert_dimensions():
    q.create_static_tables() # Crea las tablas estáticas
    q.insert_dim_depe(st.data_depe) # Inserta los valores a las tablas estáticas
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
    print('Dimensions inserted successfully!')

def insert_tables():
    q.create_tables()

def ram(info='unknown'):
    process = psutil.Process(os.getpid())
    print('RAM:', process.memory_info()[0]/8000000, 'mb ('+info+')')

if __name__ == "__main__":
    #q.insert_alumnox()
    #q.insert_establecimiento_test()
    

    """
    # Inserta todas las dimensiones estáticas
    ram(info='Loading program')
    q.drop_tables() # Elimina las tablas si existen
    insert_dimensions()
    insert_tables()

    del st
    ram(info='Delete imports')

    """

    
    # Leemos los archivos
    df_2010 = read_file(year=2010, drops=['SIT_FIN_R'])

    
    """
    df_2011 = read_file(year=2011, drops=['COD_SEC', 'COD_ESPE', 'FEC_ING_ALU', 'SIT_FIN_R'])
    df_2012 = read_file(year=2012, drops=['COD_SEC', 'COD_ESPE', 'SIT_FIN_R'])
    df_2013 = read_file(year=2013, drops=['COD_SEC', 'COD_ESPE', 'COD_TIP_CUR', 'GD_ALU', 'COD_REG_ALU', 'COD_RAMA', 'SIT_FIN_R'])
    df_2014 = read_file(year=2014, drops=['COD_SEC', 'COD_ESPE', 'COD_RAMA', 'COD_REG_ALU', 'GD_ALU', 'COD_TIP_CUR', 'COD_DEPE2'])
    df_2015 = read_file(year=2015, drops=['COD_SEC', 'COD_ESPE', 'COD_DEPROV_RBD', 'NOM_DEPROV_RBD', 'COD_DEPE2', 'ESTADO_ESTAB', 'COD_TIP_CUR', 'GD_ALU', 'COD_REG_ALU', 'COD_RAMA', 'SIT_FIN_R'])
    df_2016 = read_file(year=2016, drops=['COD_SEC', 'COD_ESPE', 'SIT_FIN_R’, ‘COD_RAMA’, ‘COD_REG_ALU’, ‘COD_DES_CUR’, ‘COD_TIP_CUR’, ‘ESTADO_ESTAB’, ‘COD_DEPE2’, ‘NOM_DEPROV_RBD’, ‘COD_DEPROV_RBD’])
    df_2017 = read_file(year=2017, drops=['COD_SEC', 'COD_ESPE', 'COD_DEPROV_RBD’, ‘NOM_DEPROV_RBD’, ‘COD_DEPE2’, ‘ESTADO_ESTAB’, ‘COD_TIP_CUR’, ‘COD_DES_CUR’, ‘COD_REG_ALU’, ‘COD_RAMA’, ‘SIT_FIN_R’])
    df_2018 = read_file(year=2018, drops=['COD_SEC', 'COD_ESPE', 'SIT_FIN_R’, ‘COD_RAMA’, ‘COD_REG_ALU’, ‘COD_DES_CUR’, ‘COD_TIP_CUR’, ‘ESTADO_ESTAB’, ‘COD_DEPE2’, ‘NOM_DEPROV_RBD’, ‘COD_DEPROV_RBD’, ‘NOM_REG_RBD_A’])
    df_2019 = read_file(year=2019, drops=['COD_SEC', 'COD_ESPE', 'NOM_REG_RBD_A’, ‘COD_DEPROV_RBD’, ‘NOM_DEPROV_RBD’, ‘COD_2’, ‘ESTADO_ESTAB’, ‘COD_TIP_CUR’, ‘COD_DES_CUR’, ‘COD_REG_ALU’, ‘COD_RAMA’, ‘COD_MEN’, ‘SIT_FIN_R’])

    ram(info='Final ram usage 2010-2019')

    #del df_2010, df_2011, df_2012, df_2013, df_2014, df_2015, df_2016, df_2017, df_2018, df_2019
    # Exportamos archivos html para ver si las tablas están bien
    #html_2010 = df_2010.to_html("df_2010.html")


    for col in df_2010.columns: 
        print(col)
    """
    ram(info='Dispose memory 2010-2019') 
    print('Finished')
    