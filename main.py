# Libraries
from numpy.core.defchararray import encode
import pandas as pd
import sqlalchemy
import numpy as np
import interface
from sqlalchemy import event
import time

# Exportamos hacia la base de datos en postgres
engine = sqlalchemy.create_engine(
    'postgresql://postgres:postgres@localhost:5432/rendimiento_escolar')

@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True
        cursor.commit()

def get_dataframes(start_time, year=2010):
    dataframes = None
    columns_to_drop = interface.get_columns_to_drop()
    amount_csv = interface.get_amount_of_csv()
    for year in range(year, year+amount_csv):
        print('------------------------------------------------------------')
        path = "datasets/Rendimiento por estudiante "+str(year)+".csv"
   
        # Leemos los datos y separamos por ; porque algunos nombres de establecimientos poseen comas y dan error
        encoding = 'utf-8'
        if year == 2014 or year == 2015:
            encoding = 'latin'
        if year == 2016 or year == 2018 or year == 2019:
            encoding += '-sig'

        print('Reading: '+path+' ('+encoding+')')
        interface.get_time(start_time)
        df = pd.read_csv(path, sep=';', low_memory=False, encoding=encoding)
        df.columns = map(str.upper, df.columns)

        drop = []
        df_columns = df.columns.values.tolist()
        for column in columns_to_drop:
            if column in df_columns:
                drop.append(column)
        #print('Dropped tables:', drop)
        df.drop(columns=drop, inplace=True, axis=1)

        # Limpiar datos: Están en todos los años
        df.fillna({'SIT_FIN': '-'}, inplace=True)
        df['SIT_FIN'] = df['SIT_FIN'].replace([' '], '-')
        df['COD_SEC'] = df['COD_SEC'].replace([' '], 0)
        df['COD_ESPE'] = df['COD_ESPE'].replace([' '], 0)
        df["PROM_GRAL"] = df["PROM_GRAL"].str.replace(',', ".").astype(float)
        
        # Faltan estos datos, rellenar vacios
        if year <= 2012:
            df["COD_PRO_RBD"] = np.nan # Está en 2013+
            df["COD_JOR"] = np.nan # Está en 2013+
        
        if year <= 2013:  # Esta solo en los años 2010-2013
            df['INT_ALU'] = df['INT_ALU'].replace(['.'], 2)
            df['INT_ALU'] = df['INT_ALU'].replace([' '], 2)
            df["COD_ENSE2"] = np.nan # Está en 2014+

        if year >= 2014: # Rellenar con vacíos
             df['INT_ALU'] = np.nan

        #print('Cantidad de datos:', len(df))
        if dataframes is None:
            dataframes = df
        else:
            dataframes = pd.concat([dataframes, df], ignore_index=True)
        #print(df.dtypes)
        del df
        #print(dataframes.columns.values.tolist())
        interface.get_ram(info='Added year to dataframe: ' + str(year))
        interface.get_time(start_time)

    print('------------------------------------------------------------')
    interface.get_ram(info='Instance dataframe 2010-2019')
    interface.get_time(start_time) 
    return dataframes


if __name__ == "__main__":
    # Inicio del programa
    interface.get_ram(info='Starting program')
    start_time = time.time()
    """
    # Cargar los datos base a la base de datos
    interface.drop_dimensions()
    interface.create_dimensions()
    interface.insert_static_dimensions()
    interface.get_time(start_time)
    """
    # Instanciar todos los dataframes en uno general ya limpiados
    df = get_dataframes(start_time)

    # Convertir la variable MRUN
    interface.get_ram(info='Converting type "MRUN" from dataframe to string')
    interface.get_time(start_time)
    df['MRUN'] = df['MRUN'].astype('string')
    interface.get_ram(info='Type converted')
    interface.get_time(start_time)
    
    """
    # Crear comunas de establecimiento y alumno, estan en todos los años (no está en la documentación)
    headers_com = ["COD_COM", "NOM_COM"]
    # Comunas donde están los establecimientos
    data_com_rbd = [df["COD_COM_RBD"], df["NOM_COM_RBD"]]
    df_com_rbd = pd.concat(data_com_rbd, axis=1, keys=headers_com)
    # Comunas donde provienen los alumnos
    data_com_alu = [df["COD_COM_ALU"], df["NOM_COM_ALU"]]
    df_com_alu = pd.concat(data_com_alu, axis=1, keys=headers_com)
    # Concatenamos las columnas
    df_com = pd.concat([df_com_rbd,df_com_alu])
    df_com = df_com.drop_duplicates(subset=['COD_COM'])
    df_com = df_com.reset_index(drop=True)
    # Insertamos datos a la dimensión comuna
    interface.insert_dim_comuna(df_com.values.tolist())
    interface.get_time(start_time)
    # Elimina residuales ram
    del headers_com, data_com_rbd, df_com_rbd, data_com_alu, df_com_alu, df_com
    df.drop(columns=['NOM_COM_RBD','NOM_COM_ALU'], inplace=True, axis=1)
    interface.get_ram(info='Dispose column comuna')
    interface.get_time(start_time) 

    # Agregar establecimientos
    data_establecimiento = [df["RBD"], df["DGV_RBD"], df["NOM_RBD"], df["RURAL_RBD"], df["COD_DEPE"], df["COD_REG_RBD"], df["COD_SEC"], df["COD_COM_RBD"]]
    headers_establecimiento = ['rbd', 'dgv_rbd', 'nom_rbd', 'rural_rbd', 'cod_depe', 'cod_reg_rbd', 'cod_sec', 'cod_com']
    interface.df_to_sql(table_name='establecimiento', engine=engine, data=data_establecimiento, headers=headers_establecimiento, remove_duplicates=['rbd','dgv_rbd'])
    del data_establecimiento, headers_establecimiento
    df.drop(columns=['NOM_RBD','RURAL_RBD','COD_DEPE','COD_REG_RBD','COD_SEC','COD_COM_RBD'], inplace=True, axis=1)
    interface.get_time(start_time)

    # Agregar alumnos
    data_alumno = [df["MRUN"], df["FEC_NAC_ALU"], df["GEN_ALU"], df["COD_COM_ALU"], df["INT_ALU"]]
    headers_alumno = ["mrun", "fec_nac_alu", "gen_alu", "cod_com", "int_alu"]
    interface.df_to_sql(table_name='alumno', engine=engine, data=data_alumno, headers=headers_alumno, remove_duplicates=['mrun'])
    del data_alumno, headers_alumno
    df.drop(columns=['FEC_NAC_ALU','GEN_ALU','COD_COM_ALU','INT_ALU'], inplace=True, axis=1)
    interface.get_time(start_time)
    """

    ### TESTING ###
    print('DROP TESTING')
    df.drop(columns=['NOM_COM_RBD','NOM_COM_ALU','NOM_RBD','RURAL_RBD','COD_DEPE','COD_REG_RBD','COD_SEC','COD_COM_RBD','FEC_NAC_ALU','GEN_ALU','COD_COM_ALU','INT_ALU'], inplace=True, axis=1)
    print('TESTING DROPPED')
    ### TESTING ###

    # Agregar notas
    data_notas = [df["AGNO"], df["MRUN"], df["RBD"], df["DGV_RBD"], df["PROM_GRAL"], df["SIT_FIN"], df['ASISTENCIA'], df['LET_CUR'], df["COD_ENSE"], df["COD_ENSE2"], df["COD_JOR"]]
    head_notas = ['agno', 'mrun', 'rbd', 'dgv_rbd', 'prom_gral', 'sit_fin', 'asistencia', 'let_cur', 'cod_ense', 'cod_ense2', 'cod_jor']
    interface.df_to_sql(table_name='notas', engine=engine, data=data_notas, headers=head_notas, remove_duplicates=['agno','mrun'])
    del data_notas, head_notas
    interface.get_ram(info='Inserted all data to database')
    interface.get_time(start_time)

    del df

    interface.get_ram(info='Dispose dataframe and finish program')
    interface.get_time(start_time)

