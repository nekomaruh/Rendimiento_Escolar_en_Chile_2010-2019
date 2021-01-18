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

def read_file(year, drops):
    print('Reading dataset', year)
    # Leemos los datos y separamos por ; porque algunos nombres de establecimientos poseen comas y dan error
    df = pd.read_csv("datasets/Rendimiento por estudiante "+str(year) +
                     ".csv", sep=';', low_memory=False, encoding='latin-1')
    print('Cantidad de datos:', len(df))

    # Eliminamos las columnas
    if drops != None:
        for col in drops:
            if col in df:
                df = df.drop(columns=col)

    # Están en todos los años
    df.fillna({'SIT_FIN': '-'}, inplace=True)
    df['COD_SEC'] = df['COD_SEC'].replace([' '], 0)
    df['COD_ESPE'] = df['COD_ESPE'].replace([' '], 0)

    if year <= 2013:  # Esta solo en los años 2010-2013
        df['INT_ALU'] = df['INT_ALU'].replace(['.'], 2)

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
    interface.df_to_sql(table_name='establecimiento', engine=engine, data=data_establecimiento, headers=headers_establecimiento, remove_duplicates=['rbd','dgv_rbd'])
    """

    # Agregar notas
    """
    data_notas = [df["AGNO"], df["MRUN"], df["RBD"], df["DGV_RBD"], df["PROM_GRAL"], df["SIT_FIN"], df["COD_ENSE"], df["COD_ENSE2"], df["COD_JOR"], df["COD_GRADO"]]
    head_notas = ['agno', 'mrun', 'rbd', 'dgv_rbd', 'prom_gral', 'sit_fin', 'cod_ense', 'cod_ense2', 'cod_jor', 'cod_grado']
    interface.df_to_sql(table_name='notas', engine=engine, data=data_notas, headers=head_notas, remove_duplicates=['agno','mrun'])
    """

    interface.ram(info='Dataframe ' + str(year))
    return df


def get_dataframes(start_time, year=2010):
    #dataframes = []
    columns_to_drop = interface.get_columns_to_drop()
    amount_csv = interface.get_amount_of_csv()
    for year in range(year, year+amount_csv):
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
        #required_columns = interface.get_required_columns(list=df_current_year.columns.tolist())
        #print(required_columns)
        drop = []
        df_columns = df.columns.values.tolist()
        for column in columns_to_drop:
            if column in df_columns:
                drop.append(column)
        #print('Drop:', drop)
        df.drop(columns=drop, inplace=True, axis=1)

        # Limpiar datos: Están en todos los años
        df.fillna({'SIT_FIN': '-'}, inplace=True)
        df['COD_SEC'] = df['COD_SEC'].replace([' '], 0)
        df['COD_ESPE'] = df['COD_ESPE'].replace([' '], 0)
        df["PROM_GRAL"] = df["PROM_GRAL"].str.replace(',', ".").astype(float)
        
        # Faltan estos datos, rellenar vacios
        if year <= 2012:
            df["COD_PRO_RBD"] = np.nan # Está en 2013+
            df["COD_JOR"] = np.nan # Está en 2013+
        
        if year <= 2013:  # Esta solo en los años 2010-2013
            df['INT_ALU'] = df['INT_ALU'].replace(['.'], 2)
            df["COD_ENSE2"] = np.nan # Está en 2014+

        if year >= 2014: # Rellenar con vacíos
             df['INT_ALU'] = np.nan

        """
        if 'Ï»¿AGNO' in df_columns:
            df = df.rename(columns={"Ï»¿AGNO": "AGNO"})
        """
        #dataframes.append(df)
        #print('Cantidad de datos:', len(df))

        if year == 2010:
            # Crear comunas de establecimiento y alumno, estan en todos los años
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

        """
        data_alu = [df["MRUN"], df["FEC_NAC_ALU"],df["GEN_ALU"], df["COD_COM_ALU"], df["INT_ALU"]]
        headers_alu = ["mrun", "fec_nac_alu", "gen_alu", "cod_com", "int_alu"]
        df_alu = pd.concat(data_alu, axis=1, keys=headers_alu)
        df_alu = df_alu.drop_duplicates(subset=['mrun'])
        df_alu = df_alu.reset_index(drop=True)
        df_alu.to_sql('alumno',engine, method='multi', if_exists='append',index=False)
        """

        # Agregar alumnos
        data_alumno = [df["MRUN"], df["FEC_NAC_ALU"],df["GEN_ALU"], df["COD_COM_ALU"], df["INT_ALU"]]
        headers_alumno = ["mrun", "fec_nac_alu", "gen_alu", "cod_com", "int_alu"]
        interface.df_to_sql(table_name='alumno', engine=engine, data=data_alumno, headers=headers_alumno, remove_duplicates=['mrun'])
        interface.get_time(start_time)
        
        # Agregar establecimientos
        data_establecimiento = [df["RBD"], df["DGV_RBD"], df["NOM_RBD"], df["RURAL_RBD"], df["COD_DEPE"], df["COD_REG_RBD"], df["COD_SEC"], df["COD_COM_RBD"]]
        headers_establecimiento = ['rbd', 'dgv_rbd', 'nom_rbd', 'rural_rbd', 'cod_depe', 'cod_reg_rbd', 'cod_sec', 'cod_com']
        interface.df_to_sql(table_name='establecimiento', engine=engine, data=data_establecimiento, headers=headers_establecimiento, remove_duplicates=['rbd','dgv_rbd'])
        interface.get_time(start_time)

        # Agregar notas
        data_notas = [df["AGNO"], df["MRUN"], df["RBD"], df["DGV_RBD"], df["PROM_GRAL"], df["SIT_FIN"], df['ASISTENCIA'], df['LET_CUR'], df["COD_ENSE"], df["COD_ENSE2"], df["COD_JOR"]]
        head_notas = ['agno', 'mrun', 'rbd', 'dgv_rbd', 'prom_gral', 'sit_fin', 'asistencia', 'let_cur', 'cod_ense', 'cod_ense2', 'cod_jor']
        interface.df_to_sql(table_name='notas', engine=engine, data=data_notas, headers=head_notas, remove_duplicates=['agno','mrun'])
        interface.get_time(start_time)
        #print(df.columns.values.tolist())

        interface.get_ram(info='Tables added to year: ' + str(year))
        interface.get_time(start_time)
    #return dataframes


if __name__ == "__main__":
    interface.get_ram(info='Starting program')
    start_time = time.time()
    
    # Cargar los datos base
    interface.drop_dimensions()
    interface.create_dimensions()
    interface.insert_static_dimensions()
    interface.get_time(start_time)
    
    # Instanciar todos los dataframes
    get_dataframes(start_time, year=2010)
    interface.get_ram(info='Instanced all dataframes')
    interface.get_time(start_time)

    #del dataframes
    
    interface.get_ram(info='Process finished')
    interface.get_time(start_time)

