# Libraries
import pandas as pd
import time
import sqlalchemy
import numpy as np
import interface
from sqlalchemy import event

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


def get_dataframes(year=2010):
    dataframes = []
    columns_to_drop = interface.get_columns_to_drop()
    amount_csv = interface.get_amount_of_csv()
    for year in range(year, year+amount_csv):
        path = "datasets/Rendimiento por estudiante "+str(year)+".csv"
        print('Reading:', path)
   
        # Leemos los datos y separamos por ; porque algunos nombres de establecimientos poseen comas y dan error
        df = pd.read_csv(path, sep=';', low_memory=False, encoding='latin-1')
        df.columns = map(str.upper, df.columns)
        #required_columns = interface.get_required_columns(list=df_current_year.columns.tolist())
        #print(required_columns)
        drop = []
        df_columns = df.columns.values.tolist()
        for column in columns_to_drop:
            if column in df_columns:
                drop.append(column)
        print('Drop:', drop)
        df = df.drop(drop, inplace=True, axis=1)

        # Limpiar datos: Están en todos los años
        df.fillna({'SIT_FIN': '-'}, inplace=True)
        df['COD_SEC'] = df['COD_SEC'].replace([' '], 0)
        df['COD_ESPE'] = df['COD_ESPE'].replace([' '], 0)

        dataframes.append(df)

        #print('Cantidad de datos:', len(df))
        print("-----")
    return dataframes


if __name__ == "__main__":
    start_time = time.time()
    interface.get_ram(info='Loading program')
    dataframes = get_dataframes()
    
    """
    interface.ram(info='Loading program')
    interface.drop_dimensions()
    interface.create_dimensions()
    interface.insert_static_dimensions()
    interface.ram(info='Program loaded')
    """

    # Leemos los archivos
    #df_2010 = read_file(year=2010, drops=['SIT_FIN_R'])

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

    for col in df_2010.columns: 
        print(col)
    """
    interface.get_ram(info='Dispose memory 2010-2019')
    print('Finished')
