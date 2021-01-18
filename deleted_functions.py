### Alumnos agregados por funcion
def insert_alumno(list):
    for i in list:
        cursor.execute("""insert into
        dim_alumno(MRUN, GEN_ALU, FEC_NAC_ALU, INT_ALU, COD_COM)
        values(%s,%s,%s,%s,%s)""",(i[0],i[1],i[2],i[3],i[4]))
    connection.commit()


# Establecimiento agregados por funcion
def insert_establecimiento(list):
    for i in list:
        cursor.execute("""insert into
        dim_establecimiento(RBD, DGV_RBD, NOM_RBD, RURAL_RBD, COD_DEPE, COD_ESPE, COD_REG_RBD, COD_SEC, COD_REG_RBD, COD_COM)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9]))
    connection.commit()


# Notas agregadas por funcion
def insert_notas(list):
    for i in list:
        cursor.execute("""insert into
        dim_establecimiento(AGNO, MRUN, RBD, DGV_RBD, PROM_GRAL, ASISTENCIA, SIT_FIN, COD_ENSE, COD_ENSE2)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8]))
    connection.commit()


def insert_alumno_test():
    cursor.execute("""insert into
        alumno(MRUN, GEN_ALU, FEC_NAC_ALU, INT_ALU, COD_COM)
        values(%s,%s,%s,%s,%s)""",("1","0","19960511","1","15101"))
    connection.commit()

def insert_establecimiento_test():
    cursor.execute("""insert into
        establecimiento(RBD, DGV_RBD, NOM_RBD, RURAL_RBD, COD_DEPE, COD_ESPE, COD_REG_RBD, COD_PRO_RBD, COD_SEC, COD_COM)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",("1","1","colegio mio","1","1","0","1","11","0","15101"))
    connection.commit()


"""
# Creamos los dataframes
df_depe = pd.DataFrame(data_depe, columns = ['COD_DEPE', 'DEPENDENCIA_ESTABLECIMIENTO'])
df_region = pd.DataFrame(data_region, columns = ['COD_REG_RBD', 'REGION', 'REGION_ABREVIADO'])
df_provincia = pd.DataFrame(data_provincia, columns = ['COD_REG_RBD', 'COD_PRO_RBD', 'PROVINCIA'])
df_rural_rbd = pd.DataFrame(data_rural_rbd, columns = ['RURAL_RBD', 'INDICE_RURALIDAD'])
df_data_ense = pd.DataFrame(data_ense, columns = ['COD_ENSE', 'DESCRIPCION'])
df_grado = pd.DataFrame(data_grado, columns = ['COD_ENSE', 'COD_GRADO', 'NOMBRE_GRADO'])
df_genero = pd.DataFrame(data_genero, columns = ['GEN_ALU', 'GENERO'])
# Aqui va COD_COM_ALU
df_sit_fin = pd.DataFrame(data_sit_fin, columns = ['SIT_FIN', 'SITUACION_CIERRE'])
df_sit_fin_t = pd.DataFrame(data_sit_fin_t, columns = ['SIT_FIN_R', 'SITUACION_CIERRE_TRASLADADO'])
df_jornada = pd.DataFrame(data_jornada, columns=['COD_JOR', 'JORNADA'])
# int_alu = [INT_ALU, INDICADOR]
# sec = [COD_SEC, SECTOR_ECONOMICO]
# espe = [COD_SEC, COD_ESPE, ESPECIALIDAD]
# ense2 = [COD_ENSE2, DESCRIPCION]
"""

# La tabla de comuna hay que autogenerarla 
# Con los datos que tenemos en el dataset

#print(df_jornada)


"""
data_alu = [df["MRUN"], df["FEC_NAC_ALU"],df["GEN_ALU"], df["COD_COM_ALU"], df["INT_ALU"]]
headers_alu = ["mrun", "fec_nac_alu", "gen_alu", "cod_com", "int_alu"]
df_alu = pd.concat(data_alu, axis=1, keys=headers_alu)
df_alu = df_alu.drop_duplicates(subset=['mrun'])
df_alu = df_alu.reset_index(drop=True)
df_alu.to_sql('alumno',engine, method='multi', if_exists='append',index=False)
"""

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