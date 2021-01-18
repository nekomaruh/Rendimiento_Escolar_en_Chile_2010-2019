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