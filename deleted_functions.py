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