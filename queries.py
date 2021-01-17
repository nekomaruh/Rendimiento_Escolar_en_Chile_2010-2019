import psycopg2 as ps

connection = ps.connect(user="postgres", 
                        password="postgres", 
                        database="rendimiento_escolar", 
                        host="localhost", 
                        port="5432")

cursor = connection.cursor()

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

def drop_tables():
    cursor.execute("DROP TABLE IF EXISTS notas;")
    cursor.execute("DROP TABLE IF EXISTS alumno;")
    cursor.execute("DROP TABLE IF EXISTS establecimiento;")

    cursor.execute("DROP TABLE IF EXISTS dim_com;")
    cursor.execute("DROP TABLE IF EXISTS dim_ense2;")
    cursor.execute("DROP TABLE IF EXISTS dim_espe;")
    cursor.execute("DROP TABLE IF EXISTS dim_sec;")
    cursor.execute("DROP TABLE IF EXISTS dim_int_alu;")
    cursor.execute("DROP TABLE IF EXISTS dim_jornada;")
    cursor.execute("DROP TABLE IF EXISTS dim_sit_fin;")
    cursor.execute("DROP TABLE IF EXISTS dim_genero;")
    cursor.execute("DROP TABLE IF EXISTS dim_grado;")
    cursor.execute("DROP TABLE IF EXISTS dim_ense;")
    cursor.execute("DROP TABLE IF EXISTS dim_rural;")
    cursor.execute("DROP TABLE IF EXISTS dim_provincia;")
    cursor.execute("DROP TABLE IF EXISTS dim_region;")
    cursor.execute("DROP TABLE IF EXISTS dim_depe;")
    connection.commit()

def create_static_tables():

    # Tabla comuna
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_com(
                        COD_COM INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        NOM_COM TEXT NOT NULL);""")
    
    # Tabla dependencia
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_depe(
                        COD_DEPE INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        DEPENDENCIA_ESTABLECIMIENTO TEXT NOT NULL);""")
    
    # Tabla región
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_region(
                        COD_REG_RBD INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        REGION TEXT NOT NULL,
                        REGION_ABREVIADO TEXT NOT NULL
                        );""")
    
    # Tabla provincia
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_provincia(
                        COD_REG_RBD INTEGER NOT NULL,
                        COD_PRO_RBD INTEGER NOT NULL UNIQUE,
                        PROVINCIA TEXT NOT NULL,
                        primary key(COD_REG_RBD, COD_PRO_RBD),
                        foreign key(COD_REG_RBD) references dim_region(COD_REG_RBD)
                    );""")
    
    # Tabla ruralidad
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_rural(
                        RURAL_RBD INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        INDICE_RURALIDAD TEXT NOT NULL
                    );""")
    
    # Tabla enseñanza
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_ense(
                        COD_ENSE INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        DESCRIPCION TEXT NOT NULL
                    );""")
    
    # Tabla grado
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_grado(
                        COD_ENSE INTEGER NOT NULL,
                        COD_GRADO INTEGER NOT NULL,
                        NOMBRE_GRADO TEXT NOT NULL,
                        primary key(COD_ENSE, COD_GRADO),
                        foreign key(COD_ENSE) references dim_ense(COD_ENSE)
                    );""")
    
    # Tabla genero
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_genero(
                        GEN_ALU INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        GENERO TEXT NOT NULL
                    );""")
    
    # Tabla situacion final
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_sit_fin(
                        SIT_FIN TEXT PRIMARY KEY NOT NULL UNIQUE,
                        SITUACION_CIERRE TEXT NOT NULL
                    );""")
    
    # Tabla jornada
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_jornada(
                        COD_JOR INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        JORNADA TEXT NOT NULL
                    );""")
    
    # Tabla int alu
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_int_alu(
                        INT_ALU INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        INDICADOR TEXT NOT NULL
                    );""")
    
    # Tabla sector economico
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_sec(
                        COD_SEC INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        SECTOR_ECONOMICO TEXT NOT NULL
                    );""")
    
    # Tabla especialidad
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_espe(
                        COD_SEC INTEGER NOT NULL,
                        COD_ESPE INTEGER NOT NULL UNIQUE,
                        ESPECIALIDAD TEXT NOT NULL,
                        primary key(COD_SEC, COD_ESPE),
                        foreign key(COD_SEC) references dim_sec(COD_SEC)
                    );""")
    
    # Tabla enseñanza resumida
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_ense2(
                        COD_ENSE2 INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        DESCRIPCION TEXT NOT NULL
                    );""")
    
    connection.commit()


def create_tables():
    cursor.execute("""CREATE TABLE IF NOT EXISTS alumno(
                        MRUN INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        FEC_NAC_ALU INTEGER,
                        GEN_ALU INTEGER,
                        COD_COM INTEGER,
                        INT_ALU INTEGER,
                        foreign key (GEN_ALU) references dim_genero(GEN_ALU),
                        foreign key (COD_COM) references dim_com(COD_COM),
                        foreign key (INT_ALU) references dim_int_alu(INT_ALU)
                    );""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS establecimiento(
                        RBD INTEGER NOT NULL,
                        DGV_RBD INTEGER NOT NULL,
                        NOM_RBD TEXT,
                        RURAL_RBD INTEGER,
                        COD_DEPE INTEGER,
                        COD_REG_RBD INTEGER,
                        COD_SEC INTEGER,
                        COD_COM INTEGER,
                        primary key (RBD, DGV_RBD),
                        foreign key (RURAL_RBD) references dim_rural(RURAL_RBD),
                        foreign key (COD_DEPE) references dim_depe(COD_DEPE),
                        foreign key (COD_REG_RBD) references dim_region(COD_REG_RBD),
                        foreign key (COD_SEC) references dim_sec(COD_SEC),
                        foreign key (COD_COM) references dim_com(COD_COM)
                    );""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS notas(
                        AGNO INTEGER NOT NULL,
                        MRUN INTEGER NOT NULL,
                        RBD INTEGER,
                        DGV_RBD INTEGER,
                        PROM_GRAL FLOAT,
                        SIT_FIN TEXT,
                        ASISTENCIA INTEGER,
                        LET_CUR INTEGER,
                        COD_ENSE INTEGER,
                        COD_ENSE2 INTEGER,
                        COD_JOR INTEGER,
                        primary key (AGNO, MRUN),
                        foreign key (MRUN) references alumno(MRUN),
                        foreign key (RBD, DGV_RBD) references establecimiento(RBD, DGV_RBD),
                        foreign key (SIT_FIN) references dim_sit_fin(SIT_FIN),
                        foreign key (COD_ENSE) references dim_ense(COD_ENSE),
                        foreign key (COD_ENSE2) references dim_ense2(COD_ENSE2),
                        foreign key (COD_JOR) references dim_jornada(COD_JOR)
                    );""")
    
    connection.commit()

def insert_dim_depe(list):
    for i in list:
        cursor.execute("""insert into
        dim_depe(COD_DEPE, DEPENDENCIA_ESTABLECIMIENTO)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()

def insert_dim_region(list):
    for i in list:
        cursor.execute("""insert into
        dim_region(COD_REG_RBD, REGION, REGION_ABREVIADO)
        values(%s,%s,%s)""",(i[0], i[1], i[2]))
    connection.commit()

def insert_dim_provincia(list):
    for i in list:
        cursor.execute("""insert into
        dim_provincia(COD_REG_RBD, COD_PRO_RBD, PROVINCIA)
        values(%s,%s,%s)""",(i[0], i[1], i[2]))
    connection.commit()

def insert_dim_rural(list):
    for i in list:
        cursor.execute("""insert into
        dim_rural(RURAL_RBD, INDICE_RURALIDAD)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()

def insert_dim_ense(list):
    for i in list:
        cursor.execute("""insert into
        dim_ense(COD_ENSE, DESCRIPCION)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()

def insert_dim_grado(list):
    for i in list:
        cursor.execute("""insert into
        dim_grado(COD_ENSE, COD_GRADO, NOMBRE_GRADO)
        values(%s,%s,%s)""",(i[0], i[1], i[2]))
    connection.commit()
    
def insert_dim_rural(list):
    for i in list:
        cursor.execute("""insert into
        dim_rural(RURAL_RBD, INDICE_RURALIDAD)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()

def insert_dim_ense(list):
    for i in list:
        cursor.execute("""insert into
        dim_ense(COD_ENSE, DESCRIPCION)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()

def insert_dim_grado(list):
    for i in list:
        cursor.execute("""insert into
        dim_grado(COD_ENSE, COD_GRADO, NOMBRE_GRADO)
        values(%s,%s,%s)""",(i[0], i[1], i[2]))
    connection.commit()

def insert_dim_genero(list):
    for i in list:
        cursor.execute("""insert into
        dim_genero(GEN_ALU, GENERO)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()

def insert_dim_sit_fin(list):
    for i in list:
        cursor.execute("""insert into
        dim_sit_fin(SIT_FIN, SITUACION_CIERRE)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()
    
def insert_dim_jornada(list):
    for i in list:
        cursor.execute("""insert into
        dim_jornada(COD_JOR, JORNADA)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()
    
def insert_dim_int_alu(list):
    for i in list:
        cursor.execute("""insert into
        dim_int_alu(INT_ALU, INDICADOR)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()
    
def insert_dim_sec(list):
    for i in list:
        cursor.execute("""insert into
        dim_sec(COD_SEC, SECTOR_ECONOMICO)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()

def insert_dim_espe(list):
    for i in list:
        cursor.execute("""insert into
        dim_espe(COD_SEC, COD_ESPE, ESPECIALIDAD)
        values(%s,%s,%s)""",(i[0], i[1], i[2]))
    connection.commit()

def insert_dim_ense2(list):
    for i in list:
        cursor.execute("""insert into
        dim_ense2(COD_ENSE2, DESCRIPCION)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()


### Comunas agregadas por funcion
def insert_dim_com(list):
    for i in list:
        cursor.execute("""insert into
        dim_com(COD_COM, NOM_COM)
        values(%s,%s)""",(i[0], i[1]))
    connection.commit()


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