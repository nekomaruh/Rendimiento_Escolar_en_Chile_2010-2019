import psycopg2 as ps

connection = ps.connect(user="postgres", 
                        password="postgres", 
                        database="rendimiento_escolar", 
                        host="localhost", 
                        port="5432")

cursor = connection.cursor()

def drop_static_tables():
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
    print('Tables deleted successfully!')

def create_static_tables():
    
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
    
    # Tabla sector economico (CAMBIAR LOS VACIOS POR 0)
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_sec(
                        COD_SEC INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        SECTOR_ECONOMICO TEXT NOT NULL
                    );""")
    
    # Tabla especialidad (CAMBIAR LOS VACIOS POR 0)
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_espe(
                        COD_SEC INTEGER NOT NULL,
                        COD_ESPE INTEGER NOT NULL UNIQUE,
                        ESPECIALIDAD TEXT NOT NULL,
                        primary key(COD_SEC, COD_ESPE),
                        foreign key(COD_SEC) references dim_sec(COD_SEC)
                    );""")
    
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_ense2(
                        COD_ENSE2 INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        DESCRIPCION TEXT NOT NULL
                    );""")
    
    connection.commit()
    print("Tables created successfully!")
    
def create_tables():
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_alumno(
                        COD_ENSE2 INTEGER PRIMARY KEY NOT NULL UNIQUE,
                        DESCRIPCION TEXT NOT NULL
                    );""")

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