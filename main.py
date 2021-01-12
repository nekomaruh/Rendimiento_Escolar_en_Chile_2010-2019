# Libraries
import pandas as pd
import os, psutil
import time

# Project functions
import static_tables as st
import queries as q

start_time = time.time()

def read_file(year, drops):
    print('Reading dataset', year)
    # Leemos los datos y separamos por ; porque algunos nombres de establecimientos poseen comas y dan error
    df = pd.read_csv("datasets/Rendimiento por estudiante "+str(year)+".csv", sep=';', low_memory=False, encoding='latin-1')
    print('Cantidad de datos:',len(df))

    
    # Eliminamos las columnas
    if drops!=None:
        df = df.drop(columns=drops)

  
    df["SIT_FIN"].fillna( method ='-', inplace = True) # Esta en todos los años
    """
    df["COD_SEC"].fillna( method ='0', inplace = True) # Esta en todos los años
    df["COD_ESPE"].fillna( method ='0', inplace = True) # Esta en todos los años
    if year <= 2013:
        df['INT_ALU'] = df['INT_ALU'].replace(['.'],'2') # Esta solo en los años 2010-2013
    
    #df_com_rbd = df.copy()
    """

    """
    data_com_rbd = [df["COD_COM_ALU"], df["NOM_COM_RBD"]]
    headers_com_rbd = ["COD_COM_ALU", "NOM_COM_RBD"]

    df3 = pd.concat(data_com_rbd, axis=1, keys=headers_com_rbd)
    print(df3)
    """
    

    #df_2010_head = df_2010.head()
    #html_2010 = df_2010_head.to_html("datasets_headers_pdf/df_"+year+".html")
    ram(info='Dataframe ' + str(year))
    return df




def insert_dimensions():
    q.drop_static_tables() # Elimina las tablas estáticas si existen
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

def ram(info='unknown'):
    process = psutil.Process(os.getpid())
    print('RAM:', process.memory_info()[0]/8000000, 'mb ('+info+')')

if __name__ == "__main__":
    # Inserta todas las dimensiones estáticas
    
    ram(info='Loading program')
    insert_dimensions()

    del st
    ram(info='Delete imports')

    # Leemos los archivos
    df_2010 = read_file(year=2010, drops=['SIT_FIN_R'])
    """
    df_2011 = read_file(year=2011, drops=['COD_SEC', 'COD_ESPE', 'FEC_ING_ALU'])
    df_2012 = read_file(year=2012, drops=['COD_SEC', 'COD_ESPE'])
    df_2013 = read_file(year=2013, drops=['COD_SEC', 'COD_ESPE'])
    df_2014 = read_file(year=2014, drops=['COD_SEC', 'COD_ESPE'])
    df_2015 = read_file(year=2015, drops=['COD_SEC', 'COD_ESPE'])
    df_2016 = read_file(year=2016, drops=['COD_SEC', 'COD_ESPE'])
    df_2017 = read_file(year=2017, drops=['COD_SEC', 'COD_ESPE'])
    df_2018 = read_file(year=2018, drops=['COD_SEC', 'COD_ESPE'])
    df_2019 = read_file(year=2019, drops=['COD_SEC', 'COD_ESPE'])
    """

    ram(info='Final ram usage 2010-2019')

    #del df_2010, df_2011, df_2012, df_2013, df_2014, df_2015, df_2016, df_2017, df_2018, df_2019
    # Exportamos archivos html para ver si las tablas están bien
    #html_2010 = df_2010.to_html("df_2010.html")

    """
    for col in df_2010.columns: 
        print(col)
    """
    ram(info='Dispose memory 2010-2019') 
    print('Finished')
    