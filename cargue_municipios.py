import psycopg2
import pandas as pd
import requests
import unicodedata
import os
import numpy as np


url_api = 'https://api.openweathermap.org/data/2.5/weather?'
api_key = 'd3c9b45e4711eb532f82fab35fc5fb53'
header = { "Content-Type":"aplication/json",
            "Accept-Encoding": "deflate"}


''' Funcion para consumir API mediante parametros
    @return archivo json
'''
def datos_geo(Pais,Zip,Ciudad,url_api,api_key):
    end_p = f"{url_api}q={Ciudad},{Zip},{Pais}&appid={api_key}&units=metric"
    res = requests.get(end_p,headers=header)

    if res.status_code == 200:
        return res.json()
    
    else:
        None


''' Funcion para quitar tildes a los municipios'''
def quitar_tildes(texto):
    if isinstance(texto, str):  
        texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
        return texto
    return texto


''' Funcion para leer archivos de municipios u geojson '''
def transformar():
    df_mun = pd.read_excel(os.path.dirname(os.path.realpath(__file__)) + r'\Municipios.xlsx')
    df_mun.sort_values(by=['Nombre_depto', 'Nombre_mpio'], ascending=[True, True], inplace=True)

    df_mun['Latitud'] = None
    df_mun['Longitud'] = None

    df_Geo = pd.read_excel(os.path.dirname(os.path.realpath(__file__)) + r'\Geo_Municipios.xlsx')
    df_Geo.sort_values(by=['Nombre_depto', 'Nombre_mpio'], ascending=[True, True], inplace=True)

    # API Openweather
    for i, row in df_mun.iterrows():

        # Llamar metodo que consume API mediante parametros
        datos = datos_geo('CO', row['Cod_mpio'] , row['Nombre_mpio'], url_api, api_key)
        
        if datos:
            df_mun.at[i, 'Latitud'] = np.float64(datos['coord']['lat'])
            df_mun.at[i, 'Longitud'] = np.float64(datos['coord']['lon'])


    # Asignar coordenadas
    for i in range(len(df_mun)):
       
        if pd.isnull(df_mun.at[i, 'Latitud']) or pd.isnull(df_mun.at[i, 'Longitud']):
            
            df_mun.at[i, 'Latitud'] = df_Geo.at[i, 'Latitud']
            df_mun.at[i, 'Longitud'] = df_Geo.at[i, 'Longitud']
            
        else:
            
            if (df_mun.at[i, 'Latitud'] != df_Geo.at[i, 'Latitud'] or df_mun.at[i, 'Longitud'] != df_Geo.at[i, 'Longitud']):
                
                df_mun.at[i, 'Latitud'] = df_Geo.at[i, 'Latitud']
                df_mun.at[i, 'Longitud'] = df_Geo.at[i, 'Longitud']

    # Quitar tildes 
    df_mun = df_mun.apply(lambda col: col.map(lambda x: quitar_tildes(x.lower()) if isinstance(x, str) else x))

    # Cargar dataframe
    cargar_dataframe(df_mun)



''' Funcion para cargar dataframe a la base de datos'''
def cargar_dataframe(df_data):

    df = pd.DataFrame(df_data)

    conn = conexion_post()

    try:

        # Crear un cursor
        with conn.cursor() as cur:
            
            # Crear un comando SQL para hacer el INSERT
            insert_query = """INSERT INTO DM_MUNICIPIOS (ID_DANE_MUNICIPIO, ID_DANE_DEPARTAMENTO, MUNICIPIO, DEPARTAMENTO, LATITUD_MUNICIPIO, LONGITUD_MUNICIPIO) VALUES (%s, %s, %s, %s, %s, %s);"""

            for index, row in df.iterrows():

                filas = row['Cod_mpio'], row['Cod_depto'], row['Nombre_mpio'], row['Nombre_depto'], row['Latitud'], row['Longitud']

                # Ejecutar el comando SQL con los datos
                cur.execute(insert_query, filas)

            # Confirmar la transaccion
            conn.commit()

        print(f"Registros insertados exitosamente.")

    except Exception as error:
        print(f"Error al insertar en la base de datos: {error}")



''' Funcion para conectarse con la base de datos'''
def conexion_post():

    # Datos de conexión
    db_host = "rdsgestion.c3eis0mqyqce.us-east-2.rds.amazonaws.com"  # Endpoint de tu RDS
    db_port = "5432"  # Puerto por defecto de PostgreSQL
    db_name = "postgres"  # Nombre de la base de datos
    db_user = "RDSGestion"  # Usuario de la base de datos
    db_password = "WXAzIDAn1ZpAtSoO28vN"  # Contraseña del usuario

    # Establecer la conexión a la base de datos
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        ) 

        return connection

    except Exception as error:
        print(f"Error al conectar con base de datos: {error}")

if __name__ == '__main__':

    transformar()
