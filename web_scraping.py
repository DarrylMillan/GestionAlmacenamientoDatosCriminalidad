# Script para descarga de archivos de excel desde la pagina de la policia nacional
# mediante la tecnica de web scraping.
# Transformacion de los archivos descargados y cargue a la tabla de hechos delitos
# en la base de datos AWS


# Librerias
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import psycopg2
import time
import os
import re
from datetime import datetime

# URL de la página web
base_url = 'https://www.policia.gov.co/estadistica-delictiva?page='

encabezado = { "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36" }


""" Funcion para realizar web scraping y descargar los archivos de excel desde la pagina de la policia nacional """
def descargar_webscraping():

    # Ruta donde se aloja el script de python
    ruta = os.path.dirname(os.path.realpath(__file__))

    # Cantida de paginas a consultar
    page = 27

    # Ciclo para recorrer todas las páginas
    for i in range(page):

        df_lista = ''
        resultados = []
        page_url = base_url + str(i+1)
        
        print(f'\n- Pagina {str(i + 1)} **************************')

        # Realizar solicitud HTTP para obtener el contenido de la página
        response = requests.get(page_url, headers=encabezado)

        # Validar respuesta de peticion get al servidor
        if response.status_code == 200:
            
            # Capturar contenido HTML
            soup = BeautifulSoup(response.content, 'html.parser')

            # Leer valores de la tabla
            contenedor_tbody = soup.find('tbody')
            filas_tr = contenedor_tbody.find_all('tr')

            # Recorrer filas de la tabla
            for fila in filas_tr:
                
                # Capturar valores de las celdas de la tabla
                celdas_filas = fila.find_all('td') 
                valores_posicion = [value.get_text(strip=True) for value in celdas_filas]

                # Capturar enlaces de los archivos
                enlaces = fila.find_all('a', href=True)
                excel_enlaces = [enlace['href'] for enlace in enlaces if enlace['href'].endswith('.xlsx')]


                # Almacenar informacion en lista resultado
                if excel_enlaces:

                    resultados.append({
                        'Nombre_delito': valores_posicion[0],
                        'Anio_delito': valores_posicion[1],
                        'Enlace_archivo': 'https://www.policia.gov.co' + excel_enlaces[0]
                    })
                
                else:
                    print('Lista vacia', i+1)
                    next

            # Crear dataframe de la lista resultados
            df_lista = pd.DataFrame(resultados)

            if df_lista.empty == False:

                # Imprimir dataframe resultado
                # print(df_lista)

                # Definir la carpeta de destino para los archivos descargados
                output_dir = ruta + r'\Bases sin procesar\Bases web scraping'
                os.makedirs(output_dir, exist_ok=True)

                time.sleep(3)

                # Descargar archivos de excel
                for index, row in df_lista.iterrows():

                    nombre_archivo = str(row['Nombre_delito']).replace(' ', '_')
                    anio = str(row['Anio_delito']) 
                    extension = str(row['Enlace_archivo']).split('.')[-1]

                    ruta_archivo = os.path.join(output_dir, nombre_archivo + '_' + anio + '.' + extension)

                    nombre_excel = nombre_archivo + '_' + anio + '.' + extension

                    # Enviar peticion de obtener archivo
                    archivo_respuesta = requests.get(row['Enlace_archivo'])

                    # Crear archivo en la ruta local
                    with open(ruta_archivo, 'wb') as file:
                        file.write(archivo_respuesta.content)

                    print(f'\n  {index + 1}. Descargue archivo: {nombre_excel}')
                   
            else:
                next

        else:
            print('Sin respuesta del servidor!')

    print(' \n************************** Proceso descarga finalizado **************************')



''' Funcion para consolidar archivos de excel descargados con web scraping en un solo archivo '''
def consolidar_data(delito):
    
    # Establecer ruta desde donde se leen los archivos ya descargados
    ruta = os.path.dirname(os.path.realpath(__file__)) + r'\Bases sin procesar\Bases web scraping'

    # Lista para almacenar los dataframes de cada archivo
    dataframes = []

    # Patrones que ayudan a filtrar archivos o contenido de los archivos
    patron = re.compile(f'^{delito}')
    patron_titulo = re.compile('^MINISTERIO')
    
    # Validar si la ruta donde se almacenan los archivos existe
    if os.path.exists(ruta):

        # Listar los archivos .xlsx
        with os.scandir(ruta) as archivos:

            for archivo in archivos:
                
                # Filtrar archivos y validar extensiones
                if archivo.is_file() and archivo.name.endswith('.xlsx') and patron.search(archivo.name):

                    nombre = archivo.name
                    print( f'\n- Archivo: {nombre}')

                    # Leer cada archivo de excel y añadirlo a un dataframe
                    ruta_archivo = os.path.join(ruta, nombre)
                    df = pd.read_excel(ruta_archivo)

                    # Reasignar encabezados del dataframe
                    for colname in df.columns:

                        # Buscar columna que contenga la palabra 'MINISTERIO' y la asigna a la variable column_name
                        if patron_titulo.search(colname):

                            column_name = colname

                            # Iterar por fila para reasignar encabezados
                            for index, row in df.iterrows():

                                # Eliminar las filas anteriores a la fila que contenga la palabra 'DEPARTAMENTO'
                                # La fila que contenga la palabra 'DEPARTAMENTO' convertirla en encabezados
                                if row[column_name] != 'DEPARTAMENTO':

                                    # Eliminar filas anteriores al titulo DEPARTAMENTO 
                                    df.drop(index, axis = 0, inplace = True)

                                else:

                                    # Reiniciar los indices
                                    df.reset_index(inplace = True)

                                    # Eliminar columna index que se crea despues de reiniciar los indices
                                    df = df.drop('index', axis = 1)

                                    # Convertir la fila 0 en los encabezados del dataframe
                                    df = df.rename(columns = df.iloc[0]).loc[1:]

                                    break

                    # Renombrar columnas
                    df = df.rename(columns={'CODIGO DANE':'CODIGO_DANE', 'ARMAS MEDIOS':'ARMA_EMPLEADA', 'FECHA HECHO':'FECHA_HECHO', 'GENERO':'SEXO', 'AGRUPA EDAD PERSONA':'AGRUPA_EDAD_PERSONA', '*AGRUPA EDAD PERSONA':'AGRUPA_EDAD_PERSONA'})

                    # Eliminar pie de pagina invirtiendo el dataframe para recorrerlo de abajo hacia arriba
                    for index, row in df.iloc[::-1].iterrows():
                                
                        # Eliminar las filas anteriores a la fila que contenga la palabra 'TOTAL'
                        # La fila que contenga la palabra 'TOTAL' se quita afuera del ciclo
                        if row['DEPARTAMENTO'] != 'TOTAL':

                            # Eliminar filas anteriores al valor TOTAL 
                            df.drop(index, axis = 0, inplace = True)

                        else:

                            # Reiniciar los indices
                            df.reset_index(inplace = True)

                            # Eliminar columna index que se crea despues de reiniciar los indices
                            df = df.drop('index', axis = 1)

                            break
                    
                    # Eliminar fila total
                    df = df.iloc[:-1]

                    # Agregar columnas
                    if delito == 'Amenazas':
                        df['CONFLICTIVIDAD'] = 'amenaza'

                    elif delito == 'Delitos_sexuales':
                        df['CONFLICTIVIDAD'] = 'delito sexual'

                    elif delito == 'Homicidios':
                        df['CONFLICTIVIDAD'] = 'homicidio'

                    elif delito == 'Hurto_a_personas':
                        df['CONFLICTIVIDAD'] = 'hurto persona'

                    elif delito == 'Lesiones_personales':
                        df['CONFLICTIVIDAD'] = 'lesiones personales'

                    elif delito == 'Extorsión':
                        df['CONFLICTIVIDAD'] = 'extorsion'

                    elif delito == 'Secuestro':
                        df['CONFLICTIVIDAD'] = 'secuestro'

                    elif delito == 'Terrorismo':
                        df['CONFLICTIVIDAD'] = 'terrorismo'


                    # Asignar id tipo delito
                    tipo_conflictividad = {
                        'CONFLICTIVIDAD': ['amenaza', 'delito sexual', 'homicidio', 'hurto persona', 'lesiones personales', 'extorsion', 'secuestro', 'terrorismo'],
                        'ID_TIPO_DELITO': ['1', '2', '3', '4', '5', '6', '7', '8']
                    }

                    # Crear dataframe con diccionario tipo delito
                    df_tipo = pd.DataFrame(tipo_conflictividad)

                    # Asignar id tipo delito al dataframe
                    df = pd.merge(df, df_tipo, on = 'CONFLICTIVIDAD')

                    # Imprimir dataframe
                    # print(df)
                    
                    # Anexar dataframea la lista datframes
                    dataframes.append(df)


            # Concatenar todos los dataFrames en uno solo
            concatenar_dataframes = pd.concat(dataframes, ignore_index=True)

            # Reiniciar los indices
            concatenar_dataframes.reset_index(inplace = True)

            # Eliminar columna index que se crea despues de reiniciar los indices
            concatenar_dataframes.drop('index', axis = 1, inplace = True)

            # Filtrar columnas especificas del dataframe
            concatenar_dataframes = concatenar_dataframes.loc[:, ['CODIGO_DANE', 'ID_TIPO_DELITO', 'ARMA_EMPLEADA', 'FECHA_HECHO', 'SEXO', 'AGRUPA_EDAD_PERSONA']]

            # Transformar dataframe
            df_final = transformar(concatenar_dataframes)
            
            # Imprimir dataframe consolidado y transformado
            # print(df_final)

            # Descargar dataframe unificado por delito 
            df_final.to_excel(os.path.dirname(os.path.realpath(__file__)) + r'\Bases procesadas\Base_unida_' + f'{delito}.xlsx', index=False)


    else:
        print('Ruta no existe!')

    print(' ************************** Consolidacion y transformacion finalizados **************************')



''' Metodo para limpiar y transformar dataframe '''
def transformar(df_):
    
    # daraframe a transformar
    df = pd.DataFrame(df_)

    # Transformar dataframe
    for col in df.columns:

        # Columnas numericas
        if col in ['CODIGO_DANE']:
            df[col] = df[col].fillna(0).astype(np.int64)

        elif col in ['ID_TIPO_DELITO']:
            df[col] = df[col].fillna('').astype(str)
        
        # Columnas de texto
        elif col in ['ARMA_EMPLEADA']:
            df[col] = df[col].fillna('sin dato')
            df[col] = df[col].astype(str).apply(lambda x: x.lower().replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u').replace('no reporta', 'sin dato').replace('no reportado', 'sin dato').replace('no registra', 'sin dato'))

        elif col in ['SEXO']:
            df[col] = df[col].fillna('sin dato')
            df[col] = df[col].astype(str).apply(lambda x: x.lower().replace('masculino', 'hombre').replace('femenino', 'mujer').replace('no reporta', 'sin dato').replace('no reportado', 'sin dato').replace('no registra', 'sin dato').replace('no resportado', 'sin dato').replace('-', 'sin dato'))

        elif col in ['AGRUPA_EDAD_PERSONA']:
            df[col] = df[col].fillna('sin dato')
            df[col] = df[col].astype(str).apply(lambda x: x.lower().replace('adultos', 'adulto').replace('no reporta', 'sin dato').replace('no reportado', 'sin dato').replace('no registra', 'sin dato').replace('no resportado', 'sin dato').replace('-', 'sin dato'))

        # columnas fechas
        elif col in ['FECHA_HECHO']:
            df[col] = pd.to_datetime(df[col], dayfirst=True)
            df[col] = df[col].dt.strftime('%Y-%m-%d')

    # Reasignar valor del codigo postal 
    for index, row in df.iterrows():
        if len(str(row['CODIGO_DANE'])) == 8:
            df.at[index, 'CODIGO_DANE'] = str(row['CODIGO_DANE'])[:5]

        else:
            df.at[index, 'CODIGO_DANE'] = str(row['CODIGO_DANE'])[:4]

    # Reclasificar la columna grupo edad
    df['AGRUPA_EDAD_PERSONA'] = [value if value in ['adulto', 'sin dato'] else 'menor de edad' for value in df['AGRUPA_EDAD_PERSONA']]

    # retorna dataframe
    return df



''' Funcion para cargar dataset recibido por parametros a la base de datos AWS'''
def cargar_dataset(dataset, tamano):

    inicio_cargue = datetime.now()

    print(f'\n - Dataset: {os.path.split(dataset)[1]}')

    # Leer dataset
    df_dataset = pd.read_excel(dataset)

    df_dataset['CODIGO_DANE'] = df_dataset['CODIGO_DANE'].fillna(0).astype(np.int64)

    
    # Imprimir dataframe
    print(df_dataset)

    # Abrir conexion con base de datos
    conn = conexion_post()

    try:

        # Insertar data en la base de datos de manera incremental de 1000 en 1000
        with conn.cursor() as cur:

            # Query sql
            insert_sql = """INSERT INTO H_DELITOS (ID_DANE_MUNICIPIO, ID_TIPO_DELITO, ARMAS_MEDIOS, FECHA_HECHO, GENERO, AGRUPA_EDAD_PERSONA) VALUES (%s, %s, %s, %s, %s, %s);"""

            # Recorrido en batches de 100
            for i in range(0, len(df_dataset), tamano):

                batch = df_dataset.iloc[i:i + tamano]

                print(batch)

                # Generar lista de tuplas de cada fila del batch
                values = [tuple(row) for row in batch.values]

                # Envio de datos a base de datos AWS
                for row in values:
                    cur.execute(insert_sql, row)

                conn.commit()

    except Exception as e:

        print(' - Error al cargar dataframe:', e)

    finally:

        # Cerrar conexion 
        cur.close()
        conn.close()

        print(' - Cargue finalizado...', '{}'.format(datetime.now() - inicio_cargue), '\n')



''' Funcion para retornar conexion con base de datos postgres en AWS '''
def conexion_post():

    # Datos de conexión
    db_host = "rdsgestion.c3eis0mqyqce.us-east-2.rds.amazonaws.com"  # Endpoint RDS
    db_port = "5432"  # Puerto por defecto de PostgreSQL
    db_name = "postgres"  # Nombre de la base de datos
    db_user = "RDSGestion"  # Usuario de la base de datos
    db_password = "WXAzIDAn1ZpAtSoO28vN"  # Contraseña del usuario

    try:

        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )

        return connection

    except Exception as e:

        print(' - Error al conectar con servidor:', e)



# Inicializar script
if __name__ == '__main__':

    ''' Descargar archivos pagina policia nacional con la tecnica de web scraping '''

    # # Tecnica de web scraping
    descargar_webscraping()

    time.sleep(3)


    ''' Consolidar archivos en un solo dataset por delito '''

    # Unificar archivos descargados de delitos en un solo dataset por tipo de delito
    consolidar_data('Amenazas')
    consolidar_data('Delitos_sexuales')
    consolidar_data('Homicidios')
    consolidar_data('Hurto_a_personas')
    consolidar_data('Extorsión')
    consolidar_data('Lesiones_personales')
    consolidar_data('Secuestro')

    time.sleep(3)


    ''' Cargar datasets a base de datos AWS '''

    # Cargar archivos unificados por delito a la base de datos AWS
    ruta = os.path.dirname(os.path.realpath(__file__)) + r'\Bases procesadas'
    filtro = 'Base_unida'  

    # Filtrar cada archivo descargado
    for archivo in os.listdir(ruta):
        if archivo.endswith('.xlsx') and archivo.startswith(filtro):
            
            ruta_completa = os.path.join(ruta, archivo)

            # Imprimir ruta del dataset a cargar
            # print(ruta_completa)

            # Cargar dataset a la base de datos en batches de 1000 en 1000
            cargar_dataset(ruta_completa, 1000)

            time.sleep(3)


