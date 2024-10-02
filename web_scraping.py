# Librerias
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
import time
import re
import numpy as np

# URL de la página web
# url = 'https://www.policia.gov.co/estadistica-delictiva'

base_url = 'https://www.policia.gov.co/estadistica-delictiva?page='

encabezado = { "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36" }


""" Funcion para realizar web scraping y descargar los archivos de excel"""
def descargar_bases():

    ruta = os.path.dirname(os.path.realpath(__file__))
    page = 27

    # Ciclo para recorrer todas las páginas de la paginación
    for i in range(page):

        df_lista = ''
        resultados = []
        page_url = base_url + str(i+1)
        
        print(f'\n- Pagina {str(i + 1)} **************************')

        # Realizar la solicitud HTTP para obtener el contenido de la página
        response = requests.get(page_url, headers=encabezado)

        # Validar respuesta de peticion get al servidor
        if response.status_code == 200:
        
            soup = BeautifulSoup(response.content, 'html.parser')

            # Leer valores de la tabla
            contenedor_tbody = soup.find('tbody')
            filas_tr = contenedor_tbody.find_all('tr')

            # Recorrer filas de la tabla
            for fila in filas_tr:
                
                # Capturar valores de celdas
                celdas_filas = fila.find_all('td') 

                valores_posicion = [value.get_text(strip=True) for value in celdas_filas]

                # Capturar enlaces de archivos
                enlaces = fila.find_all('a', href=True)

                excel_enlaces = [enlace['href'] for enlace in enlaces if enlace['href'].endswith('.xlsx')]

                if excel_enlaces:

                    # Almacenar informacion en lista resultado
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

        # # Filtrar por delito de impacto y año
        # def filtrar_archivo_excel(file_path, delito, año):
        #     # Cargar el archivo Excel en un DataFrame de pandas
        #     df = pd.read_excel(file_path)
            
        #     # Filtrar por delito de impacto y año
        #     df_filtrado = df[(df['Delito'] == delito) & (df['Año'] == año)]
            
        #     return df_filtrado

        # # Ejemplo de uso
        # delito = 'Homicidio'  # Reemplaza con el delito deseado
        # año = 2023  # Reemplaza con el año deseado

        # # Filtrar cada archivo descargado
        # for file_name in os.listdir(output_dir):
        #     if file_name.endswith('.xlsx'):
        #         file_path = os.path.join(output_dir, file_name)
        #         df_filtrado = filtrar_archivo_excel(file_path, delito, año)
        #         print(df_filtrado)


''' Funcion para consolidar dataframes por delito en un solo archivo '''
def consolidar_data(delito):
    
    ruta = os.path.dirname(os.path.realpath(__file__)) + r'\Bases sin procesar\Bases web scraping'

    # Lista para almacenar los dataFrames de cada archivo
    dataframes = []
    patron = re.compile(f'^{delito}')
    patron_titulo = re.compile('^MINISTERIO')
    
    # Validar si la ruta donde se almacenan los archivos existe
    if os.path.exists(ruta):

        # Listar los archivos .xlsx
        with os.scandir(ruta) as archivos:

            for archivo in archivos:
                
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

               
                    if 'SEXO' not in df.columns: 
                        df['SEXO'] = 'sin dato '
                    
                    if 'AGRUPA_EDAD_PERSONA' not in df.columns: 
                        df['AGRUPA_EDAD_PERSONA'] = 'sin dato '

                    # Asignar id tipo delito
                    tipo_conflictividad = {
                        'CONFLICTIVIDAD': ['amenaza', 'delito sexual', 'homicidio', 'hurto persona', 'lesiones personales', 'extorsion', 'secuestro', 'terrorismo'],
                        'ID_TIPO_DELITO': ['1', '2', '3', '4', '5', '6', '7', '8']
                    }

                    # Crear dataframe con diccionario tipo delito
                    df_tipo = pd.DataFrame(tipo_conflictividad)
                    df = pd.merge(df, df_tipo, on = 'CONFLICTIVIDAD')

                    # Imprimir dataframe
                    # print(df)
                    
                    dataframes.append(df)


            # Concatenar todos los dataFrames en uno solo
            concatenar_dataframes = pd.concat(dataframes, ignore_index=True)

            # Reiniciar los indices
            concatenar_dataframes.reset_index(inplace = True)

            # Eliminar columna index que se crea despues de reiniciar los indices
            concatenar_dataframes.drop('index', axis = 1, inplace = True)

            # Filtrar columnas especificas para cargar
            concatenar_dataframes = concatenar_dataframes.loc[:, ['CODIGO_DANE', 'ID_TIPO_DELITO', 'ARMA_EMPLEADA', 'FECHA_HECHO', 'SEXO', 'AGRUPA_EDAD_PERSONA']]

            # Imprimir dataFrame consolidado
            # print(concatenar_dataframes)

            # Transformar datframe
            df_final = transformar(concatenar_dataframes)
            
            # print(df_final)

            # Descargar datframe
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

    for index, row in df.iterrows():
        if len(str(row['CODIGO_DANE'])) == 8:
            df.at[index, 'CODIGO_DANE'] = str(row['CODIGO_DANE'])[:5]

        else:
            df.at[index, 'CODIGO_DANE'] = str(row['CODIGO_DANE'])[:4]


    df['AGRUPA_EDAD_PERSONA'] = [value if value in ['adulto', 'sin dato'] else 'menor de edad' for value in df['AGRUPA_EDAD_PERSONA']]

    # retorna dataframe
    return df


# Inicializar script
if __name__ == '__main__':

    # Tecnica de web scraping
    descargar_bases()

    # Consolidar dataframes
    consolidar_data('Amenazas')
    consolidar_data('Delitos_sexuales')
    consolidar_data('Homicidios')
    consolidar_data('Hurto_a_personas')
    consolidar_data('Extorsión')
    consolidar_data('Lesiones_personales')
    consolidar_data('Secuestro')


