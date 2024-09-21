# Librerias
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
import time
import re

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
                    
                    nombre_excel = str(row['Nombre_delito']).replace(' ', '_') + '_' + str(row['Anio_delito']) + f'.{str(row['Enlace_archivo']).split('.')[-1]}'
                    ruta_archivo = os.path.join(output_dir, nombre_excel)

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


''' Funcion para consolidar bases en un solo archivo '''
def consolidar_data():
    
    ruta = os.path.dirname(os.path.realpath(__file__)) + r'\Bases sin procesar\Bases web scraping'

    # Lista para almacenar los dataFrames de cada archivo
    dataframes = []
    patron = re.compile('^Secuestro')
    patron_titulo = re.compile('^MINISTERIO')
    
    # Validar si la ruta donde se almacenan los archivos existe
    if os.path.exists(ruta):

        # Listar los archivos .xlsx
        with os.scandir(ruta) as archivos:

            for archivo in archivos:
                
                if archivo.is_file() and archivo.name.endswith('.xlsx') and patron.search(archivo.name):

                    nombre = archivo.name
                    print( f'- Archivo: {nombre} \n')

                    # Leer cada archivo de excel y añadirlo a un dataframe
                    ruta_archivo = os.path.join(ruta, nombre)
                    df = pd.read_excel(ruta_archivo)

                    # print(df)

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
                    
                    # Imprimir dataframe con nuevos encabezados
                    print(df)
                    
                    # dataframes.append(df)


            # # Concatenar todos los dataFrames en uno solo
            # final_dataframe = pd.concat(dataframes, ignore_index=True)

            # # Imprimir dataFrame consolidado
            # print(final_dataframe)

    else:
        print('Ruta no existe!')


    
    print(' ************************** Proceso renombrar finalizado **************************')


if __name__ == '__main__':
    descargar_bases()
    # consolidar_data()

