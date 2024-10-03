# Script para transformar y limpiar dataset de conflictividad encontrado 
# para la ciudad de cali en las fuentes de datos abiertos de colombia
# finalmente el dataset se carga en la tabla de hechos delitos en AWS



# Librerias
import os
import pandas as pd
import psycopg2
from datetime import datetime


''' Metodo para transformar y limpiar dataset obtenido desde los datos abiertos de colombia'''
def limpiar_dataframe_cali():

    tipo_conflictividad = {
        'CONFLICTIVIDAD': ['amenaza', 'delito sexual', 'homicidio', 'hurto persona', 'lesiones personales'],
        'ID_TIPO_DELITO': ['1', '2', '3', '4', '5']
    }

    # Crear dataframe con diccionario tipo delito
    df_tipo = pd.DataFrame(tipo_conflictividad)
    
    # dataframe desde archivo csv
    df = pd.read_csv(os.path.dirname(os.path.realpath(__file__)) + r'\Bases sin procesar\registro-de-conflictividades-cali-2010-al-2019.csv', encoding='latin-1', sep=';')

    # Vision general
    print(df.head(10))
    print(df.info())

    # Transformar dataset
    for col in df.columns:

        # Eliminar columnas
        if col in ['VIGENCIA', 'MES', 'DIA_NUMERO', 'DIA']:

            del df[col]

        # Columnas de texto
        elif col in ['CONFLICTIVIDAD', 'TIPO_SITIO', 'ARMA_EMPLEADA', 'MOVIL_AGRESOR', 'MOVIL_AFECTADO', 'SEXO', 'ESTADO_CIVIL', 'PAIS_NACIMIENTO', 'CLASE_EMPLEADO', 'PROFESION', 'ESCOLARIDAD', 'ZONA', 'BARRIO', 'CGTO_NOMBRE']:

            df[col] = df[col].astype(str).apply(lambda x: x.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u').lower())

        # Columnas numericas
        elif col in ['COMUNA', 'COD_BARRIO', 'CANTIDAD']:
            df[col] = df[col].fillna('').astype(str)
            df[col] = df[col].apply(lambda x: x.replace('No aplica', '').replace('Sin dato', ''))

        # Columna edad
        elif col in ['EDAD']:
            df[col] = df[col].astype(str)
            df[col] = df[col].apply(lambda x: x.replace('Sin dato', ''))


        # columnas fechas
        elif col in ['FECHA_HECHO']:
            df[col] = pd.to_datetime(df[col], dayfirst=True)
            df[col] = df[col].dt.strftime('%Y-%m-%d')

        # columnas horas
        elif col in ['HORA_HECHO']:
            df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce')
            df[col] = df[col].dt.strftime('%H:%M:%S')

    # Crear columnas de clasificacion
    df['CODIGO_DANE'] = '76001'

    df['AGRUPA_EDAD_PERSONA'] = ['menor de edad' if value < '18' else 'adulto' for value in df['EDAD']]

    # Asignar id tipo delito
    df = pd.merge(df, df_tipo, on = 'CONFLICTIVIDAD')

    # Filtrar columnas especificas para cargar
    df_final = df.loc[:, ['CODIGO_DANE', 'ID_TIPO_DELITO', 'ARMA_EMPLEADA', 'FECHA_HECHO', 'SEXO', 'AGRUPA_EDAD_PERSONA']]

    # Imprimir dataframe
    # print(df_final)

    # Exportar excel limpio
    # Se crea un archivo completo donde mostramos la transformacion general del archivo
    # y se crea otro archivo filtrado por columnas, el cual utilizaremos para la carga
    df.to_excel(os.path.dirname(os.path.realpath(__file__)) + r'\Bases procesadas\Conflictividad_cali.xlsx', sheet_name='Base', index=False)
    df_final.to_excel(os.path.dirname(os.path.realpath(__file__)) + r'\Bases procesadas\Conflictividad_cali_filtrado.xlsx', sheet_name='Base', index=False)
    

    # Cargar dataframe a base de datos postgres
    cargar_dataframe(df_final, 1000)



''' Funcion para cargar dataframe a base de datos AWS'''
def cargar_dataframe(df_data, batch_size):

    inicio_cargue = datetime.now()

    print('\n - Inicio cargue dataframe:', inicio_cargue.strftime('%d/%m/%Y %I:%M:%S %p'), '\n')

    df_ = pd.DataFrame(df_data)

    # Abrir conexion con base de datos
    conn = conexion_post()

    try:

        # Insertar data en la base de datos de manera incremental de 1000 en 1000
        with conn.cursor() as cur:

            # Query sql
            insert_sql = """INSERT INTO H_DELITOS (ID_DANE_MUNICIPIO, ID_TIPO_DELITO, ARMAS_MEDIOS, FECHA_HECHO, GENERO, AGRUPA_EDAD_PERSONA) VALUES (%s, %s, %s, %s, %s, %s);"""

            # Recorrido en batches de 100
            for i in range(0, len(df_), batch_size):

                batch = df_.iloc[i:i + batch_size]

                # Imprimir grupos de 1000
                print(batch)

                # Generar lista de tuplas del grupo de 1000
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
    
    limpiar_dataframe_cali()