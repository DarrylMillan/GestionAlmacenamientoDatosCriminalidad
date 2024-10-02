import psycopg2
import pandas as pd

''' Funcion para cargar dataframe municipios a la base de datos '''
def cargar_dataframe():
    
    # Diccionario tipo delito
    tipo_conflictividad = {
            'CONFLICTIVIDAD': ['amenaza', 'delito sexual', 'homicidio', 'hurto persona', 'lesiones personales', 'extorsion', 'secuestro', 'terrorismo'],
            'ID_TIPO_DELITO': ['1', '2', '3', '4', '5', '6', '7', '8']
    }

    # datframe tipo delito
    df = pd.DataFrame(tipo_conflictividad)

    conn = conexion_post()

    # Crear un cursor
    with conn.cursor() as cur:

        try:

            if df.empty == False:
            
                # Query para insertar a base de datos
                insert_query = """INSERT INTO DIM_TIPO_DELITOS (ID_TIPO_DELITO, TIPO_DELITO) VALUES (%s, %s);"""

                for index, row in df.iterrows():

                    filas = row['ID_TIPO_DELITO'], row['CONFLICTIVIDAD']

                    # Ejecutar SQL con los datos
                    cur.execute(insert_query, filas)

                # Confirmar la transacción
                conn.commit()

                print(f"Registro insertado exitosamente en la tabla.")
            
            else:
                print(' - Dataframe vacio!')

        except Exception as error:

            print(f"Error al insertar en la base de datos: {error}")

        finally:

            # Cerrar conexion 
            if conn:
                cur.close()
                conn.close()




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
        print(f"Error al insertar en la base de datos: {error}")


cargar_dataframe()