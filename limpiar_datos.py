# Librerias
import os
import pandas as pd
import numpy as np

""" Metodo para limpiar dataset"""
def limpiar():
    
    # Leer archivo csv
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
        elif col in ['EDAD', 'COMUNA', 'COD_BARRIO', 'CANTIDAD']:
            df[col] = df[col].fillna('').astype(str)
            df[col] = df[col].apply(lambda x: x.replace('No aplica', '').replace('Sin dato', ''))

        # columnas fechas
        elif col in ['FECHA_HECHO']:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
            df[col] = df[col].dt.strftime('%d/%m/%Y')

        # columnas horas
        elif col in ['HORA_HECHO']:
            df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce')
            df[col] = df[col].dt.strftime('%H:%M:%S')

    # Imprimir dataframe
    print(df)

    # Exportar excel limpio
    df.to_excel(r'Bases procesadas\Conflictividad_cali.xlsx', sheet_name='Base', index=False)

# Inicializar script
if __name__ == '__main__':
    limpiar()