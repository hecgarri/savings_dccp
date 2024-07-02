"""
Rutina: ActualizacionDiaria.py

Descripción:
El objetivo de esta rutina es conectarse a la API de combustibles de la Comisión Nacional de Energía
(CNE) para luego dar formato a los datos y cargarlos en la base de datos Estudios del DataWareHouse.

@author:hector.garrido
Correo electrónico: hector.garrido@chilecompra.cl
Fecha de creación: Miércoles 4 de noviembre de 2020
Fecha de actualización:         Miércoles 8 de mayo de 2024

Funciones:
- get_auth_token: genera el token mediante el método HTTP POST 
- make_authenticated_request: Obtiene los datos mediante el método HTTP GET
- transform_json_to_dataframe: toma los datos desde la API (json) y los formatea para su carga en el DataWareHouse
- cargar_y_modificar_aux: carga y modifica tabla auxiliar de carga de datos
- elimina_duplicados: Elimina duplicados antes de realizar la carga en la tabla histórica
- insert_new_eds: Inserta datos de estaciones de servicio nuevas 
- update_hist: Actualiza tabla históricas con nuevos datos depurados 

"""

import os  # Módulo para interactuar con el sistema operativo, proporciona funciones para trabajar con archivos, directorios, variables de entorno, etc.
import numpy as np  # Biblioteca para computación numérica en Python, proporciona estructuras de datos y funciones para trabajar con matrices y vectores de manera eficiente.
import pandas as pd  # Biblioteca para análisis y manipulación de datos en Python, proporciona la estructura de datos DataFrame para trabajar con conjuntos de datos tabulares.
pd.options.mode.chained_assignment = None  # Configuración para suprimir los warnings de "copia encadenada" en pandas, para evitar posibles advertencias que no son relevantes en nuestro código.
import requests as r  # Biblioteca para enviar solicitudes HTTP en Python, facilita la realización de solicitudes a servidores web y el manejo de respuestas.
import sqlalchemy as sa  # Biblioteca para interactuar con bases de datos relacionales en Python, proporciona herramientas para conectarse a bases de datos, enviar consultas SQL, etc.
from sqlalchemy.sql import text  # Clase para construir expresiones SQL en SQLAlchemy, útil para generar consultas SQL de manera programática.
from sqlalchemy.types import NVARCHAR  # Tipo de datos específico de SQLAlchemy, usado para definir columnas de tipo NVARCHAR en bases de datos SQL Server.
import urllib  # Módulo para trabajar con URL en Python, proporciona funciones para manipular URLs, codificar y decodificar componentes de URL, etc.
import json  # Módulo para trabajar con datos JSON en Python, proporciona funciones para codificar y decodificar datos JSON.
from datetime import date  # Clase para representar fechas en Python, útil para manipular y trabajar con fechas en nuestras aplicaciones.
from datetime import timedelta  # Clase para representar duraciones de tiempo en Python, útil para calcular y manipular intervalos de tiempo.
import urllib                        #Para formatear string de conexión


param_DW = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.34.71.202;UID=datawarehouse;PWD=datawarehouse;DATABASE=Estudios;TrustServerCertificate=yes")
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % param_DW)

url = "https://api.cne.cl/api/login"
email = "hectorgarridohenriquez@gmail.com"
password = "r2ikSGpgwetgjAk"

def get_auth_token(email, password):
    url = "https://api.cne.cl/api/login"
    response = r.post(
        url,
        json={"email": email, "password": password}
    )
    return response.json()["token"]

token = get_auth_token(email, password)

# Función para hacer una solicitud autenticada utilizando el token
def make_authenticated_request(token, endpoint):
    url = "https://api.cne.cl" + endpoint
    response = r.get(
        url,
        headers={"Authorization": "Bearer " + token}
    )
    return response.json()

resp = make_authenticated_request(token, "/api/v4/estaciones")


def transform_json_to_dataframe(resp):
    # Crear DataFrame a partir del json
    estaciones_df = pd.DataFrame(resp)
    
    # Agregar columna de fecha actual
    hoy = date.today()
    estaciones_df['fecha_subida'] = hoy
    
    # Agregar columnas de precios por combustible y otras columnas
    estaciones_df['precio93'] = np.nan
    estaciones_df['precio95'] = np.nan
    estaciones_df['precio97'] = np.nan
    estaciones_df['precio_diesel'] = np.nan
    estaciones_df['proveedor'] = np.nan
    estaciones_df['latitud'] = np.nan
    estaciones_df['longitud'] = np.nan
    estaciones_df['direccion_calle'] = np.nan

    # Iterar sobre cada fila del DataFrame para asignar los precios y otras columnas
    for i in range(len(estaciones_df)):
        precios = estaciones_df['precios'][i]
        if isinstance(precios, dict):
            if '93' in precios.keys():
                estaciones_df.loc[i, 'precio93'] = precios['93']['precio']
                estaciones_df.loc[i,'fecha_actualizacion'] = precios['93']['fecha_actualizacion']
                estaciones_df.loc[i,'hora_actualizacion'] = precios['93']['hora_actualizacion']
            if '95' in precios.keys():
                estaciones_df.loc[i, 'precio95'] = precios['95']['precio']
                estaciones_df.loc[i,'fecha_actualizacion'] = precios['95']['fecha_actualizacion']
                estaciones_df.loc[i,'hora_actualizacion'] = precios['95']['hora_actualizacion']    
            if '97' in precios.keys():
                estaciones_df.loc[i, 'precio97'] = precios['97']['precio']
                estaciones_df.loc[i,'fecha_actualizacion'] = precios['97']['fecha_actualizacion']
                estaciones_df.loc[i,'hora_actualizacion'] = precios['97']['hora_actualizacion']
            if 'DI' in precios.keys():
                estaciones_df.loc[i, 'precio_diesel'] = precios['DI']['precio']
                estaciones_df.loc[i,'fecha_actualizacion'] = precios['DI']['fecha_actualizacion']
                estaciones_df.loc[i,'hora_actualizacion'] = precios['DI']['hora_actualizacion']
            estaciones_df.loc[i, 'proveedor'] = estaciones_df['distribuidor'][i]['marca']
            estaciones_df.loc[i, 'latitud'] = estaciones_df['ubicacion'][i]['latitud']
            estaciones_df.loc[i, 'longitud'] = estaciones_df['ubicacion'][i]['longitud']
            estaciones_df.loc[i, 'direccion_calle'] = estaciones_df['ubicacion'][i]['direccion']
    
    # Eliminar filas donde todos los precios son NaN
    missing_all_prices = (estaciones_df['precio93'].isna()
                          & estaciones_df['precio95'].isna()
                          & estaciones_df['precio97'].isna()
                          & estaciones_df['precio_diesel'].isna())
    estaciones_df = estaciones_df[~missing_all_prices]
    
    # Renombrar columnas y realizar otras transformaciones
    estaciones_df = estaciones_df.rename(columns={'codigo': 'id'})
    estaciones_df['id'] = estaciones_df['id'].str.strip()
    estaciones_df['id_est_fecha']= estaciones_df['id'] + estaciones_df['fecha_subida'].astype(str)
    estaciones_df['id_est_fecha'] = estaciones_df['id_est_fecha'].str.replace("-","")
    first_col = estaciones_df.pop("id_est_fecha")
    estaciones_df.insert(0, "id_est_fecha", first_col)
    col_aux = estaciones_df.pop("proveedor")
    estaciones_df.insert(2, "proveedor", col_aux)
    estaciones_df = estaciones_df.drop(['precios','razon_social','direccion_calle','horario_atencion',
             'distribuidor', 'metodos_de_pago', 'ubicacion','servicios','punto_electrico' ], axis = 1)
    
    # Crear DataFrames individuales para cada tipo de combustible
    c93_df = estaciones_df[["id","fecha_actualizacion","fecha_subida", "precio93",'hora_actualizacion']]
    c93_df = c93_df.rename(columns = {"precio93":"precio" , "fecha_actualizacion":"fecha_inicio",
                                      "fecha_subida": "fecha_fin"}) 
    c93_df.dropna(subset = ["precio"], inplace=True)
    c93_df['tipo'] = 'Bencina93'

    c95_df = estaciones_df[["id","fecha_actualizacion","fecha_subida", "precio95",'hora_actualizacion']]
    c95_df = c95_df.rename(columns = {"precio95":"precio" , "fecha_actualizacion":"fecha_inicio",
                                      "fecha_subida": "fecha_fin"}) 
    c95_df.dropna(subset = ["precio"], inplace=True)
    c95_df['tipo'] = 'Bencina95'

    c97_df = estaciones_df[["id","fecha_actualizacion","fecha_subida", "precio97",'hora_actualizacion']]
    c97_df = c97_df.rename(columns = {"precio97":"precio" , "fecha_actualizacion":"fecha_inicio",
                                      "fecha_subida": "fecha_fin"}) 
    c97_df.dropna(subset = ["precio"], inplace=True)
    c97_df['tipo'] = 'Bencina97'

    c_diesel_df = estaciones_df[["id","fecha_actualizacion", "fecha_subida","precio_diesel",'hora_actualizacion']]
    c_diesel_df = c_diesel_df.rename(columns = {"precio_diesel":"precio" , "fecha_actualizacion":"fecha_inicio",
                                      "fecha_subida": "fecha_fin"}) 
    c_diesel_df.dropna(subset = ["precio"], inplace=True)
    c_diesel_df['tipo'] = 'Diesel'
    
    # Almacenar los DataFrames en un diccionario
    result = {
        'c93_df': c93_df,
        'c95_df': c95_df,
        'c97_df': c97_df,
        'c_diesel_df': c_diesel_df,
        'eds':estaciones_df
    }
    
    return result
data_frames = transform_json_to_dataframe(resp)


c93_df = data_frames['c93_df']  # Accede al DataFrame de Bencina 93
c95_df = data_frames['c95_df']  # Accede al DataFrame de Bencina 95
c97_df = data_frames['c97_df']  # Accede al DataFrame de Bencina 97
c_diesel_df = data_frames['c_diesel_df']  # Accede al DataFrame de Diesel
def cargar_y_modificar_aux(tabla_aux, datos):
    # Paso 1: Modificar la tabla auxiliar
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Borrar los datos existentes
            conn.execute(text(f'''
            DELETE FROM Estudios.dbo.{tabla_aux}
            '''))

            # Alterar la tabla para añadir la columna 'hora_actualizacion'
            #conn.execute(text(f'''
            #ALTER TABLE {tabla_aux}
            #ADD hora_actualizacion TIME;
            #'''))
            
            trans.commit()
        except:
            trans.rollback()
            raise

    # Paso 2: Cargar los nuevos datos en la tabla auxiliar
    datos.to_sql(tabla_aux, con=engine, if_exists='append', index=False, dtype={
        'id': sa.types.NVARCHAR(length=50),
        'precio': sa.types.Float(),
        'tipo': sa.types.NVARCHAR(length=50),
        'fecha_inicio': sa.types.Date(),
        'fecha_fin': sa.types.Date(),
        'hora_actualizacion': sa.types.Time()
    })

# Ejemplo de uso
cargar_y_modificar_aux("Bencina93Aux", c93_df)
cargar_y_modificar_aux("Bencina95Aux",c95_df)
cargar_y_modificar_aux("Bencina97Aux",c97_df)
cargar_y_modificar_aux("DieselAux",c_diesel_df)
#Elimina observaciones duplicadas en la tabla auxiliar 

def elimina_duplicados(table_aux,table_ant):
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(
                text(
                f'''
                DELETE b
                FROM Estudios.dbo.{table_aux} b
                INNER JOIN Estudios.dbo.{table_ant} a
                ON a.id = b.id 
                AND a.fecha_inicio = b.fecha_inicio 
                AND a.precio = b.precio;
                '''
                )
            )
            trans.commit()
        except Exception as e:
            trans.rollback()
            print(f"Ha ocurrido un error al momento de modificar {table_aux}:{e}")
        finally:
            conn.close()

elimina_duplicados("Bencina93Aux","Bencina93Anterior")
elimina_duplicados("Bencina95Aux","Bencina95Anterior")
elimina_duplicados("Bencina97Aux","Bencina97Anterior")
elimina_duplicados("DieselAux","DieselAnterior")

# CASO N°1: Estaciones nuevas se insertan
##Se inserta dentro de la tabla histórica, todos los elementos de la tabla de 
##elementos nuevos de la tabla auxiliar

def insert_new_eds(table_aux, table_hist, table_ant):
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text(
                f'''
                INSERT INTO [Estudios].[dbo].{table_hist} (id, fecha_inicio, fecha_fin, precio, tipo)
                SELECT a.id, a.fecha_inicio, a.fecha_fin, a.precio, a.tipo
                FROM [Estudios].[dbo].{table_aux} AS a
                LEFT JOIN [Estudios].[dbo].{table_ant} AS b
                ON a.id = b.id
                WHERE b.id IS NULL
                '''
            ))
            trans.commit()
        except Exception as e:
            trans.rollback()
            print(f"Ha ocurrido un error al insertar en la tabla {table_hist}: {e}")
        finally:
            pass


insert_new_eds('Bencina93Aux','Bencina93Hist','Bencina93Anterior')
insert_new_eds('Bencina95Aux','Bencina95Hist','Bencina95Anterior')
insert_new_eds('Bencina97Aux','Bencina97Hist','Bencina97Anterior')
insert_new_eds('DieselAux','DieselHist','DieselAnterior')

# CASO N°2: Se insertan todas las nuevas observaciones de estaciones ya existentes

def update_hist(table_ant, table_hist, table_aux):
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text(
            f'''
            WITH temp AS (
                SELECT 
                    b.id,
                    MAX(fecha_inicio) AS [fecha_inicio]
                FROM estudios.dbo.{table_ant} b
                GROUP BY b.id
            )

            INSERT INTO estudios.dbo.{table_hist} (id, fecha_inicio, fecha_fin, precio, tipo)
            SELECT 
                a.id,
                a.fecha_inicio,
                a.fecha_fin,
                a.precio,
                a.tipo
            FROM estudios.dbo.{table_aux} AS a
            INNER JOIN temp b ON a.id = b.id
            WHERE a.fecha_inicio > b.fecha_inicio
            ORDER BY a.fecha_inicio ASC;

                                    '''
            ))
            trans.commit()
        except Exception as e:
            trans.rollback()
            print(f"Ha ocurrido un error al actualizar la tabla {table_hist}:{e}")
        finally:
            pass

update_hist('Bencina93Anterior','Bencina93Hist','Bencina93Aux')
update_hist('Bencina95Anterior','Bencina95Hist','Bencina95Aux')
update_hist('Bencina97Anterior','Bencina97Hist','Bencina97Aux')
update_hist('DieselAnterior','DieselHist','DieselAux')