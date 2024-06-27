# -*- coding: utf-8 -*-

# Importar packages
#import os
import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None 
import requests as r
import sqlalchemy as sa
import urllib
#import json
from datetime import date
from datetime import timedelta  
#import pyodbc

# =============================================================================
# #Conexion a DW
# =============================================================================

#Fijarse que la base de datos es Estudios
param = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=SQLSHP\datawarehouse;DATABASE=Estudios;UID=datawarehouse;PWD=datawarehouse")
conn = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % param)

# =============================================================================
# Inputs para API
# =============================================================================
# test =os.getenv()
#Ingresar token de api fne
api_token= 'U8dCZgLxLI'
url_base = 'http://api.cne.cl/v3/combustibles/vehicular/estaciones?token='

# Se utiliza una f antes del texto para ingresar variables
url = f"{url_base}{api_token}"
#Extraigo todos los datos de todas las estaciones en una sola consulta
resp = r.get(url).json()

# Revisar campos y numero de campos
resp.keys()

#Transformar json a DF
estaciones = resp['data']
estaciones_df = pd.DataFrame(estaciones)
precios_crudo= pd.DataFrame(estaciones_df['precios'])

#Agregar columna de día actual
hoy = date.today()
#Fecha para testear el append
#hoy = hoy + timedelta(days=3)  

estaciones_df['fecha_subida']= hoy

#Agregar columnas de precios por combustible a df original
estaciones_df['precio93'] = np.nan
estaciones_df['precio95'] = np.nan
estaciones_df['precio97'] = np.nan
estaciones_df['precio_diesel'] = np.nan

#Agregar columna con nombre proveedor
estaciones_df['proveedor'] = np.nan
estaciones_df['latitud'] = np.nan
estaciones_df['longitud'] = np.nan
#Num filas
num_estaciones= len(estaciones)


#Transformar diccionarios en DF y adjuntarlos como valor a cada uno de los ítems 
##del DF original

    
# for i in range(0,(num_estaciones-1)):
for i in range(0,(num_estaciones)):
    if 'gasolina 93' in estaciones_df['precios'][i].keys():
        estaciones_df['precio93'][i] = estaciones_df['precios'][i]['gasolina 93']
    if 'gasolina 95' in estaciones_df['precios'][i].keys():
        estaciones_df['precio95'][i] = estaciones_df['precios'][i]['gasolina 95']    
    if 'gasolina 97' in estaciones_df['precios'][i].keys():
        estaciones_df['precio97'][i] = estaciones_df['precios'][i]['gasolina 97']
    if 'petroleo diesel' in estaciones_df['precios'][i].keys():
        estaciones_df['precio_diesel'][i] = estaciones_df['precios'][i]['petroleo diesel']
    estaciones_df['proveedor'][i] = estaciones_df['distribuidor'][i]['nombre']
    estaciones_df['latitud'][i] = estaciones_df['ubicacion'][i]['latitud']
    estaciones_df['longitud'][i] = estaciones_df['ubicacion'][i]['longitud']

#Eliminar filas sin usar y con solo gas licuado

# del estaciones_df['precios']
estaciones_df = estaciones_df.drop(['precios','razon_social','direccion_calle', 'direccion_numero','horario_atencion',
         'distribuidor', 'metodos_de_pago', 'ubicacion','servicios', ], axis = 1)

estaciones_df = estaciones_df.drop(estaciones_df[(np.isnan(estaciones_df['precio93']) == True) 
                                                    & (np.isnan(estaciones_df['precio95']) == True)
                                                    & (np.isnan(estaciones_df['precio97'])== True)
                                                    & (np.isnan(estaciones_df['precio_diesel'])== True)].index)

#Los IDs de las estaciones son únicos, pero deben ser diferenciados por fecha de carga
#ID único será concatenado id y fecha carga
estaciones_df['id'] = estaciones_df['id'].str.strip()
estaciones_df['id_est_fecha']= estaciones_df['id'] + estaciones_df['fecha_subida'].astype(str) # strftime("%Y%m%d")
estaciones_df['id_est_fecha'] = estaciones_df['id_est_fecha'].str.replace("-","")
#PAsar columna id_est_fecha a primera posición
first_col = estaciones_df.pop("id_est_fecha")
estaciones_df.insert(0, "id_est_fecha", first_col)

#Pasar columna proveedor a la tercera posición
col_aux = estaciones_df.pop("proveedor")
estaciones_df.insert(2, "proveedor", col_aux)


# =============================================================================
# Insertar en Data Warehouse
# =============================================================================

#En primera iteración se crea archivo en csv para crear tabla en SQL

#Obs: en csv no funciona el encoding utf-8 , en excel si
#Obs: funciona solo con UTF-16

#Quitando espacios blancos de nombres de columnas
estaciones_df.columns = estaciones_df.columns.str.strip()

#Respaldo en csv y excel
estaciones_df.to_csv("C:/Users/esteban.olivares/Documents/visualizaciones/Combustibles/respaldo datos/carga"+str(hoy)+".csv",
                      index=False , encoding="UTF-16")


estaciones_df.to_excel("C:/Users/esteban.olivares/Documents/visualizaciones/Combustibles/respaldo datos/carga"+str(hoy)+".xlsx"
                       ,  index=False , encoding="utf-8")


#Agregar datos de dataframe a tabla en el DW: Estudios.dbo.Combustibles
estaciones_df.to_sql("Combustibles", con=conn, if_exists= 'append', index=False)





