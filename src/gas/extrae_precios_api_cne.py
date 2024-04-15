"""
Script que extrae los precios diarios de gas licuado
desde la API de la comision nacional de energia (CNE).

@autor = javier guajardo
"""

import requests
import json
import pandas as pd
import os
from datetime import date, datetime, timedelta

first_day_month = date.today().replace(day=1)
current_day = date.today()
period = print(first_day_month, " | ", current_day)


# directorio
os.chdir('/Users/javier.guajardo/Documents/GitHub/ahorro_magento/api cne/')

# token
token_cne = 'R5oIeVXwSG'

# API de la CNE
url_api_1 = 'http://api.cne.cl/v3/combustibles/calefaccion/callcenters?token='+token_cne+''    # callcenter
url_api_2 = 'http://api.cne.cl/v3/combustibles/calefaccion/puntosdeventa?token=' + \
    token_cne+''  # puntos de venta presenciales

# &callback=callback
print('\n')
print("Iniciamos el proceso de extraccion de precios de gas licuado......:")

try:
    # response_cne = )
    response_cne = requests.get(url_api_1)
    response_cne.raise_for_status()
    # Additional code will only run if the request is successful
except requests.exceptions.HTTPError as server_error:
    print(server_errors)
except requests.exceptions.ConnectionError as conn_error:
    print(conn_error)
except requests.exceptions.ConnectTimeout as timeout_error:
    print(timeout_error)
except requests.exceptions.RequestException as rqst_error:
    print(rqst_error)

json_data = response_cne.json()

# transforming json to dataframe
df_gas = pd.json_normalize(json_data,
                           record_path=['data', 'callcenters'],
                           meta=[
                               ['data', 'id_empresa'],
                               ['data', 'nombre_empresa'],
                               ['data', 'tipo_empresa'],
                               ['data', 'marca']])

# sorting columns
df_gas = df_gas.rename(columns={'data.id_empresa': 'id_empresa',
                                'data.nombre_empresa': 'nombre_empresa',
                                'data.tipo_empresa': 'tipo_empresa',
                                'data.marca': 'marca'})

df_gas = df_gas[[
                'id_empresa',
                'nombre_empresa',
                'marca',
                'tipo_gas',
                'tamano',
                'medida',
                'precio',
                'id_comuna',
                'nombre_comuna',
                'id_region',
                'nombre_region']]

# creating dates: extraction and last update
fecha_actualizacion = datetime.today()
df_gas['fecha_consulta'] = fecha_actualizacion
df_gas['fecha_actualizacion'] = df_gas['fecha_consulta'] - timedelta(days=1)

# writing a json file
file_name = 'precios_gas_'+date.strftime(fecha_actualizacion-timedelta(days=1), format='%Y%m%d')

with open(file_name+'.json', 'w') as file_objects:
    json.dump(json_data, file_objects)

# writing a csv file
df_gas.to_csv(file_name+'.csv', index=False, sep=';', header=True)
