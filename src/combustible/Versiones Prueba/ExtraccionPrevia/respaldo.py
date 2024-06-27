# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 15:50:27 2020

@author: hugo.gallardo
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 15:13:36 2020
@author: hugo.gallardo
"""
# Importar package
import os
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None #Remover warnigs de copia encadenada
import requests as r
import sqlalchemy as sa
from sqlalchemy.sql import text
from sqlalchemy.types import NVARCHAR
import urllib
import json
from datetime import date
from datetime import timedelta  


# =============================================================================
# #Conexion a DW
# =============================================================================
#Fijarse que la base de datos es Estudios
param = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=SQLSHP\datawarehouse;DATABASE=Estudios;UID=datawarehouse;PWD=datawarehouse")
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % param)

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
# hoy = hoy + timedelta(days=1)  

estaciones_df['fecha_subida']= hoy

#Transformar diccionarios en DF y adjuntarlos como valor a cada uno de los ítems 
##del DF original
#Agregar columnas de precios por combustible a df original
estaciones_df['precio93'] = np.nan
estaciones_df['precio95'] = np.nan
estaciones_df['precio97'] = np.nan
estaciones_df['precio_diesel'] = np.nan

#Agregar columna con nombre proveedor
estaciones_df['proveedor'] = np.nan
estaciones_df['latitud'] = np.nan
estaciones_df['longitud'] = np.nan
estaciones_df['direccion_calle'] = np.nan
#Num filas
num_estaciones= len(estaciones)

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
    estaciones_df['direccion_calle'][i] = estaciones_df['direccion_calle'][i]
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

#Para testing 
#estaciones_df_antigua = estaciones_df
# estaciones_df = estaciones_df_antigua
#Extraer outliers
#1 - Estaciones sin actualizar en 1 mes (Veriricar)

# =============================================================================
# Creación tablas diarias precio bencina
# =============================================================================
#Agregar fecha de inicio y término de precio como el mismo para primera carga
#subset
c93_df = estaciones_df[["id","fecha_hora_actualizacion","fecha_subida", "precio93"]]
#renombrar columna precio
c93_df = c93_df.rename(columns = {"precio93":"precio" , "fecha_hora_actualizacion":"fecha_inicio",
                                  "fecha_subida": "fecha_fin"}) 
#Remover na
c93_df.dropna(subset = ["precio"], inplace=True)
#generar columnas de inicio, fin y tipo bencina
c93_df["tipo"] = "Bencina93"

c95_df = estaciones_df[["id","fecha_hora_actualizacion","fecha_subida", "precio95"]]
#renombrar columna precio
c95_df = c95_df.rename(columns = {"precio95":"precio" , "fecha_hora_actualizacion":"fecha_inicio",
                                  "fecha_subida": "fecha_fin"}) 
#Remover na
c95_df.dropna(subset = ["precio"], inplace=True)
#generar columnas de inicio, fin y tipo bencina
c95_df["tipo"] = "Bencina95"

c97_df = estaciones_df[["id","fecha_hora_actualizacion","fecha_subida", "precio97"]]
#renombrar columna precio
c97_df = c97_df.rename(columns = {"precio97":"precio" , "fecha_hora_actualizacion":"fecha_inicio",
                                  "fecha_subida": "fecha_fin"}) 
#Remover na
c97_df.dropna(subset = ["precio"], inplace=True)
#generar columnas de inicio, fin y tipo bencina
c97_df["Tipo"] = "Bencina97"

c_diesel_df = estaciones_df[["id","fecha_hora_actualizacion", "fecha_subida","precio_diesel"]]
#renombrar columna precio
c_diesel_df = c_diesel_df.rename(columns = {"precio_diesel":"precio" , "fecha_hora_actualizacion":"fecha_inicio",
                                  "fecha_subida": "fecha_fin"}) 
#Remover na
c_diesel_df.dropna(subset = ["precio"], inplace=True)
#generar columnas de inicio, fin y tipo bencina
c_diesel_df["tipo"] = "Diesel"

# =============================================================================
# Creación de tablas históricas. Ejecutar solo en caso de crear de cero
# =============================================================================

#Agregar datos de dataframe a tabla en el DW: Estudios.dbo.Combustibles
# c93_df.to_sql("Bencina93Hist", con=engine, if_exists= 'append', index=False,
#               dtype = { 'id' : sa.types.NVARCHAR(length=50),
#                        'precio' : sa.types.Float(),
#                        'tipo' : sa.types.NVARCHAR(length=50),
#                        'fecha_inicio' : sa.types.Date(),
#                        'fecha_fin' : sa.types.Date()
#                   })

# c95_df.to_sql("Bencina95Hist", con=engine, if_exists= 'append', index=False,
#                dtype = { 'id' : sa.types.NVARCHAR(length=50),
#                        'precio' : sa.types.Float(),
#                        'tipo' : sa.types.NVARCHAR(length=50),
#                        'fecha_inicio' : sa.types.Date(),
#                        'fecha_fin' : sa.types.Date()
#                   })

# c97_df.to_sql("Bencina97Hist", con=engine, if_exists= 'append', index=False,
#               dtype = { 'id' : sa.types.NVARCHAR(length=50),
#                        'precio' : sa.types.Float(),
#                        'tipo' : sa.types.NVARCHAR(length=50),
#                        'fecha_inicio' : sa.types.Date(),
#                        'fecha_fin' : sa.types.Date()
#                   })

# c_diesel_df.to_sql("DieselHist", con=engine, if_exists= 'append', index=False,
#                    dtype = { 'id' : sa.types.NVARCHAR(length=50),
#                        'precio' : sa.types.Float(),
#                        'tipo' : sa.types.NVARCHAR(length=50),
#                        'fecha_inicio' : sa.types.Date(),
#                        'fecha_fin' : sa.types.Date()
#                   })

# =============================================================================
# Proceso BENCINA 93
# =============================================================================

#Remover tablas auxiliares
conn = engine.connect()
trans = conn.begin()
conn.execute('''
    drop table Estudios.dbo.Bencina93Anterior;
    drop table Estudios.dbo.Bencina93Aux; 
                        ''')
trans.commit()
conn.close()

#Replicar historico en tabla auxiliar en estudios
#Bencina93Aux = tabla A 
#Estudios.dbo.Bencina93Anterior = Tabla B
#Almacenar precios del dia en tabla auxiliar en Estudios (Tabla a)
c93_df.to_sql("Bencina93Aux", con=engine, if_exists= 'append', index=False,
              dtype = { 'id' : sa.types.NVARCHAR(length=50),
                       'precio' : sa.types.Float(),
                       'tipo' : sa.types.NVARCHAR(length=50),
                       'fecha_inicio' : sa.types.Date(),
                       'fecha_fin' : sa.types.Date()
                  })

#Almancenar en tabla auxiliar precios del último día actualizado (Tabla b)
conn = engine.connect()
trans = conn.begin()
conn.execute('''
    declare @fechaactualizacion as date
    set @fechaactualizacion= (select top 1 fecha_fin
    from [Estudios].[dbo].Bencina93Hist
    order by fecha_fin desc); 
    
    --Obtiene maestra de productos de último día de registro
    select *
    into Estudios.dbo.Bencina93Anterior
    from [Estudios].[dbo].Bencina93Hist
    where fecha_fin=@fechaactualizacion;
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°1: Estaciones nuevos se insertan
##Se inserta dentro de la tabla histórica, todos los elementos de la tabla de 
##elementos nuevos que no se encuentren en el histórico.

conn = engine.connect()
trans = conn.begin()
conn.execute('''
--CASO N°1: Estaciones nuevas se insertan
insert into [Estudios].[dbo].Bencina93Hist (id, fecha_inicio, fecha_fin, precio,tipo)
select a.*
from [Estudios].[dbo].Bencina93Aux as a
	left join [Estudios].[dbo].Bencina93Anterior as b
	on a.id = b.id
where b.id is null
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°2: estación existente precio varía -> en este caso hay que hacer un insert
conn = engine.connect()
trans = conn.begin()
conn.execute('''
insert into [Estudios].[dbo].Bencina93Hist (id, fecha_inicio, fecha_fin, precio,tipo)
select a.*
from [Estudios].[dbo].Bencina93Aux as a
inner join [Estudios].[dbo].Bencina93Anterior as b
	on a.id = b.id
where a.precio != b.precio
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°3: estación existente precio no varía - > en este caso hay que hacer un update
conn = engine.connect()
trans = conn.begin()
conn.execute('''
update [Estudios].[dbo].Bencina93Hist
set fecha_fin = (select top 1 fecha_fin from [Estudios].[dbo].Bencina93Aux )
where fecha_fin= (select top 1 fecha_fin from [Estudios].[dbo].Bencina93Anterior)
and id in 
(select a.id
from [Estudios].[dbo].Bencina93Aux as a
inner join [Estudios].[dbo].Bencina93Anterior as b
	on a.id = b.id
where a.precio = b.precio )
                        ''')
trans.commit()
# Close connection
conn.close()

# =============================================================================
# Proceso Bencina95
# =============================================================================

#Remover tablas auxiliares
conn = engine.connect()
trans = conn.begin()
conn.execute('''
    drop table Estudios.dbo.Bencina95Anterior;
    drop table Estudios.dbo.Bencina95Aux;
                        ''')
trans.commit()
conn.close()

#Replicar historico en tabla auxiliar en estudios
#Bencina93Aux = tabla A 
#Estudios.dbo.Bencina93Anterior = Tabla B
#Almacenar precios del dia en tabla auxiliar en Estudios (Tabla a)
c95_df.to_sql("Bencina95Aux", con=engine, if_exists= 'append', index=False,
              dtype = { 'id' : sa.types.NVARCHAR(length=50),
                       'precio' : sa.types.Float(),
                       'tipo' : sa.types.NVARCHAR(length=50),
                       'fecha_inicio' : sa.types.Date(),
                       'fecha_fin' : sa.types.Date()
                  })

#Almancenar en tabla auxiliar precios del último día actualizado (Tabla b)
conn = engine.connect()
trans = conn.begin()
conn.execute('''
    declare @fechaactualizacion as date
    set @fechaactualizacion= (select top 1 fecha_fin
    from [Estudios].[dbo].Bencina95Hist
    order by fecha_fin desc); 
    
    --Obtiene maestra de productos de último día de registro
    select *
    into Estudios.dbo.Bencina95Anterior
    from [Estudios].[dbo].Bencina95Hist
    where fecha_fin=@fechaactualizacion;
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°1: Estaciones nuevos se insertan
##Se inserta dentro de la tabla histórica, todos los elementos de la tabla de 
##elementos nuevos que no se encuentren en el histórico.

conn = engine.connect()
trans = conn.begin()
conn.execute('''
--CASO N°1: Estaciones nuevas se insertan
insert into [Estudios].[dbo].Bencina95Hist (id, fecha_inicio, fecha_fin, precio,tipo)
select a.*
from [Estudios].[dbo].Bencina95Aux as a
	left join [Estudios].[dbo].Bencina95Anterior as b
	on a.id = b.id
where b.id is null
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°2: estación existente precio varía -> en este caso hay que hacer un insert
conn = engine.connect()
trans = conn.begin()
conn.execute('''
insert into [Estudios].[dbo].Bencina95Hist (id, fecha_inicio, fecha_fin, precio,tipo)
select a.*
from [Estudios].[dbo].Bencina95Aux as a
inner join [Estudios].[dbo].Bencina95Anterior as b
	on a.id = b.id
where a.precio != b.precio
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°3: estación existente precio no varía - > en este caso hay que hacer un update
conn = engine.connect()
trans = conn.begin()
conn.execute('''
update [Estudios].[dbo].Bencina95Hist
set fecha_fin = (select top 1 fecha_fin from [Estudios].[dbo].Bencina95Aux )
where fecha_fin= (select top 1 fecha_fin from [Estudios].[dbo].Bencina95Anterior)
and id in 
(select a.id
from [Estudios].[dbo].Bencina95Aux as a
inner join [Estudios].[dbo].Bencina95Anterior as b
	on a.id = b.id
where a.precio = b.precio )
                        ''')
trans.commit()
# Close connection
conn.close()

# =============================================================================
# Proceso Bencina97
# =============================================================================
#Remover tablas auxiliares
conn = engine.connect()
trans = conn.begin()
conn.execute('''
    drop table Estudios.dbo.Bencina97Anterior;
    drop table Estudios.dbo.Bencina97Aux;
                        ''')
trans.commit()
conn.close()

#Replicar historico en tabla auxiliar en estudios
#Bencina93Aux = tabla A 
#Estudios.dbo.Bencina93Anterior = Tabla B
#Almacenar precios del dia en tabla auxiliar en Estudios (Tabla a)
c97_df.to_sql("Bencina97Aux", con=engine, if_exists= 'append', index=False,
              dtype = { 'id' : sa.types.NVARCHAR(length=50),
                       'precio' : sa.types.Float(),
                       'tipo' : sa.types.NVARCHAR(length=50),
                       'fecha_inicio' : sa.types.Date(),
                       'fecha_fin' : sa.types.Date()
                  })

#Almancenar en tabla auxiliar precios del último día actualizado (Tabla b)
conn = engine.connect()
trans = conn.begin()
conn.execute('''
    declare @fechaactualizacion as date
    set @fechaactualizacion= (select top 1 fecha_fin
    from [Estudios].[dbo].Bencina97Hist
    order by fecha_fin desc); 
    
    --Obtiene maestra de productos de último día de registro
    select *
    into Estudios.dbo.Bencina97Anterior
    from [Estudios].[dbo].Bencina97Hist
    where fecha_fin=@fechaactualizacion;
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°1: Estaciones nuevos se insertan
##Se inserta dentro de la tabla histórica, todos los elementos de la tabla de 
##elementos nuevos que no se encuentren en el histórico.
conn = engine.connect()
trans = conn.begin()
conn.execute('''
--CASO N°1: Estaciones nuevas se insertan
insert into [Estudios].[dbo].Bencina97Hist (id, fecha_inicio, fecha_fin, precio,tipo)
select a.*
from [Estudios].[dbo].Bencina97Aux as a
	left join [Estudios].[dbo].Bencina97Anterior as b
	on a.id = b.id
where b.id is null
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°2: estación existente precio varía -> en este caso hay que hacer un insert
conn = engine.connect()
trans = conn.begin()
conn.execute('''
insert into [Estudios].[dbo].Bencina97Hist (id, fecha_inicio, fecha_fin, precio,tipo)
select a.*
from [Estudios].[dbo].Bencina97Aux as a
inner join [Estudios].[dbo].Bencina97Anterior as b
	on a.id = b.id
where a.precio != b.precio
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°3: estación existente precio no varía - > en este caso hay que hacer un update
conn = engine.connect()
trans = conn.begin()
conn.execute('''
update [Estudios].[dbo].Bencina97Hist
set fecha_fin = (select top 1 fecha_fin from [Estudios].[dbo].Bencina97Aux )
where fecha_fin= (select top 1 fecha_fin from [Estudios].[dbo].Bencina97Anterior)
and id in 
(select a.id
from [Estudios].[dbo].Bencina97Aux as a
inner join [Estudios].[dbo].Bencina97Anterior as b
	on a.id = b.id
where a.precio = b.precio )
                        ''')
trans.commit()
# Close connection
conn.close()

# =============================================================================
# Proceso Diesel
# =============================================================================

#Remover tablas auxiliares
conn = engine.connect()
trans = conn.begin()
conn.execute('''
    drop table Estudios.dbo.DieselAnterior;
    drop table Estudios.dbo.DieselAux;
                        ''')
trans.commit()
conn.close()

#Replicar historico en tabla auxiliar en estudios
#Bencina93Aux = tabla A 
#Estudios.dbo.Bencina93Anterior = Tabla B
#Almacenar precios del dia en tabla auxiliar en Estudios (Tabla a)
c_diesel_df.to_sql("DieselAux", con=engine, if_exists= 'append', index=False,
              dtype = { 'id' : sa.types.NVARCHAR(length=50),
                       'precio' : sa.types.Float(),
                       'tipo' : sa.types.NVARCHAR(length=50),
                       'fecha_inicio' : sa.types.Date(),
                       'fecha_fin' : sa.types.Date()
                  })

#Almancenar en tabla auxiliar precios del último día actualizado (Tabla b)
conn = engine.connect()
trans = conn.begin()
conn.execute('''
    declare @fechaactualizacion as date
    set @fechaactualizacion= (select top 1 fecha_fin
    from [Estudios].[dbo].DieselHist
    order by fecha_fin desc); 
    
    --Obtiene maestra de productos de último día de registro
    select *
    into Estudios.dbo.DieselAnterior
    from [Estudios].[dbo].DieselHist
    where fecha_fin=@fechaactualizacion;
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°1: Estaciones nuevos se insertan
##Se inserta dentro de la tabla histórica, todos los elementos de la tabla de 
##elementos nuevos que no se encuentren en el histórico.
conn = engine.connect()
trans = conn.begin()
conn.execute('''
--CASO N°1: Estaciones nuevas se insertan
insert into [Estudios].[dbo].DieselHist (id, fecha_inicio, fecha_fin, precio,tipo)
select a.*
from [Estudios].[dbo].DieselAux as a
	left join [Estudios].[dbo].DieselAnterior as b
	on a.id = b.id
where b.id is null
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°2: estación existente precio varía -> en este caso hay que hacer un insert
conn = engine.connect()
trans = conn.begin()
conn.execute('''
insert into [Estudios].[dbo].DieselHist (id, fecha_inicio, fecha_fin, precio,tipo)
select a.*
from [Estudios].[dbo].DieselAux as a
inner join [Estudios].[dbo].DieselAnterior as b
	on a.id = b.id
where a.precio != b.precio
                        ''')
trans.commit()
# Close connection
conn.close()

# CASO N°3: estación existente precio no varía - > en este caso hay que hacer un update
conn = engine.connect()
trans = conn.begin()
conn.execute('''
update [Estudios].[dbo].DieselHist
set fecha_fin = (select top 1 fecha_fin from [Estudios].[dbo].DieselAux )
where fecha_fin= (select top 1 fecha_fin from [Estudios].[dbo].DieselAnterior)
and id in 
(select a.id
from [Estudios].[dbo].DieselAux as a
inner join [Estudios].[dbo].DieselAnterior as b
	on a.id = b.id
where a.precio = b.precio )
                        ''')
trans.commit()
# Close connection
conn.close()

# =============================================================================
# Fin
# =============================================================================
