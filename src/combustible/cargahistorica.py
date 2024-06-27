import pandas as pd
import numpy as np
import sqlalchemy as sa              #Para conexión a BD, requerido para usar pd.read_sql()
from sqlalchemy import text
import urllib                        #Para formatear string de conexión


param_DW = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.34.71.202;UID=datawarehouse;PWD=datawarehouse;DATABASE=Estudios;TrustServerCertificate=yes")
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % param_DW)

# Crea copias de respaldo de tablas de bencina históricas. 

#with engine.connect() as conn:
#    trans = conn.begin()
#    conn.execute(text('''
#    SELECT *
#    INTO Bencina93Respaldo
#    FROM Bencina93Hist                  
#                      ''')
#    )
#    trans.commit()

#with engine.connect() as conn:
#    trans = conn.begin()
#    conn.execute(text('''
#    SELECT *
#    INTO Bencina95Respaldo
#    FROM Bencina95Hist                  
#                      ''')
#    )
#    trans.commit()

#with engine.connect() as conn:
#    trans = conn.begin()
#    conn.execute(text('''
#    SELECT *
#    INTO Bencina97Respaldo
#    FROM Bencina97Hist                  
#                      ''')
#    )
#    trans.commit()

#with engine.connect() as conn:
#    trans = conn.begin()
#    conn.execute(text('''
#    SELECT *
#    INTO DieselRespaldo
#    FROM DieselHist                  
#                      ''')
#    )
#    trans.commit()

# Carga datos históricos desde energía abierta (csv)
data_2021 = pd.read_csv("C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/combustible/precios por estacion/energia_abierta/2021.csv/2021.csv", sep=";")
data_2022 = pd.read_csv("C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/combustible/precios por estacion/energia_abierta/2022.csv/2022.csv", sep=";")
data_2023 = pd.read_csv("C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/combustible/precios por estacion/energia_abierta/2023.csv/2023.csv", sep=";")

#La base de datos de por sí trae datos duplicados, sin embargo, no registra hora de actualización de los precios.
#debido a esta razón, un criterio conservador es quedarse con el precio más alto registrado para un mismo día. 
#Este problema sólo está presente desde el 2023 hacia atrás, en 2024 incluyeron la hora de actualización 

def get_highest_price(data, fuel_type):
    data = data.rename(columns={"fecha_actualizacion":"fecha_inicio"})
    filtered_data = data[data['combustible'] == fuel_type]
    selected_columns = filtered_data[['fecha_inicio', 'id', 'combustible', 'precio']]
    grouped_data = selected_columns.groupby(['fecha_inicio', 'id'])
    max_price_data = grouped_data.apply(lambda x: x.nlargest(1, 'precio'))
    max_price_data = max_price_data.reset_index(drop = True)
    return max_price_data


data_2021_2023 = [data_2021, data_2022, data_2023]  

# Aplicar la función a cada DataFrame en la lista
bencinas_93 = [get_highest_price(df, 'Gasolina 93') for df in data_2021_2023]

bencinas_95 = [get_highest_price(df, 'Gasolina 95') for df in data_2021_2023]

bencinas_97 = [get_highest_price(df, 'Gasolina 97') for df in data_2021_2023]

petroleo_diesel = [get_highest_price(df,  'Petroleo Diesel') for df in data_2021_2023]


#Verifica los rangos de fecha

#data_2021[data_2021['combustible']=='Gasolina 93'].shape
#data_2021.groupby('combustible').count()

#[df['fecha_inicio'].min() for df in bencinas_93]
#[df['fecha_inicio'].max() for df in bencinas_93]

#[df['fecha_inicio'].min() for df in bencinas_95]
#[df['fecha_inicio'].max() for df in bencinas_95]

#[df['fecha_inicio'].min() for df in bencinas_97]
#[df['fecha_inicio'].max() for df in bencinas_97]

#Verifica que no haya duplicados según fecha y estación de servicio

#[df.duplicated(subset=['fecha_inicio','id']).sum() for df in bencinas_93]
#[df.duplicated(subset=['fecha_inicio','id']).sum() for df in bencinas_95]
#[df.duplicated(subset=['fecha_inicio','id']).sum() for df in bencinas_97]
#[df.duplicated(subset=['fecha_inicio','id']).sum() for df in petroleo_diesel]

#Asigna fecha actual como fecha_fin que indica la actualización de los datos

fecha_actual = pd.Timestamp.now().strftime('%Y-%m-%d')

bencinas_93 = [df.assign(fecha_fin=lambda x: fecha_actual) for df in bencinas_93]
bencinas_95 = [df.assign(fecha_fin=lambda x: fecha_actual) for df in bencinas_95]
bencinas_97 = [df.assign(fecha_fin=lambda x: fecha_actual) for df in bencinas_97]
petroleo_diesel = [df.assign(fecha_fin=lambda x: fecha_actual) for df in petroleo_diesel]

bencinas_93 = [df.drop(columns = ['combustible']) for df in bencinas_93]
bencinas_95 = [df.drop(columns = ['combustible']) for df in bencinas_95]
bencinas_97 = [df.drop(columns = ['combustible']) for df in bencinas_97]
petroleo_diesel = [df.drop(columns = ['combustible']) for df in petroleo_diesel]

bencinas_93 = [df.assign(tipo = lambda x: 'Bencina93') for df in bencinas_93]
bencinas_95 = [df.assign(tipo = lambda x: 'Bencina95') for df in bencinas_95]
bencinas_97 = [df.assign(tipo = lambda x: 'Bencina97') for df in bencinas_97]
petroleo_diesel = [df.assign(tipo = lambda x: 'Diesel') for df in petroleo_diesel]

nuevo_orden = ['id', 'fecha_inicio','fecha_fin', 'precio', 'tipo']

bencinas_93 = [df[nuevo_orden] for df in bencinas_93]
bencinas_95 = [df[nuevo_orden] for df in bencinas_95]
bencinas_97 = [df[nuevo_orden] for df in bencinas_97]
petroleo_diesel = [df[nuevo_orden] for df in petroleo_diesel]


#Borra bases de datos históricas

def delete_historic(table_name):
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(
                text(f'''DELETE 
                FROM Estudios.dbo.{table_name} 
                      ''')
            )
            
            # Confirma la transacción 
            trans.commit()
        except Exception as e:
            trans.rollback()
            print(f"Ha ocurrido un error al momento de actualizar {table_name}:{e}")
        finally:
            conn.close()

historicas = ['Bencina93Hist','Bencina95Hist','Bencina97Hist','DieselHist']
    
for hist in historicas:
    delete_historic(hist)


#Carga datos desde Energía Abierta a Tabla Histórica 

def upload_historic_data(tabla, datos):
    cargar_a_tabla_hist = lambda df: df.to_sql(tabla, con=engine, if_exists='append', index=False, dtype={
        'id': sa.types.NVARCHAR(length=50),
        'fecha_inicio': sa.types.Date(),
        'fecha_fin': sa.types.Date(),
        'precio': sa.types.Float(),
        'tipo': sa.types.NVARCHAR(length=50)
    })
    [cargar_a_tabla_hist(df) for df in datos]


upload_historic_data("Bencina93Hist", bencinas_93)
upload_historic_data("Bencina95Hist", bencinas_95)
upload_historic_data("Bencina97Hist", bencinas_97)
upload_historic_data("DieselHist", petroleo_diesel)

# Formatea los datos obtenidos desde Energía Abierta para el año 2024

data_2024 = pd.read_csv("C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/combustible/precios por estacion/energia_abierta/2024.csv/2024.csv", sep=",")

reemplazos = {
    'DI':'Diesel',
    '93':'Bencina93',
    '95':'Bencina95',
    '97':'Bencina97'
}


def modify_data(data,fueltype, reemplazos):
    renamed_data = data.rename(columns={'codigo':'id','fecha_actualizacion':'fecha_inicio','combustible':'tipo'})
    renamed_data['fecha_fin'] = pd.Timestamp.now().strftime('%Y-%m-%d')
    reordered_data = renamed_data[['id','fecha_inicio','fecha_fin','precio','tipo','hora_actualizacion']]
    reordered_data['tipo'] = reordered_data['tipo'].replace(reemplazos)
    modified_data = reordered_data[reordered_data['tipo'].isin(reemplazos.values())]
    filtered_data = modified_data[modified_data['tipo']==fueltype]
    return filtered_data

Bencina93_24, Bencina95_24, Bencina97_24, Diesel_24 = (modify_data(data_2024, name, reemplazos) for name in ['Bencina93', 'Bencina95', 'Bencina97', 'Diesel'])

    

# Realiza respaldo de la tabla histórica en la tabla Anterior


def update_table(table_ant,table_hist):
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Eliminar los registros existentes en la tabla
            conn.execute(
                text(f'''
                DELETE FROM {table_ant}
                ''')
            )
            
            # Insertar los registros desde la tabla Histórica
            conn.execute(
                text(f'''
                INSERT INTO Estudios.dbo.{table_ant} (id, fecha_inicio, fecha_fin, precio, tipo)
                SELECT id, fecha_inicio, fecha_fin, precio, tipo
                FROM {table_hist}
                ''')
            )
            
            # Confirmar la transacción
            trans.commit()
        except Exception as e:
            trans.rollback()
            print(f"Ha ocurrido un error al momento de actualizar {table_ant}: {e}")
        finally:
            conn.close()

update_table('Bencina93Anterior','Bencina93Hist')
update_table('Bencina95Anterior','Bencina95Hist')
update_table('Bencina97Anterior','Bencina97Hist')
update_table('DieselAnterior','DieselHist')

#Modifica tabla auxilar en la que se irán cargando los nuevos datos

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
cargar_y_modificar_aux("Bencina93Aux", Bencina93_24)
cargar_y_modificar_aux("Bencina95Aux",Bencina95_24)
cargar_y_modificar_aux("Bencina97Aux",Bencina97_24)
cargar_y_modificar_aux("DieselAux",Diesel_24)
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
