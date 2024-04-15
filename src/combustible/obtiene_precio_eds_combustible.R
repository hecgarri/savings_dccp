rm(list=ls())

library(dplyr)
library(ggplot2)
library(lubridate)

setwd('C:/o/OneDrive - DCCP/Traspaso/2. Cálculo de Ahorro')
myconn <- RODBC::odbcConnect("DW_new", uid="datawarehouse" , pwd="datawarehouse")

# fecha de calculo de los precios 
fecha <- '20230901'

# datos de precios por estacion
precio_93 <- RODBC::sqlQuery(myconn,"SELECT * FROM [Estudios].[dbo].[Bencina93Hist]")
precio_95 <- RODBC::sqlQuery(myconn,"SELECT * FROM [Estudios].[dbo].[Bencina95Hist]")
precio_97 <- RODBC::sqlQuery(myconn,"SELECT * FROM [Estudios].[dbo].[Bencina97Hist]")
precio_diesel <- RODBC::sqlQuery(myconn,"SELECT * FROM [Estudios].[dbo].[DieselHist]")

names(precio_97)[names(precio_97) == 'Tipo']   <- 'tipo'
precio_por_estacion <- bind_rows(precio_93,precio_95,precio_97,precio_diesel)
precio_por_estacion <- precio_por_estacion %>% mutate(codigo_producto = case_when(tipo == 'Bencina93' ~ 93,
                                                                                  tipo == 'Bencina95' ~ 95,
                                                                                  tipo == 'Bencina97' ~ 97,
                                                                                  tipo == 'Diesel' ~ 99,
                                                                                  TRUE ~ 0)
                                                      )

# eliminamos outlier de precios
precio_por_estacion <- precio_por_estacion[(precio_por_estacion$precio > 200 & precio_por_estacion$precio < 2000),]


# diccionario de estaciones de servicio
estaciones <- readxl::read_xlsx('./data/combustible/Diccionario Estaciones de Servicio.xlsx')

# ejecutamos el archivo que procesa los datos recibidos por parte de los proveedores
source('./src/procesa_datos_combustible.R')

# tabla con cobertura de eds 
transacciones_final %>% 
  mutate(sin_eds = if_else(is.na(id_estacion_proveedor)==TRUE,1,0)) %>%
  group_by(sin_eds) %>%
  count()

# extraccion del precio lista en la estacion de servicio
# --------------------------------------------------------------
precios_extraer <- transacciones_final %>%
  group_by(id_estacion_proveedor,fecha,codigo_producto) %>%
  count() %>%
  select(id_estacion_proveedor,fecha,codigo_producto)

precios_extraer <- merge(precios_extraer,
                         estaciones %>% select(id_proveedor,id_cne,comuna,region,proveedor),
                         by.x = 'id_estacion_proveedor', by.y = 'id_proveedor', all.x = TRUE)

writexl::write_xlsx(data.frame('id_estacion_proveedor' = unique(precios_extraer$id_estacion_proveedor[is.na(precios_extraer$id_cne)==TRUE])),
                    paste0('./outputs/revisiones/Revision_Combustible_',fecha,'.xlsx'))

# capturamos el precio de la estacion solo en aquellas homologadas
precios_extraer <- precios_extraer[is.na(precios_extraer$id_cne)==FALSE,]
A <- precios_extraer

db_prueba <- merge(A,precio_por_estacion, #%>% select(-proveedor), 
                   by.x = c('id_cne','codigo_producto'), by.y = c('id','codigo_producto'), 
                   all.x = TRUE)

db_prueba$fecha_inicio <- as.Date(db_prueba$fecha_inicio, format = '%Y-%m-%d') 
db_prueba$fecha_fin <- as.Date(db_prueba$fecha_fin, format = '%Y-%m-%d') 
db_prueba$es_fecha <- if_else(db_prueba$fecha >= db_prueba$fecha_inicio & db_prueba$fecha <= db_prueba$fecha_fin,1,0)

base_final <- db_prueba[db_prueba$es_fecha == 1,]

saveRDS(base_final,paste0('./data/combustible/precios por estacion/precios_por_estacion_',substr(fecha,1,4),substr(fecha,5,6),'.rds'))
