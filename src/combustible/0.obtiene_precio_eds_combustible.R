rm(list=ls())

library(dplyr)
library(ggplot2)
library(lubridate)
library(tidyr)

setwd('C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp')
myconn <- RODBC::odbcConnect("dw", uid="datawarehouse" , pwd="datawarehouse")

# fecha de calculo de los precios 
fecha <- '20240801'
anio_ahorro <- substr(fecha,1,4)
mes_ahorro <- as.numeric(substr(fecha,5,6))

inicio <- as.Date(fecha,format = "%Y%m%d") %m-% months(2)
inicio_mes <- as.Date(fecha,format = "%Y%m%d")
final <- as.Date(fecha,format = "%Y%m%d") %m+% months(1) %m-% days(1)

# datos de precios por estacion
# Aquí incluyo mes_ahorro-1 para tener una ventana más amplia para capturar 
# los precios que no varían, aunque luego use sólo un mes en adelante.
precio_93 <- RODBC::sqlQuery(myconn,paste0("SELECT DISTINCT 
                                    id
                                    ,fecha_inicio
                                    ,hora_actualizacion
                                    ,precio
                                    ,tipo
                                    from estudios.dbo.Bencina93Hist
                                    where cast(fecha_inicio as date) between '"
                                    ,as.Date(fecha,format = "%Y%m%d") %m-% months(2),"' and '"
                                    ,as.Date(fecha,format = "%Y%m%d") %m+% months(1),"'"))
precio_95 <- RODBC::sqlQuery(myconn,paste0("SELECT DISTINCT 
                                    id
                                    ,fecha_inicio
                                    ,hora_actualizacion
                                    ,precio
                                    ,tipo
                                    from estudios.dbo.Bencina95Hist
                                    where fecha_inicio between '"
                                    ,as.Date(fecha,format = "%Y%m%d") %m-% months(2),"' and '"
                                    ,as.Date(fecha,format = "%Y%m%d") %m+% months(1),"'"))
precio_97 <- RODBC::sqlQuery(myconn,paste0("SELECT DISTINCT 
                                    id
                                    ,fecha_inicio
                                    ,hora_actualizacion
                                    ,precio
                                    ,tipo
                                    from estudios.dbo.Bencina97Hist
                                    where fecha_inicio between '"
                                    ,as.Date(fecha,format = "%Y%m%d") %m-% months(2),"' and '"
                                    ,as.Date(fecha,format = "%Y%m%d") %m+% months(1),"'"))
precio_diesel <- RODBC::sqlQuery(myconn,paste0("SELECT DISTINCT 
                                    id
                                    ,fecha_inicio
                                    ,hora_actualizacion
                                    ,precio
                                    ,tipo
                                    from estudios.dbo.DieselHist
                                    where fecha_inicio between '"
                                    ,as.Date(fecha,format = "%Y%m%d") %m-% months(2),"' and '"
                                    ,as.Date(fecha,format = "%Y%m%d") %m+% months(1),"'"))

###==========================================================================
# Esta sección es nueva y busca darle una estructura de panel a los datos de bencinas =====================
# porque de lo contrario, al momento de realzar el merge con los datos de proveedores
# se genera una brecha importante que puede afectar el cálculo. Fecha comentario: 17 de junio de 2024


fill_missing <- function(df, start_date, end_date) {
  id <- unique(df$id)
  
  # Crear una secuencia de fechas desde start_date hasta end_date
  dates <- data.frame(
    fecha = seq(as.Date(start_date), as.Date(end_date), by = "day")
  )
  
  # Crear un data.frame con todas las combinaciones de fechas e IDs
  fechas_completas <- expand.grid(fecha_inicio = dates$fecha, id = id)
  
  # Asegurarse de que las columnas fecha_inicio y hora en df sean de tipo Date y POSIXct respectivamente
  df$fecha_inicio <- as.Date(df$fecha_inicio)
  df$fecha_hora <- as.POSIXct(paste(df$fecha_inicio, df$hora_actualizacion), format = "%Y-%m-%d %H:%M:%S")
  
  # Unir las tablas y rellenar los precios
  precio_por_estacion <- fechas_completas %>%
    left_join(df, by = c("fecha_inicio", "id")) %>%
    arrange(id, fecha_inicio, fecha_hora) %>% # Ordenar por id, fecha y hora
    group_by(id) %>%
    fill(precio, .direction = "down") %>%
    fill(tipo, .direction = "down") %>%
    ungroup() %>% 
    mutate(fecha_hora = if_else(is.na(hora_actualizacion)
                               ,as.POSIXct(paste(fecha_inicio,"00:00:00"), format = "%Y-%m-%d %H:%M:%S")
                               ,fecha_hora))

  return(precio_por_estacion)
}

precio_por_estacion <- list(precio_93,precio_95,precio_97,precio_diesel)



precio_por_estacion <- lapply(precio_por_estacion
                       ,function(x) fill_missing(x
                                   ,start_date = inicio, end_date = final) %>% 
                               filter(fecha_inicio>=inicio_mes)) %>% 
  data.table::rbindlist()


#==========================================================================
##==========================================================================
###==========================================================================

precio_por_estacion <- precio_por_estacion %>% mutate(codigo_producto = case_when(tipo == 'Bencina93' ~ 93,
                                                                                  tipo == 'Bencina95' ~ 95,
                                                                                  tipo == 'Bencina97' ~ 97,
                                                                                  tipo == 'Diesel' ~ 99,
                                                                                  TRUE ~ 0)
                                                      ) %>% 
  rename(fecha = fecha_inicio)

# eliminamos outlier de precios
precio_por_estacion <- precio_por_estacion[(precio_por_estacion$precio > 200 & precio_por_estacion$precio < 2000),]

# diccionario de estaciones de servicio
estaciones <- readxl::read_xlsx('./data/combustible/Diccionario Estaciones de Servicio.xlsx')

# ejecutamos el archivo que procesa los datos recibidos por parte de los proveedores
source('./src/combustible/2.procesa_datos_combustible.R')

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

# writexl::write_xlsx(data.frame('id_estacion_proveedor' = unique(precios_extraer$id_estacion_proveedor[is.na(precios_extraer$id_cne)==TRUE])),
#                     paste0('./outputs/revisiones/Revision_Combustible_',fecha,'.xlsx'))

# capturamos el precio de la estacion solo en aquellas homologadas
precios_extraer <- precios_extraer[is.na(precios_extraer$id_cne)==FALSE,]
A <- precios_extraer



  db_prueba <- merge(A,precio_por_estacion, #%>% select(-proveedor), 
                     by.x = c('id_cne','codigo_producto','fecha')
                     , by.y = c('id','codigo_producto','fecha'), 
                     all.x = TRUE)  


# db_prueba$fecha_inicio <- as.Date(db_prueba$fecha_inicio, format = '%Y-%m-%d') 
# db_prueba$fecha_fin <- as.Date(db_prueba$fecha_fin, format = '%Y-%m-%d') 
# db_prueba$es_fecha <- if_else(db_prueba$fecha >= db_prueba$fecha_inicio & db_prueba$fecha <= db_prueba$fecha_fin,1,0)
# 
 base_final <- db_prueba[db_prueba$es_fecha == 1,]

saveRDS(db_prueba,paste0('./data/combustible/precios por estacion/precios_por_estacion_',substr(fecha,1,4),substr(fecha,5,6),'.rds'))
