rm(list = ls())

library(httr)
library(tidyverse)

setwd("C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/")

url <- "https://api.cne.cl/api/login"
email <- "hectorgarridohenriquez@gmail.com"
password <- "r2ikSGpgwetgjAk"

get_auth_token <- function(email, password) {
  url <- "https://api.cne.cl/api/login"
  response <- POST(
    url,
    body = list(email = email, password = password),
    encode = "json"
  )
  content(response)$token
}

token <- get_auth_token(email = email, password = password)

# Función para hacer una solicitud autenticada utilizando el token
make_authenticated_request <- function(token, endpoint) {
  url <- paste0("https://api.cne.cl", endpoint)
  response <- GET(
    url,
    add_headers(Authorization = paste("Bearer", token))
  )
  content(response)
}


data <- make_authenticated_request(token = token, endpoint = "/api/v4/estaciones")

fecha <- gsub("-", "", Sys.Date())

saveRDS(data,paste0('./data/combustible/precios por estacion/raw_data/datos_desestructurados_',fecha,'.rds'))


# [id_est_fecha]
# ,[id]
# ,[proveedor]
# ,[fecha_hora_actualizacion]
# ,[id_comuna]
# ,[nombre_comuna]
# ,[id_region]
# ,[nombre_region]
# ,[fecha_subida]
# ,[precio93]
# ,[precio95]
# ,[precio97]
# ,[precio_diesel]
# ,[latitud]
# ,[longitud]


sapply(data[[76]], function(x) if (class(x)=="list") "list" else "vector")


convertir_lista_a_dataframe <- function(data) {
  df <- data.frame(
    id = sapply(data, function(x) x$codigo),
    proveedor = sapply(data, function(x) x$distribuidor$marca),
    fecha_hora_actualizacion = sapply(data, function(x) paste0(x$precios$`93`$fecha_actualizacion, " ", x$precios$`93`$hora_actualizacion)),
    id_comuna = sapply(data, function(x) x$ubicacion$codigo_comuna),
    nombre_comuna = sapply(data, function(x) x$ubicacion$nombre_comuna),
    id_region = sapply(data, function(x) x$ubicacion$codigo_region),
    nombre_region = sapply(data, function(x) x$ubicacion$nombre_region),
    precio93 = ifelse(sapply(data, function(x) length(x$precios$`93`$precio)== 0) , NA, unlist(sapply(data, function(x) x$precios$`93`$precio))),
    precio95 = ifelse(sapply(data, function(x) length(x$precios$`95`$precio) == 0), NA, unlist(sapply(data, function(x) x$precios$`95`$precio))),
    precio97 = ifelse(sapply(data, function(x) length(x$precios$`97`$precio) == 0), NA, unlist(sapply(data, function(x) x$precios$`97`$precio))),
    precio_diesel = ifelse(sapply(data, function(x) length(x$precios$`DI`$precio) == 0), NA, unlist(sapply(data, function(x) x$precios$`DI`$precio))),
    latitud = sapply(data, function(x) x$ubicacion$latitud),
    longitud = sapply(data, function(x) x$ubicacion$longitud)
    )
    return(df)
}



df <- convertir_lista_a_dataframe(data)

saveRDS(df, paste0('./data/combustible/precios por estacion/daily_data/datos_estructurados_',fecha,'.rds'))

