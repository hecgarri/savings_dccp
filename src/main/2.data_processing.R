# script que procesa la informacion que viene del mercado

message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message(' 1) Comienza el proceso de calculo de ahorro semanal')

# por default el import_date corresponde a cada domingo de la semana
# el script debe ejecutarse entre lunes y domingo de la semana siguiente. 
# no importa el día en que se procese de esa semana, siempre extrae los precios y archivo de
# la semana anterior

download_day  <- 1 # parametro que cambia de acuerdo con la fecha de carga de los precios (lunes actualmente)
week_last_day <- ceiling_date(today,'weeks')

import_date <- floor_date(today,'weeks', week_start = 1) - days(download_day)
month_first_day <- floor_date(import_date,'month')
month_last_day  <- ceiling_date(import_date,'month') - days(1)

if (wday(today) == 2) {
  message(paste0('====> ',import_date,': fecha correcta de actualizacion de precios!'))
}


# if (wday(import_date,week_start = 1) == download_day + 1 & today >= import_date & today < week_last_day) {
#   message(paste0('====> ',import_date,': fecha correcta de descarga de precios!'))
# } else {
#   message('====> descarga de precios de esta semana aun no disponible!') 
#   message('====> se descargan los precios de la semana previa.') 
#   import_date     <- import_date - days(7)
#   month_first_day <- floor_date(import_date,'month')
#   month_last_day  <- ceiling_date(import_date,'month') - days(1)
# }
# 
# transformamos la fecha de carga de los datos a numerica para posterior creacion de archivos
import_date_numeric <- paste0(year(import_date),
                              ifelse(month(import_date)<10,paste0(0,month(import_date)),month(import_date)),
                              ifelse(day(import_date)<10,paste0(0,day(import_date)),day(import_date)))

# importamos el archivo
files      <- list.files(path="./data/shopeo")
main_files <- files[str_detect(str_to_lower(files),"reportepreciossemanal") & 
                    str_detect(str_to_lower(files),paste0(format(import_date,'%d-%m%-%Y'))) & 
                    !str_detect(str_to_lower(files),"~")]
market_prices <- readxl::read_xlsx(paste0('./data/shopeo/',main_files))
message(paste0('====> se importa el archivo ',main_files,' actualizado el ',format(import_date,'%d-%m%-%Y')),
        ' y procesado el ',format(today,'%d-%m%-%Y'))

# importamos las tiendas
# tiendas <- readxl::read_xlsx('./savings_cm/input/tiendas.xlsx')
# message('====> se importa el archivo Tiendas.xlsx')
# market_prices <- merge(market_prices,# %>% select(-NombreTienda),
#                        tiendas,
#                        by = 'IdTienda',
#                        all.x =  TRUE)


# modificamos el nombre de las columnas
names(market_prices)[names(market_prices) == "CodigoMC"]        <- 'id_producto'
names(market_prices)[names(market_prices) == "IdTienda"]        <- 'id_tienda'
names(market_prices)[names(market_prices) == "Categoria"]       <- 'convenio'
names(market_prices)[names(market_prices) == "FechaCaptura"]    <- 'fecha_captura'
names(market_prices)[names(market_prices) == "NombreTienda"]    <- 'tienda'
names(market_prices)[names(market_prices) == "LinkProducto"]    <- 'url'
names(market_prices)[names(market_prices) == "NombreProducto"]  <- 'producto'
names(market_prices)[names(market_prices) == "PrecioCapturado"] <- 'precio_capturado'

market_prices$PrecioConvenioMarco <- NULL
market_prices$convenio[market_prices$convenio == 'Alimentos']  <- 'Alimentos RM'
market_prices$convenio[market_prices$convenio == 'Escritorio'] <- 'Escritorio RM'

message("========================")
message("resumen importacion de precios")
summary_market_prices <- market_prices %>% 
  group_by(convenio) %>% 
  summarise(productos = n_distinct(id_producto), 
            n_precios = n()/productos)

print(summary_market_prices)



# rm(list = c('download_day',
#             'week_last_day',
#             'summary_market_prices',
#             'files'))

message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message(' 2) Comienza el proceso de validacion de precios de mercado')

market_prices_old <- market_prices
market_prices     <- market_prices_old %>% distinct()   # elimina los duplicados totales
message(paste0('====> se eliminan ',nrow(market_prices_old) - nrow(market_prices),' capturas de precio duplicadas'))
rm(market_prices_old)

# elimina duplicados de una misma captura a distintas horas del dia 
# (mismo producto y proveedor)
market_prices_old <- market_prices
elimina_primera <- FALSE 

if (elimina_primera == TRUE) {
  # eliminamos la primera captura realizada
  market_prices <- market_prices %>% 
    arrange(id_producto,id_tienda,url,desc(fecha_captura)) %>%   # agrupamos por producto, proveedor y fecha descendente
    distinct(id_producto,id_tienda,url, .keep_all = TRUE)        # eliminamos la primera cotizacion capturada
} else {
  # eliminamos la ultima captura realizada
  market_prices <- market_prices %>% 
    arrange(id_producto,id_tienda,url,fecha_captura) %>%   # agrupamos por producto, proveedor y fecha ascendente
    distinct(id_producto,id_tienda,url,.keep_all = TRUE)  # eliminamos la ultima cotizacion capturada
}

message(paste0('====> se eliminan ',nrow(market_prices_old) - nrow(market_prices),' capturas de precio tomados en una misma fecha'))
rm(market_prices_old)

# empresas no validas
empresa_no_valida <- c('insumos esami',
                       'puntopapelexpress',
                       'soin',
                       'coloma',
                       'dydvaldivia',
                       'market coupling',
                       'cobronce',
                       #'farmazon',
                       'mercadito saludable',
                       'ahorroexpress',
                       'olostocks')

market_prices_old <- market_prices
market_prices <- market_prices[market_prices$precio_capturado>0,]                # eliminamos las capturas con precio 0
market_prices <- market_prices[is.na(market_prices$tienda)==FALSE,]              # eliminamos las capturas sin un proveedor definido
market_prices <- market_prices[!market_prices$tienda %in% empresa_no_valida,]    # eliminamos capturas de tiendas no validas
message(paste0('====> se eliminan ',nrow(market_prices_old) - nrow(market_prices),' capturas de precio = 0, sin proveedor definido o de tiendas no validas'))
rm(market_prices_old)

# productos que contienen dos item distintos (ej: estufa + kit de instalacion)
# en estos casos se reemplaza el valor de cada parte por la suma total del producto y sus accesorios
multiproductos <- c(
  1634347,
  1634472,
  1634761,
  1635306,
  1635368,
  1702610,
  1702621
)

sumar_disco <- c(1848417)
sumar_ram_4 <- c(1848424)
sumar_ram_8 <- c(1848426)

for (item in multiproductos) {
  message(unique(market_prices$producto[market_prices$id_producto == item]))
  message(paste0('se reemplaza el precio del componente: ',market_prices$precio_capturado[market_prices$id_producto == item],' '))
  market_prices$precio_capturado[market_prices$id_producto == item] <- sum(market_prices$precio_capturado[market_prices$id_producto == item])
}

for (item in sumar_ram_4) {
  precio_old = market_prices$precio_capturado[market_prices$id_producto==item]
  precio_ram = if (length(market_prices$precio_capturado[market_prices$id_producto==1])==0) {0} else{
    market_prices$precio_capturado[market_prices$id_producto==1]
  }
  market_prices$precio_capturado[market_prices$id_producto==item] <- precio_old + precio_ram
}


for (item in sumar_disco) {
  precio_old = market_prices$precio_capturado[market_prices$id_producto==item]
  precio_ram = market_prices$precio_capturado[market_prices$id_producto==2]
  market_prices$precio_capturado[market_prices$id_producto==item] <- precio_old + precio_ram
}

if (any(market_prices$id_producto==3)){
  for (item in sumar_ram_8) {
    precio_old = market_prices$precio_capturado[market_prices$id_producto==item]
    precio_ram = market_prices$precio_capturado[market_prices$id_producto==3]
    market_prices$precio_capturado[market_prices$id_producto==item] <- precio_old + precio_ram
  }  
} else {message("No se encontraron productos equivalentes para realizar el reemplazo")}


  
market_prices <- market_prices %>%
  group_by(id_producto) %>%
  mutate(sd_prom = (precio_capturado-mean(precio_capturado))/mean(precio_capturado)*100,
         revisar = if_else(sd_prom < lo_lim | sd_prom > up_lim,1,0))

price_outlier <- market_prices$id_producto[market_prices$revisar == 1]
price_outlier_tab <- market_prices[market_prices$id_producto %in% price_outlier,]
writexl::write_xlsx(price_outlier_tab,paste0('./output/validate/capturas_revision_',import_date_numeric,'.xlsx'))

market_prices_fin <- market_prices[!market_prices$id_producto %in% price_outlier,]
message(paste0('====> no se consideran ',nrow(market_prices) - nrow(market_prices_fin),' capturas de precio por posible error'))
market_prices <- market_prices_fin 

rm(list=c('elimina_primera',
          'up_lim',
          'lo_lim',
          'price_outlier',
          'price_outlier_tab',
          'market_prices_fin'#,
          #'import_date_numeric'
))

message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message('Procesamiento de datos finaliza correctamente!')