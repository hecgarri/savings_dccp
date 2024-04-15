
message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message(' 1) Comienza el proceso de calculo de ahorro semanal')

# por default el import_date corresponde a cada jueves de la semana
# el script debe ejecutarse entre jueves y domingo de esa semana

# fecha de ejecucion
today <- Sys.Date()
#today         <- as.Date('2021-11-12')
download_day  <- 3 # parametro que cambia de acuerdo con la fecha de carga de los precios (jueves actualmente)
week_last_day <- ceiling_date(today,'weeks')

import_date     <- floor_date(today,'weeks', week_start = 1) + days(download_day)
month_first_day <- floor_date(import_date,'month')
month_last_day  <- ceiling_date(import_date,'month') - days(1)

if (wday(import_date,week_start = 1) == download_day + 1 & today >= import_date & today < week_last_day) {
  message(paste0('====> ',import_date,': fecha correcta de descarga de precios!'))
} else {
  message('====> descarga de precios de esta semana aun no disponible!') 
  message('====> se descargan los precios de la semana previa.') 
  import_date     <- import_date - days(7)
  month_first_day <- floor_date(import_date,'month')
  month_last_day  <- ceiling_date(import_date,'month') - days(1)
}

# transformamos la fecha de carga de los datos a numerica para posterior creacion de archivos 
import_date_numeric <- paste0(year(import_date),
                              ifelse(month(import_date)<10,paste0(0,month(import_date)),month(import_date)),
                              ifelse(day(import_date)<10,paste0(0,day(import_date)),day(import_date)))

# importamos el archivo
files      <- list.files(path="./savings_cm/data")
main_files <- files[str_detect(str_to_lower(files),"precios") == TRUE & str_detect(str_to_lower(files),import_date_numeric) == TRUE]
market_prices <- readxl::read_xlsx(paste0('./savings_cm/data/',main_files))

message(paste0('====> se importa el archivo ',main_files,' cargado el ',import_date))

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

summary_market_prices

# agregamos un case para la primera y ultima semana del mes
# la idea es que no queden dias sueltos a inicio ni a fin de mes

if (month(import_date) != month(import_date - days(7))) {
  # inicio de mes
  days_left     <- days(import_date - month_first_day)
  import_date   <- month_first_day
  is_first_week <- 1
  
} else {
  is_first_week <- 0
}


if (month(import_date) != month(import_date + days(7))) {
  # final de mes
  days_left     <- days(month_last_day - import_date)
  import_date   <- month_last_day
  is_final_week <- 1
  
} else {
    is_final_week <- 0
  }

rm(list = c('download_day',
            'week_last_day',
            'summary_market_prices',
            'files'))