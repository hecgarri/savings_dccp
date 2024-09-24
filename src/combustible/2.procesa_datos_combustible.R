  # script que procesa datos de consumo de combustible
  
  # datos transaccionales
  carpeta_copec <- list.files(path="C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/combustible/Copec")
  carpeta_esmax <- list.files(path="C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/combustible/Esmax")
  #carpeta_enex  <- list.files(path="C:/o/OneDrive - DCCP/Datos Prov Combustible/Enex")
  
  archivos_copec <- carpeta_copec[stringr::str_detect(stringr::str_to_lower(carpeta_copec),'copec') & (stringr::str_detect(stringr::str_to_lower(carpeta_copec),fecha))]
  archivos_esmax <- carpeta_esmax[stringr::str_detect(stringr::str_to_lower(carpeta_esmax),'esmax') & (stringr::str_detect(stringr::str_to_lower(carpeta_esmax),fecha))]
  
  transacciones_copec <- data.frame()
  transacciones_esmax <- data.frame()
  
  variables_necesarias <- c('id_estacion_proveedor','fecha','rut','codigo_producto','monto_pagado','litros')
  dir_inputs <- "C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/combustible"

# ========================================================= #
# copec
# ========================================================= #
for (i in 1:length(archivos_copec)) {
  temp_data <- readxl::read_xlsx(paste0(dir_inputs,"/Copec/",archivos_copec[i]))
  
  names(temp_data)[(names(temp_data) == 'EdsSAP') | (names(temp_data) == 'Eds') | (names(temp_data)=='C?digo EDS') | (names(temp_data)=='COD_ES') | (names(temp_data))=='CODIGO EDS']  <- 'id_estacion_proveedor'
  names(temp_data)[(names(temp_data) == 'CodProdu') | (names(temp_data)=='C?d. Producto') | (names(temp_data)=='C?digo producto') | (names(temp_data)=='COD_PROD') | (names(temp_data)=='CODIGO PRODU')] <- 'codigo_producto'
  names(temp_data)[(names(temp_data) == 'MontoTrx') | (names(temp_data)=='Monto Transacci?n') | (names(temp_data)=='MONTO_TRN') | (names(temp_data)=='MONTO TRN')] <- 'monto_pagado_sin_descuento'
  names(temp_data)[(names(temp_data) == 'VolTrx') | (names(temp_data)=='Volumen Transacci?n') | (names(temp_data)=='VOLUMEN_TRN') | (names(temp_data)=='VOLUMEN TRN')] <- 'litros'
  names(temp_data)[(names(temp_data) == 'Rut') | (names(temp_data)=='RUT Cliente') | (names(temp_data)=='RUT_CLIENTE') | (names(temp_data)=='RUT CLIENTE')] <- 'rut'
  names(temp_data)[(names(temp_data) == 'Monto Descuento') | (names(temp_data) == 'Descuento Transacci?n') | (names(temp_data) =='MONTO_DESC') | (names(temp_data) == 'MONTO DESCTO')] <- 'descuento_unidad'
  names(temp_data)[(names(temp_data) == 'Fecha') | (names(temp_data) == 'Fecha de transacci?n') | (names(temp_data)=='FECHA_TRN') | (names(temp_data)=='FECHA TRN')] <- 'fecha'
  
  # aplicamos el descuento en el caso de copec
  temp_data$descuento_unidad <- as.numeric(temp_data$descuento_unidad)
  temp_data$litros <- as.numeric(temp_data$litros)
  temp_data$monto_pagado <- temp_data$monto_pagado_sin_descuento-(temp_data$descuento_unidad*temp_data$litros)
  
  # rut
  temp_data <- temp_data %>% mutate(rut_organismo = paste0(substr(rut,1,2),'.',substr(rut,3,5),'.',substr(rut,6,8)))
  print(table(temp_data$fecha))
  # fecha
  if (archivos_copec[i] == 'trans_copec_20201001.xlsx') {
    temp_data$fecha <- lubridate::make_date(year = substr(temp_data$fecha,1,4),
                                            month = substr(temp_data$fecha,5,6),
                                            day = substr(temp_data$fecha,7,8))
    
  } else if  (archivos_copec[i] == 'trans_copec_20201201.xlsx' | archivos_copec[i] == 'trans_copec_20210101.xlsx' | archivos_copec[i] == 'trans_copec_20230501.xlsx') {
    temp_data$fecha <- as.Date.character(temp_data$fecha, format = '%Y-%m-%d')
    
  } 
  transacciones_copec <- bind_rows(transacciones_copec,
                                   temp_data %>% select(all_of(variables_necesarias)))
} 

  
# ========================================================= #
# petrobras
# ========================================================= #
for (i in 1:length(archivos_esmax)) {
  temp_data <- data.table::fread(paste0(dir_inputs,"/Esmax/",archivos_esmax[i]),sep = ";", encoding = 'UTF-8')

  names(temp_data)[names(temp_data) == 'Cod..Platino' | (names(temp_data) == 'Cod.Platino') | (names(temp_data) == 'Cod. Platino')]    <- 'id_estacion_proveedor'
  names(temp_data)[names(temp_data) == 'Producto']        <- 'codigo_producto'
  names(temp_data)[names(temp_data) == 'Total.Pago' | names(temp_data) == 'Total Pago']      <- 'monto_pagado'
  names(temp_data)[names(temp_data) == 'Volumen..L.' | (names(temp_data) == 'Volumen')  | (names(temp_data) == 'Volumen (L)')]     <- 'litros'
  names(temp_data)[(names(temp_data) == 'Fecha de transaccii?n') | (names(temp_data) == 'Fecha.Transacci?n') 
                  | (names(temp_data) == 'Fecha.Transacción') | (names(temp_data) == 'Fecha.Transaccion') | 
                    names(temp_data)=='Fecha Transacción'] <- 'fecha'

  
  esmax_corr <- c('trans_esmax_20210101.csv',
                  'trans_esmax_20210601.csv',
                  'trans_esmax_20210801.csv',
                  'trans_esmax_20210901.csv',
                  'trans_esmax_20211101.csv',
                  'trans_esmax_20211201.csv',
                  'trans_esmax_20220201.csv',
                  'trans_esmax_20220301.csv',
                  'trans_esmax_20220401.csv',
                  'trans_esmax_20220501.csv',
                  'trans_esmax_20220601.csv',
                  'trans_esmax_20220701.csv',
                  'trans_esmax_20220901.csv',
                  'Trans_esmax_20230701.csv'
                  )

  if (archivos_esmax[i] %in% esmax_corr) {
    temp_data$fecha <- as.Date(temp_data$fecha, format = '%d-%m-%Y')
  } else {
    temp_data$fecha <- as.Date(temp_data$fecha, format = '%d/%m/%Y')
  }
  
  temp_data <- as.data.frame(temp_data)
  temp_data <- temp_data[,!duplicated(colnames(temp_data))]
  
  
  temp_data <- temp_data %>% mutate(rut = substr(RUT,1,10))
  temp_data$monto_pagado <- as.numeric(gsub(',','.',gsub('.','',as.character(temp_data$monto_pagado), fixed = TRUE), fixed = TRUE))
  if (is.character(temp_data$litros)){
    temp_data$litros <- as.numeric(gsub(',','.',gsub('.','',as.character(temp_data$litros), fixed = TRUE), fixed = TRUE))  
  }
  
  
  transacciones_esmax <- bind_rows(transacciones_esmax,
                                   temp_data %>% select(all_of(variables_necesarias)) %>% filter(codigo_producto != 'KEROSENE'))
} 

#transacciones_copec$fecha <- lubridate::as_date(fecha)  
  
# ========================================================= #
# ========================================================= #

transacciones_copec$proveedor <- 'Copec'
transacciones_esmax$proveedor <- 'Petrobras'

# limpieza de datos

transacciones_esmax <- transacciones_esmax %>% 
  mutate(codigo_producto = case_when((trimws(codigo_producto, which = 'right') == 'GASOLINA 93' | trimws(codigo_producto, which = 'right') == 'GASOLINA 93 RM' | trimws(codigo_producto, which = 'right') == 'Gasolina 93') ~ 93,
                                     (trimws(codigo_producto, which = 'right') == 'GASOLINA 95' | trimws(codigo_producto, which = 'right') == 'GASOLINA 95 RM' | trimws(codigo_producto, which = 'right') == 'Gasolina 95') ~ 95,
                                     (trimws(codigo_producto, which = 'right') == 'GASOLINA 97' | trimws(codigo_producto, which = 'right') == 'GASOLINA 97 RM' | trimws(codigo_producto, which = 'right') == 'V-POWER') ~ 97,
                                     (trimws(codigo_producto, which = 'right') == 'DIESEL A1' | trimws(codigo_producto, which = 'right') == 'DIESEL B' | 
                                        trimws(codigo_producto, which = 'right') == 'Petroleo Diesel' | trimws(codigo_producto, which = 'right') == 'SD Extra') ~ 99,
                                     TRUE ~ 0))

transacciones_copec <- transacciones_copec %>% 
  mutate(codigo_producto = case_when(codigo_producto == 103 ~ 93,
                                     codigo_producto == 105 ~ 95,
                                     codigo_producto == 107 ~ 97,
                                     codigo_producto == 311 ~ 99,
                                     TRUE ~ 0))

# fecha de calculo de ahorro
fecha_inicial <- lubridate::make_date(substr(fecha,1,4),substr(fecha,5,6),substr(fecha,7,8))
fecha_final   <- lubridate::make_date(substr(fecha,1,4),as.numeric(substr(fecha,5,6)),substr(fecha,7,8)) 
fecha_final   <- floor_date(fecha_final + days(35), unit = 'month') - lubridate::days(1)

# unimos datos de los distintos proveedores
transacciones_final <- rbind(transacciones_esmax %>% filter(fecha >= fecha_inicial & fecha <= fecha_final & monto_pagado > 0),
                             transacciones_copec %>% filter(fecha >= fecha_inicial & fecha <= fecha_final & monto_pagado > 0))
