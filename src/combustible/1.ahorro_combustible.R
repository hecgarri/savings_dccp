rm(list=ls())

library(dplyr)
library(lubridate)
library(ggplot2)

#setwd('C:/o/OneDrive - DCCP/Traspaso/2. Cálculo de Ahorro')

# fecha de calculo
anio_ahorro <- 2023
mes_ahorro  <- 07
fecha       <- paste0(anio_ahorro,ifelse(mes_ahorro<10,paste0(0,mes_ahorro),mes_ahorro),'01')

source('./src/procesa_datos_combustible.R')

tabla_precio_estacion <- readRDS(paste0('./data/combustible/precios por estacion/precios_por_estacion_',
                                        paste0(anio_ahorro,ifelse(mes_ahorro<10,paste0(0,mes_ahorro),mes_ahorro)),'.rds'))

# unimos
ahorro_por_transaccion <- merge(transacciones_final %>% filter(is.na(id_estacion_proveedor)==FALSE),
                                tabla_precio_estacion %>% select(-proveedor),
                                by = c('id_estacion_proveedor','fecha','codigo_producto'),
                                all.x = TRUE)

ahorro_por_transaccion <- distinct(ahorro_por_transaccion)

# validaciones
length(ahorro_por_transaccion$monto_pagado[!is.na(ahorro_por_transaccion$id_cne)])
length(ahorro_por_transaccion$monto_pagado)
sum(ahorro_por_transaccion$monto_pagado[!is.na(ahorro_por_transaccion$id_cne)])
sum(ahorro_por_transaccion$monto_pagado)

# removemos outlier en el precio pagaod
ahorro_por_transaccion <- ahorro_por_transaccion %>%
  mutate(precio_pagado = monto_pagado/litros) %>%
  filter(between(precio_pagado,200,3000))

# cobertura
paste0('La cobertura obtenida corresponde a ',
       round(sum(ahorro_por_transaccion$monto_pagado[!is.na(ahorro_por_transaccion$id_cne)])/sum(ahorro_por_transaccion$monto_pagado)*100,2),'%')


ahorro_por_transaccion <- ahorro_por_transaccion  %>% 
  mutate(
    poiNroLicitacionPublica = '2239-21-LR22',
    convenio = 'Combustibles',
    CodigoOC = NA, #paste0(id_cne,codigo_producto,as.numeric(fecha)),
    MonedaOC = 'CLP',
    es_80p_gen = 1,
    n_cotizaciones = 1,
    precio_pagado,
    tipo_producto = case_when(codigo_producto == 99 ~ 'Diesel',
                              TRUE ~ 'Gasolina'),
    descuento_efectivo = round(precio - precio_pagado,0),
    ahorro_por_transaccion = descuento_efectivo * litros,
    ahorro_DCCP = round((1 - (precio_pagado / precio)),3)) %>%
  select(IDProductoCM=codigo_producto,
         id_licitacion = poiNroLicitacionPublica,
         CodigoOC,
         FechaOC = fecha,
         #Organismo = ,
         Proveedor = proveedor,
         Cantidad_Item = litros,
         MontoTotal_Item = monto_pagado,
         PrecioUnit_NETO = precio_pagado,
         MonedaOC,
         #producto_rm = ,
         producto = tipo_producto,
         precio_capturado_prom = precio,
         n_cotizaciones,
         convenio,
         precio_unitario = precio_pagado,
         ahorro_item = ahorro_DCCP,
         monto_ahorro_item = ahorro_por_transaccion,
         es_80p_gen
  )

# ahorro general y cobertura del periodo
paste0('El ahorro ponderado corresponde a ',round(weighted.mean(ahorro_por_transaccion$ahorro_item,ahorro_por_transaccion$MontoTotal_Item, na.rm = TRUE)*100,2),'%')
paste0('El ahorro total del periodo corresponde a $',round(sum(ahorro_por_transaccion$monto_ahorro_item, na.rm = TRUE),0))

writexl::write_xlsx(
  ahorro_por_transaccion %>% filter(year(FechaOC)==anio_ahorro & month(FechaOC)==mes_ahorro),
  paste0('C:/Users/javier.guajardo/Documents/GitHub/eficiencia_nuevo_modelo_cm/savings_cm/output/weekly savings/ahorro_combustible_',
         substr(fecha,1,6),'.xlsx'))

# distribucion
ahorro_por_transaccion %>%
  ggplot(aes(x = ahorro_item)) +
    geom_density()


