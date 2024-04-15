# script que calcula el ahorro semanal de los convenios monitoreados
rm(list=ls())

# librerias
library(dplyr)
library(lubridate)
library(stringr)
library(progress)
library(ggplot2)


# directorios
setwd('C:/O/OneDrive - DCCP/Github/eficiencia_nuevo_modelo_cm')

format_number <- function(x, digit,is_percent = FALSE) {
  x_new = format(x, digits = digit, big.mark = '.', decimal.mark = ',', scientific = FALSE)

  if (is_percent == FALSE) {
    x_new = paste0('$',x_new)
  } else {
    x_new = paste0(x_new,'%')
  }
  return(x_new)
}

# Parametros
# -------------------------------------------- #

# fecha de ejecucion
#today <- Sys.Date()
today         <- as.Date('2023-10-09') # fecha debe ser la de un lunes

# generamos una medida de las diferencias de precios por proveedor
# este es un proceso iterativo comenzando siempre desde 0.5 + y -
# de ahi en adelante vamos revisando hasta utilizar todas la cotizaciones
up_lim  <-  70   # por default dejan en 70%. 100% de diferencias como maximo
lo_lim  <- -70  
lim_inf <- -0.5  # limite de ahorro final 
lim_sup <-  0.6  # Limite de ahorro final

# --------- processing market prices --------- #

source('./savings_cm/source code/data_processing - test.R')

# -------- extracting cm prices -------------- #

source('./savings_cm/source code/extract_weekly.R')

# -------- getting savings ------------------- #

source('./savings_cm/source code/get_savings.R')

# ------------- tablas de resultado ---------- #

message('=============================')
message('calculamos resultados finales')

savings <- savings %>%
mutate(convenio = case_when(id_licitacion == '2239-7-LR17'  ~ 'Alimentos RM',
                            id_licitacion == '2239-5-LR19'  ~ 'Aseo',
                            id_licitacion == '2239-9-LR22'  ~ 'Aseo',
                            id_licitacion == '2239-17-LR20' ~ 'Escritorio RM',
                            id_licitacion == '2239-20-LR20' ~ 'Computadores',
                            id_licitacion == '2239-10-LR21' ~ 'Computadores',
                            id_licitacion == '2239-17-LR22' ~ 'Computadores',
                            id_licitacion == '2239-2-LR18'  ~ 'Ferreteria',
                            id_licitacion == '2239-13-LR21' ~ 'Insumos Salud',
                            TRUE ~ 'Otros')) %>%
  select(IDProductoCM,
         id_licitacion,
         CodigoOC,
         FechaOC,
         Organismo,
         Proveedor,
         producto,
         Cantidad_Item,
         MontoTotal_Item,
         PrecioUnit_NETO,
         MonedaOC,
         producto_rm,
         precio_capturado_prom,
         n_cotizaciones,
         convenio,
         precio_unitario,
         ahorro_item,
         monto_ahorro_item,
         es_80p_gen
  )

# monto de ahorro y cobertura por convenio
savings_final <- savings %>%
  filter(ahorro_item > lim_inf & ahorro_item < lim_sup & between(FechaOC, month_first_day, month_last_day)) 

summary_table <- merge(savings_final %>%
                         group_by(convenio) %>%
                         summarise(ahorro_promedio = weighted.mean(ahorro_item,MontoTotal_Item),
                                   productos = n_distinct(IDProductoCM),
                                   total_ahorro = sum(monto_ahorro_item),
                                   total_monitoreado = sum(MontoTotal_Item)),
                       transaction %>%
                         filter(producto_rm == 1) %>%
                         mutate(convenio = case_when(id_licitacion == '2239-7-LR17'  ~ 'Alimentos RM',
                                                     id_licitacion == '2239-5-LR19'  ~ 'Aseo',
                                                     id_licitacion == '2239-9-LR22'  ~ 'Aseo RM',
                                                     id_licitacion == '2239-17-LR20' ~ 'Escritorio RM',
                                                     id_licitacion == '2239-20-LR20' ~ 'Computadores',
                                                     id_licitacion == '2239-10-LR21' ~ 'Computadores',
                                                     id_licitacion == '2239-17-LR22' ~ 'Computadores',
                                                     id_licitacion == '2239-2-LR18'  ~ 'Ferreteria',
                                                     id_licitacion == '2239-13-LR21'  ~ 'Insumos Salud',
                                                     TRUE ~ 'Otros')) %>%
                         group_by(convenio) %>% 
                         summarise(total_transado = sum(MontoTotal_Item)),
                       by = 'convenio',
                       all.x = TRUE)

message('===========================================')
message('         tabla resumen de la semana        ')
message('===========================================')
summary_table %>%
  mutate(cobertura_monto = total_monitoreado / total_transado * 100) %>%
  select(convenio,ahorro_promedio,productos,total_ahorro,cobertura_monto) %>%
  arrange(desc(total_ahorro))

message('===========================================')
message('===========================================')
message(paste0('el ahorro de la semana corresponde a ',
               savings_final %>% summarise(total_ahorro = format_number(sum(monto_ahorro_item), digit = 1,is_percent = FALSE)),
             ' pesos, con una cobertura agregada del ',
             summary_table %>% 
               summarise(cobertura_monto_total = format_number(sum(total_monitoreado) / sum(total_transado) * 100,
                                                                               digit = 1, is_percent = TRUE)),'.'
             )
        )


# distribucion de ahorros por convenio
savings_final %>%
  filter(unique(year(savings_final$FechaOC)) == year(import_date) & unique(month(savings_final$FechaOC)) == month(import_date)) %>%
  ggplot(aes(x = ahorro_item, color = convenio, fill = convenio)) +
  geom_density(alpha = 0.5) +
  geom_vline(xintercept = 0, linetype = 'dashed', color = 'red') +
  labs(title = 'Distribucion de ahorro por convenio') +
  scale_x_continuous(labels = scales::percent) +
  theme_bw()


# --------- storing savings --------- #

writexl::write_xlsx(savings_final,
  #savings_final %>% filter(!is.na(precio_unitario)), 
  paste0('./savings_cm/output/weekly savings/ahorro_',import_date_numeric,'.xlsx'))


# estudios <- RODBC::odbcConnect("Estudios DW", uid="datawarehouse", pwd="datawarehouse")
# 
# variables_relevantes <- c('codigo_oc',
#                           'fecha_oc',
#                           'id_convenio',
#                           'convenio',
#                           'id_producto',
#                           'producto',
#                           'precio_transado',
#                           'precio_referencia',
#                           'monto_ahorro_item')
# 
# first_storage_date <- min(as.Date(savings$FechaOC))  
# last_storage_date  <- max(as.Date(savings$FechaOC))  
# 
# # de aqui se vuelve a ejecutar
# date_checking <- RODBC::sqlQuery(estudios,
#                                  paste0(
#                                  "
#                                  SELECT DISTINCT codigo_oc
#                                  FROM Estudios.dbo.ahorro_oficial_cm 
#                                  WHERE MIN(fecha_oc) = ",first_storage_date,"
#                                  AND MAX(fecha_oc) = ",last_storage_date,""
#                                  )
#                                  )
# 
# outlier_overpriced <- monthly_savings %>% 
#   filter(ahorro_item < 0 & convenio == 'Alimentos RM' & year(fecha) == year(report_date) & month(fecha) == month(report_date)) %>% 
#   group_by(id_producto,producto,convenio) %>% 
#   summarise(
#     'precio minimo transado' = min(precio_unitario),
#     'precio maximo transado' = max(precio_unitario),
#     'precio capturado promedio' = mean(precio_capturado_prom),
#     'Total Transado' = sum(monto_ahorro_item),
#     'ahorro promedio' = weighted.mean(ahorro_item,monto_total_item)) %>% 
#   arrange(`ahorro promedio`)
# 
# lista_overpriced <- list('productos' = outlier_overpriced,
#                          'cotizaciones' = market_prices %>% 
#                            filter(id_producto %in% outlier_overpriced$id_producto) %>%
#                            arrange(producto)
# )
# 
# writexl::write_xlsx(lista_overpriced,paste0('./savings_cm/output/overpriced/sobreprecio_',import_date_numeric,'.xlsx'))


# --------- reporting savings results --------- #

writexl::write_xlsx(savings,
                    paste0('./savings_cm/output/backup/transaction_',import_date_numeric,'.xlsx'))




