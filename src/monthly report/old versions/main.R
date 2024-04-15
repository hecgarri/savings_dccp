# script que calcula el ahorro semanal de los convenios monitoreados
rm(list=ls())

# librerias
library(dplyr)
library(lubridate)
library(stringr)
library(progress)


# directorios
setwd('C:/Users/javier.guajardo/Documents/GitHub/eficiencia_nuevo_modelo_cm')

format_number <- function(x, digit,is_percent = FALSE) {
  x_new = format(x, digits = digit, big.mark = '.', decimal.mark = ',', scientific = FALSE)
  
  if (is_percent == FALSE) {
    x_new = paste0('$',x_new)
  } else {
    x_new = paste0(x_new,'%')
  }
  return(x_new)
}

# --------- processing market prices --------- #

source('./savings_cm/source code/data_processing.R')

# --------- importing market prices --------- #

#source('./savings_cm/source code/import.R')

# -------- processing market prices --------- #

#source('./savings_cm/source code/validate.R')

# -------- storing market prices ------------ #



# -------- extracting cm prices ------------- #

source('./savings_cm/source code/extract_weekly.R')

# -------- getting savings ------------------ #

source('./savings_cm/source code/get_savings.R')


savings %>%
  filter(es_80p == 0) %>%
  group_by(no_monitoreado = ifelse(is.na(precio_capturado_prom),1,0),convenio) %>%
  summarise(productos = n_distinct(IDProductoCM))

# tabla resumen con productos no monitoreados
not_monitoring <- savings %>%
  filter(es_80p != 0 & is.na(precio_capturado_prom)) %>%
  group_by(IDProductoCM) %>%
  summarise(monto_transado = sum(MontoTotal_Item)) %>%
  arrange(desc(monto_transado)) %>%
  mutate(particip = monto_transado / sum(monto_transado),
         acum = cumsum(particip))

not_monitoring <- merge(not_monitoring,
                                   most_value_prod %>% select(id_producto,producto,convenio,es_80p),
                                   by.x = 'IDProductoCM', by.y = 'id_producto',
                                   all.x = TRUE)

not_monitoring %>%
  select(IDProductoCM,producto,monto_transado,acum,es_80p) %>%
  arrange(acum) %>%
  head(10)

# no encontrados en ferreteria
not_found_ferr <- savings %>% 
  filter(id_licitacion == '2239-2-LR18' & is.na(precio_capturado_prom)) %>%
  select(IDProductoCM) %>% unique()

tabla_ferr <- transaction %>% 
  filter(IDProductoCM %in% not_found_ferr$IDProductoCM) %>% 
  group_by(IDProductoCM,NombreProducto) %>% 
  summarise(monto = sum(MontoTotal_Item)) %>% arrange(desc(monto))


# participacon por categoria de producto (80 -20)
savings %>% 
  group_by(id_licitacion,es_80p) %>% 
  summarise(monto = sum(MontoTotal_Item, na.rm = TRUE)) %>% 
  mutate(total = sum(monto), particip = monto /total)

# distribucion de ahorros
lim_inf <- -10  #readline(prompt = "Ingrese un límite inferior para el desahorro: "); print(lim_inf)
lim_sup <- 1   #readline(prompt = "Ingrese un límite inferior para el ahorro: "); print(lim_sup)

#lim_inf <- as.numeric(lim_inf)
#lim_sup <- as.numeric(lim_sup)
library(ggplot2)
savings %>%
  filter(ahorro_item > lim_inf & ahorro_item < lim_sup) %>%
  ggplot(aes(x = ahorro_item, color = convenio, fill = convenio)) +
  geom_density(alpha = 0.5) +
  geom_vline(xintercept = 0, linetype = 'dashed', color = 'red') +
  labs(title = 'Distribucion de ahorro por convenio') +
  scale_x_continuous(labels = scales::percent) +
  theme_bw()

# productos con desahorro
savings %>% filter(ahorro_item < 0) %>% 
  group_by(IDProductoCM,producto,convenio) %>% 
  summarise(
    precio_minimo_transado = min(precio_unitario),
    precio_maximo_transado = max(precio_unitario),
    precio_capturado_prom = mean(precio_capturado_prom),
    ahorro_promedio = weighted.mean(ahorro_item,MontoTotal_Item)) %>% 
  arrange(ahorro_promedio)

# ------------- tablas de resultado --------------- #

message('=======================')
message('calculamos resultados finales')

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
                                                     id_licitacion == '2239-17-LR20' ~ 'Escritorio RM',
                                                     id_licitacion == '2239-20-LR20' ~ 'Computadores',
                                                     id_licitacion == '2239-2-LR18'  ~ 'Ferreteria',
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
               savings_final %>% summarise(total_ahorro = format_number(sum(monto_ahorro_item), digit = 0,is_percent = FALSE)),
             ' pesos, con una cobertura agregada del ',
             summary_table %>% summarise(cobertura_monto_total = format_number(sum(total_monitoreado) / sum(total_transado) * 100,
                                                                               digit = 0, is_percent = TRUE)),'.'
             )
        )


# --------- getting overpriced --------- #

message('=======================')
message('Outlier de desahorro')
message('=======================')
outlier_overpriced <- savings %>% 
  filter(ahorro_item < 0) %>% 
  group_by(IDProductoCM,producto,convenio) %>% 
  summarise(
    'precio minimo transado' = min(precio_unitario),
    'precio maximo transado' = max(precio_unitario),
    'precio capturado promedio' = mean(precio_capturado_prom),
    'ahorro promedio' = weighted.mean(ahorro_item,MontoTotal_Item)) %>% 
  arrange(`ahorro promedio`)

lista_overpriced <- list('productos' = outlier_overpriced,
                         'cotizaciones' = market_prices %>% 
                           filter(id_producto %in% outlier_overpriced$IDProductoCM) %>%
                           select(-sd_prom,-revisar) %>%
                           arrange(producto)
                           )
  
writexl::write_xlsx(lista_overpriced,paste0('./savings_cm/output/overpriced/sobreprecio_',import_date_numeric,'.xlsx'))

lim_sup_rev <- 0.6
message('=======================')
message('Outlier de ahorro')
message('=======================')
outlier_savings <- savings %>% 
  filter(ahorro_item > lim_sup_rev) %>%
  #select(IDProductoCM,producto,precio_capturado_prom,precio_unitario,ahorro_item) %>%
  group_by(IDProductoCM,producto,convenio) %>%
    summarise(
      'precio minimo transado' = min(precio_unitario),
      'precio maximo transado' = max(precio_unitario),
      'precio capturado promedio' = mean(precio_capturado_prom),
      'ahorro promedio' = weighted.mean(ahorro_item,MontoTotal_Item)) %>% 
      arrange(`ahorro promedio`)

writexl::write_xlsx(outlier_savings,paste0('./savings_cm/output/overpriced/outlier_ahorro_',import_date_numeric,'.xlsx'))

# --------- storing savings --------- #

writexl::write_xlsx(
  savings_final %>% filter(!is.na(precio_unitario)), 
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

# --------- reporting savings results --------- #




