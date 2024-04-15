#rm(list=ls())

# librerias
library(dplyr)
library(lubridate)
library(ggplot2)

# ======================================================== #
# parametros

#report_date <- as.Date() # fecha del reporte
#floor_date(max(monthly_savings$FechaOC),'month')
mes_completo  <- TRUE # TRUE si el mes esta cerrado
genera_dipres <- FALSE # TRUE si queremos generar el archivos a la dipres



# ======================================================== #

# directorio
setwd('C:/o/OneDrive - DCCP/Traspaso/2. Cálculo de Ahorro')
myconn <- RODBC::odbcConnect("DW_new", uid="datawarehouse" , pwd="datawarehouse")

savings_all <- readRDS("./data/ahorros_historicos.rds")
message('1. Iniciamos consolidacion de los ahorros')
message('Importando resultados semanales')
message('Cargando.......')

# importamos datos de ahorro para combustibles
source('./src/monthly report/group_savings.R')

# agregamos el tipo de cambio para transformar montos
monthly_savings <- merge(monthly_savings %>% mutate(Anio = year(FechaOC), Mes = month(FechaOC)),
                         tipo_de_cambio,by.x = c('Anio','Mes'), by.y = c('YEAR','MONTH'),
                         all.x = TRUE)

monthly_savings$monto_ahorro_item_usd <- monthly_savings$monto_ahorro / monthly_savings$tipo_de_cambio
monthly_savings$monto_total_item_usd  <- monthly_savings$MontoTotal_Item / monthly_savings$tipo_de_cambio

# podemos revisar el resultado a la fecha de consulta o el periodo completo

if (mes_completo) 
{#source('./ahorro/rfiles/savings test/extract_monthly_test.R')
 source('./src/monthly report/extract_monthly_backup.R')
  } else {
  source('./src/monthly report/old versions/extract_monthly.R')
  }

#tabla resumen con el ultimo mes creado
var_dccp <- c('fecha','convenio','id_producto','monto_total_item','ahorro_item','monto_ahorro_item')

names(monthly_savings)[names(monthly_savings)=='FechaOC']         <- 'fecha'
names(monthly_savings)[names(monthly_savings)=='IDProductoCM']    <- 'id_producto'
names(monthly_savings)[names(monthly_savings)=='MontoTotal_Item'] <- 'monto_total_item'

total_amount_cm <- bind_rows(total_amount_cm,
                             total_monitored_comb,
                             total_monitored_gas)

summary_table <- merge(monthly_savings %>%
                         filter(year(fecha) == year(report_date) & month(fecha) == month(report_date) & ahorro_item < 0.8 & ahorro_item > -2) %>%
                         group_by(convenio) %>%
                         summarise(ahorro_promedio = weighted.mean(ahorro_item,monto_total_item),
                                   productos = n_distinct(id_producto),
                                   total_ahorro = sum(monto_ahorro_item, na.rm = TRUE),
                                   total_monitoreado = sum(monto_total_item)),
                       total_amount_cm %>% filter(year(fecha) == year(report_date) & month(fecha) == month(report_date)),
                       by = 'convenio',
                       all.x = TRUE)

writexl::write_xlsx(summary_table,'./output/tabla_mes_final.xlsx')

message('===========================================')
message('         tabla resumen del mes             ')
message('===========================================')
summary_table %>%
  mutate(cobertura_monto = total_monitoreado / monto_transado * 100) %>%
  select(convenio,ahorro_promedio,productos,total_ahorro,cobertura_monto) %>%
  arrange(desc(total_ahorro))

# distribucion de ahorros por convenio
monthly_savings %>%
  filter(year(fecha) == year(report_date) & month(fecha) == month(report_date)) %>%
  ggplot(aes(x = ahorro_item, color = convenio, fill = convenio)) +
  geom_density(alpha = 0.5) +
  geom_vline(xintercept = 0, linetype = 'dashed', color = 'red') +
  labs(title = 'Distribucion de ahorro por convenio') +
  scale_x_continuous(labels = scales::percent) +
  theme_bw()

# productos no monitoreados
monthly_savings %>% 
  group_by(convenio,is_na = ifelse(is.na(precio_capturado_prom),1,0),es_80p_gen) %>% 
  summarise(productos = n_distinct(id_producto))

# generamos reporte dipres
if (genera_dipres) {
  source('./ahorro/rfiles/savings test/report_dipres.R')
}


