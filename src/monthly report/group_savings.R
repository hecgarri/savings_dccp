library(dplyr)
library(stringr)

message('Importando resultados semanales y mensuales')
message('Cargando ahorros.......')


# carpeta con los resultados semanales
all_files <- list.files(path="./output/weekly savings")

comb_files <- all_files[str_detect(all_files,'combustible')]
gas_files  <- all_files[str_detect(all_files,'gas')]
rest_files <- all_files[!str_detect(all_files,'combustible') & !str_detect(all_files,'gas')]

# importamos datos de ahorro 
monthly_savings_ini <- data.frame()

# convenios generales
for (file in rest_files) {
  temp_data <- readxl::read_xlsx(paste0("./output/weekly savings/",file))
  temp_data$FechaOC <- as.Date(temp_data$FechaOC)
  monthly_savings_ini   <- rbind(monthly_savings_ini,temp_data)
} 

# convenio de combustibles
comb_monthly_savings <- data.frame()
for (file in comb_files) {
  temp_data <- readxl::read_xlsx(paste0("./output/weekly savings/",file))
  temp_data$FechaOC <- as.Date(temp_data$FechaOC)
  comb_monthly_savings   <- rbind(comb_monthly_savings,temp_data)
}

# convenio de gas
gas_monthly_savings <- data.frame()
for (file in gas_files) {
  temp_data <- readxl::read_xlsx(paste0("./output/weekly savings/",file))
  temp_data$FechaOC <- as.Date(temp_data$FechaOC)
  
  #if (!file %in% c('ahorro_gas_202307.xlsx')) {
  if (file %in% gas_files[1:16]) {
    temp_data <- temp_data %>% select(-nombre_region,-n_cotizaciones) 
  } else if (file %in% gas_files[19:length(gas_files)]) {
    temp_data <- temp_data %>% select(-nombre_region,-idConvenioMarco) 
  }
  gas_monthly_savings   <- rbind(gas_monthly_savings,temp_data)
}

gas_monthly_savings <- gas_monthly_savings %>%
  mutate(producto_rm = 1,
         es_80p_gen = 1)

# unimos convenios generales mas combustibles
monthly_savings <- bind_rows(monthly_savings_ini,
                             comb_monthly_savings %>% filter(!is.na(precio_capturado_prom)),
                             gas_monthly_savings %>%
                               select(IDProductoCM,id_licitacion,CodigoOC,FechaOC,Organismo,Proveedor = proveedor,producto = NombreProducto,
                                      Cantidad_Item,MontoTotal_Item,PrecioUnit_NETO,MonedaOC,producto_rm,precio_capturado_prom,
                                      convenio,precio_unitario,ahorro_item,monto_ahorro_item,es_80p_gen)
                             )

total_monitored_comb <- data.frame('convenio' = 'Combustible',
                                   'fecha' = report_date,
                                   'monto_transado' = sum(comb_monthly_savings$MontoTotal_Item))

total_monitored_comb <- comb_monthly_savings %>%
  group_by(convenio,fecha = floor_date(FechaOC,'month')) %>%
  summarise(monto_transado = sum(MontoTotal_Item))
  
total_monitored_gas <- gas_monthly_savings %>%
  group_by(convenio,fecha = floor_date(FechaOC,'month')) %>%
  summarise(monto_transado = sum(MontoTotal_Item))

rm(list=c('temp_data',
          'monthly_savings_ini',
          'comb_monthly_savings',
          'all_files',
          'comb_files',
          'rest_files',
          'file')
   )


