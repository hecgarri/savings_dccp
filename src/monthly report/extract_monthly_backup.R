library(dplyr)
library(stringr)

message('Importando resultados semanales y mensuales')
message('Cargando ahorros.......')

#anio <- year(report_date)
#mes  <- month(report_date)
#fecha_num <- paste0(anio,ifelse(mes<10,paste0(0,mes),mes))

# carpeta con los resultados semanales
all_files <- list.files(path="./output/backup")
month_files <- all_files#[str_detect(all_files,fecha_num)]

# importamos datos de ahorro 
monthly_transaction <- data.frame()

# convenios generales
for (file in month_files) {
  temp_data <- readxl::read_xlsx(paste0("./output/backup/",file))
  monthly_transaction <- rbind(monthly_transaction,temp_data)
} 

# resumimos las transacciones por convenio y mes
total_amount_cm <- monthly_transaction %>%
  mutate(convenio = case_when(id_licitacion == "2239-5-LR19" ~ "Aseo",
                              id_licitacion == "2239-9-LR22" ~ "Aseo",
                              id_licitacion == "2239-2-LR18" ~ "Ferreteria",
                              id_licitacion == "2239-20-LR20" ~ "Computadores",
                              id_licitacion == "2239-10-LR21" ~ "Computadores",
                              id_licitacion == "2239-17-LR22" ~ "Computadores",
                              id_licitacion == "2239-17-LR20" ~ "Escritorio RM",
                              id_licitacion == "2239-6-LR20" ~ "Combustible",
                              id_licitacion == "2239-7-LR17" ~ "Alimentos RM",
                              id_licitacion == "2239-13-LR21" ~ "Insumos Salud",
                              TRUE ~ 'Otro')) %>%
  #group_by(convenio,fecha = make_date(anio,mes,01)) %>%
  group_by(convenio,fecha = floor_date(FechaOC,'month')) %>%
  summarise(monto_transado = sum(MontoTotal_Item)) %>%
  arrange(convenio,fecha)

rm(list=c('all_files',
          'month_files',
          'file'))
