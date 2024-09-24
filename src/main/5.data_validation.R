rm(list = ls())

# librerias
library(dplyr)
library(lubridate)
library(stringr)
library(progress)
library(ggplot2)
library(DBI)
library(RODBC)

# importamos el archivo
files      <- list.files(path="./data/shopeo")
main_files <- files[str_detect(str_to_lower(files),"reportepreciossemanal")  & 
                      !str_detect(str_to_lower(files),"~")]
market_prices <- lapply(main_files, 
                        function(files) 
                          readxl::read_xlsx(paste0('./data/shopeo/',files)) %>% 
                          mutate(file = files)) %>% 
  data.table::rbindlist() %>% 
  mutate(fecha_reporte = str_extract(file, "_(\\d{2}-\\d{2}-\\d{4})\\.xlsx$"))  %>%
  mutate(fecha_reporte = str_remove(fecha_reporte, "^_"))  %>%
  mutate(fecha_reporte = str_replace(fecha_reporte, "\\.xlsx$", ""))

# message(paste0('====> se importa el archivo ',main_files,' actualizado el ',format(import_date,'%d-%m%-%Y')),
#         ' y procesado el ',format(today,'%d-%m%-%Y'))


weekly_data <- market_prices %>% 
  mutate(fecha_reporte = as.Date(fecha_reporte, format = "%d-%m-%Y")) %>%
  distinct(CodigoMC,.keep_all = TRUE) %>% 
  group_by(fecha_reporte, Categoria) %>% 
  count() %>% 
  arrange(fecha_reporte)


market_prices %>% 
  mutate(fecha_reporte = as.Date(fecha_reporte, format = "%d-%m-%Y")) %>% 
  group_by(fecha_reporte, Categoria) %>% 
  count() %>% 
  arrange(fecha_reporte) %>% 
  group_by(Categoria) %>% 
  count()


writexl::write_xlsx(weekly_data,"./data/shopeo/20240717_cobertura_productos.xlsx")