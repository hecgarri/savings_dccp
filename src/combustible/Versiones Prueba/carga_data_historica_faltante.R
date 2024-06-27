rm(list = ls())

library(odbc)
library(DBI)
library(tidyverse)


con4 <- dbConnect(odbc()
                  , Driver = "ODBC Driver 17 for SQL Server"
                  , Server = "10.34.71.202"
                  , UID = "datawarehouse"
                  , PWD = "datawarehouse")


#Elimina datos de la base de datos considerados incorrectos: 
#


# Elimina registros incorrectos de la base de datos de combustibles =============

# # Define la fecha límite
# fecha_limite <- "2023-07-01"  # Cambia esta fecha a la que desees
# 
# # Construye la consulta SQL para eliminar datos basados en la fecha
# elimina_data_93 <- paste("
#                   DELETE 
#                   FROM Estudios.dbo.bencina93Hist 
#                   WHERE fecha_inicio >= '", fecha_limite, "'", sep = "")
# 
# # Ejecuta la consulta
# dbExecute(con4, elimina_data_93)


# # # Define la fecha límite
#  fecha_limite <- "2023-07-01"  # Cambia esta fecha a la que desees
# #
# # # Construye la consulta SQL para eliminar datos basados en la fecha
#  elimina_data_95 <- paste("
#                    DELETE
#                    FROM Estudios.dbo.bencina95Hist
#                    WHERE fecha_inicio >= '", fecha_limite, "'", sep = "")
# 
# # # Ejecuta la consulta
#  dbExecute(con4, elimina_data_95)


# # Define la fecha límite
#  fecha_limite <- "2023-07-01"  # Cambia esta fecha a la que desees
# # 
# # # Construye la consulta SQL para eliminar datos basados en la fecha
#  elimina_data_97 <- paste("
#                    DELETE 
#                    FROM Estudios.dbo.bencina97Hist 
#                    WHERE fecha_inicio >= '", fecha_limite, "'", sep = "")
# # 
# # # Ejecuta la consulta
#  dbExecute(con4, elimina_data_97)


# # # Define la fecha límite
#  fecha_limite <- "2023-07-01"  # Cambia esta fecha a la que desees
# # 
# # # Construye la consulta SQL para eliminar datos basados en la fecha
#  elimina_data_di <- paste("
#                    DELETE 
#                    FROM Estudios.dbo.DieselHist 
#                    WHERE fecha_inicio >= '", fecha_limite, "'", sep = "")
#  
# # # Ejecuta la consulta
#  dbExecute(con4, elimina_data_di)
#  

# Carga datos históricos desde energía abierta (csv) =====================
# 

# Cierra la conexión ==========
dbDisconnect(con4)





data_hist = data.table::fread(file =paste0("C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/",
                                           "savings_dccp/data/combustible/precios por estacion/",
                                           "energia_abierta/2023.csv/2023.csv"), sep = ";")




estudios <- dbConnect(odbc::odbc()
                      , Driver = "ODBC Driver 17 for SQL Server"
                      , Server = "10.34.71.202"
                      , UID = "datawarehouse"
                      , PWD = "datawarehouse" 
                      ,Database = "Estudios")

data_93 = data_hist %>% 
  select(id, fecha_inicio = fecha_actualizacion, precio, tipo = combustible) %>% 
  filter(fecha_inicio>='2023-07-01'
         ,tipo == 'Gasolina 93') %>% 
  mutate(fecha_fin = Sys.Date()
         ,tipo = 'Bencina93') %>% 
  select(id, fecha_inicio, fecha_fin, precio, tipo)

tabla = 'dbo.Bencina93Hist'

dbWriteTable(estudios, tabla, data_93, append = TRUE)








dbDisconnect(con4)