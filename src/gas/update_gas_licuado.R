rm(list = ls())

library(DBI)
library(RODBC)
library(data.table)
library(tidyverse)
library(readr)


establecer_conexion <- function(opcion) {
  switch(opcion,
         "dw" = dbConnect(odbc::odbc(), 
                          Driver = "ODBC Driver 17 for SQL Server", 
                          Server = "10.34.71.202",
                          database = "Estudios",
                          UID = "datawarehouse", 
                          PWD = "datawarehouse"),
         "aq" = dbConnect(odbc::odbc(), 
                          Driver = "ODBC Driver 17 for SQL Server", 
                          Server = "10.34.71.146\\AQUILES_CONSULTA", 
                          UID = "datawarehouse", 
                          PWD = "datawarehouse"),
         # Agregar mÃ¡s casos segÃºn sea necesario
         stop("OpciÃ³n de conexiÃ³n no vÃ¡lida.")
  )
}

# FunciÃ³n para cerrar conexiÃ³n
cerrar_conexion <- function(con) {
  dbDisconnect(con)
}

conn <- establecer_conexion("dw")


data_2024_1 = readxl::read_excel("C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/gas/hist_precios_call_center_jun_2024.xlsx")

data_2024_2 = read_csv2("C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/gas/hist_precios_call_center_jul_2024.csv")

data_2024 <- dplyr::anti_join(data_2024_2, data_2024_1, by = "emga_id")


# Guardar el data frame en la base de datos
dbWriteTable(conn, "GasLicuadoHist_callcenter",data_2024, row.names = FALSE, append = TRUE)



