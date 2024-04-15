# Obtenemos los ahorros historicos que sirven de inputs para distintos procesos

# librerias
library(dplyr)
library(lubridate)

message('Cargando historico de ahorros.......')

# directorio 1
#setwd("C:/Users/javier.guajardo/Documents/GitHub/eficiencia_nuevo_modelo_cm/ahorro/outputs")

# ----------- tipo de cambio -------------- #

myconn <- RODBC::odbcConnect("DW_new", uid="datawarehouse" , pwd="datawarehouse")

tipo_de_cambio <- RODBC::sqlQuery(myconn,"SELECT 
	moneda.YEAR,
	moneda.MONTH,
	moneda.VMCLP AS tipo_de_cambio
FROM DPA..paridadmoneda as moneda
WHERE moneda.MONEDA = 'USD'
")

# ------------ Datos de ahorro --------------- #

# buscamos en la carpeta 
files_1 <- list.files(path="./output/historico")
files_2 <- list.files(path="./output/historico/savings oficial")
filesAseo <- files_1[stringr::str_detect(files_1,"Ahorro") == TRUE & stringr::str_detect(files_1,"Aseo") == TRUE]
filesFerr <- files_1[stringr::str_detect(files_1,"Ahorro") == TRUE & stringr::str_detect(files_1,"Ferreteria") == TRUE]
filesComp <- files_1[stringr::str_detect(files_1,"Ahorro") == TRUE & stringr::str_detect(files_1,"Computadores") == TRUE]
filesEscr <- files_1[stringr::str_detect(files_1,"Ahorro") == TRUE & stringr::str_detect(files_1,"Escritorio") == TRUE]
filesComb <- files_1[stringr::str_detect(files_1,"Ahorro") == TRUE & stringr::str_detect(files_1,"Combustible") == TRUE]
filesAlim <- files_1[stringr::str_detect(files_1,"Ahorro") == TRUE & stringr::str_detect(files_1,"Alimentos") == TRUE]
filesNmet <- files_2[stringr::str_detect(files_2,"ahorro") == TRUE]

# importamos aseo
Ahorro_Aseo <- data.frame()
for (i in 1:length(filesAseo)) {
  temp_data <- readxl::read_xlsx(paste0("./output/historico/",filesAseo[i]))
  temp_data$FechaOC <- as.Date(temp_data$FechaOC)
  Ahorro_Aseo <- rbind(Ahorro_Aseo,temp_data)
} 

# importamos ferreteria
Ahorro_Ferr <- data.frame()
for (i in 1:length(filesFerr)) {
  temp_data <- readxl::read_xlsx(paste0("./output/historico/",filesFerr[i]))
  Ahorro_Ferr <- rbind(Ahorro_Ferr,temp_data)
} 

# importamos alimentos
Ahorro_Alim <- data.frame()
for (i in 1:length(filesAlim)) {
  temp_data <- readxl::read_xlsx(paste0("./output/historico/",filesAlim[i]))
  temp_data$FechaOC <- as.Date(temp_data$FechaOC)
  Ahorro_Alim <- rbind(Ahorro_Alim,temp_data)
} 

# importamos computadores
Ahorro_Comp <- data.frame()
for (i in 1:length(filesComp)) {
  temp_data <- readxl::read_xlsx(paste0("./output/historico/",filesComp[i]))
  Ahorro_Comp <- rbind(Ahorro_Comp,temp_data)
} 

Ahorro_Comp$PrecioUnit_Bruto <- Ahorro_Comp$PrecioUnit_NETO_CLP
Ahorro_Comp$FechaOC <- as.Date(Ahorro_Comp$FechaOC)

# importamos escritorio
Ahorro_Escr <- data.frame()
for (i in 1:length(filesEscr)) {
  temp_data <- readxl::read_xlsx(paste0("./output/historico/",filesEscr[i]))
  Ahorro_Escr <- rbind(Ahorro_Escr,temp_data)
} 

# generamos una var que indique si es transaccion de la rm
Ahorro_Escr <- Ahorro_Escr %>%
  mutate(
    producto_rm = if_else(stringr::str_detect(NombreProducto,'MACROZONA RM'),1,0))

Ahorro_Escr <- Ahorro_Escr[Ahorro_Escr$producto_rm == 1,]

# importamos combustibles
Ahorro_Comb <- data.frame()
for (i in 1:length(filesComb)) {
  temp_data <- readxl::read_xlsx(paste0("./output/historico/",filesComb[i]))
  Ahorro_Comb <- rbind(Ahorro_Comb,temp_data)
}

Ahorro_Comb <- Ahorro_Comb %>%
  mutate(id_transaccion = row_number()) %>%
  select(poiNroLicitacionPublica,
         FechaOC=fecha,
         CodigoOC = id_transaccion,
         IDProductoCM = codigo_producto,
         NombreProducto = tipo_producto,
         MontoTotal_Item = monto_pagado,
         Cantidad_Item = litros,
         PrecioUnit_Bruto = precio_pagado,
         CotProm = precio_estacion,
         Ahorro = ahorro_DCCP,
         MontoAhorro = ahorro_por_transaccion)

Ahorro_nueva_metodologia <- data.frame()
for (i in 1:length(filesNmet)) {
  temp_data <- readxl::read_xlsx(paste0("./output/historico/savings oficial/",filesNmet[i]))
  temp_data <- temp_data %>% select(IDProductoCM,
                                    poiNroLicitacionPublica = id_licitacion,
                                    NombreProducto = producto,
                                    CodigoOC,
                                    FechaOC,
                                    Cantidad_Item,
                                    MontoTotal_Item,
                                    PrecioUnit_Bruto = precio_unitario,
                                    CotProm = precio_capturado_prom,
                                    Ahorro = ahorro_item,
                                    MontoAhorro = monto_ahorro_item)
  Ahorro_nueva_metodologia <- rbind(Ahorro_nueva_metodologia,temp_data)
}

# Calculamos el ahorro 
variables <- c(
               'IDProductoCM',
               'NombreProducto',
               'poiNroLicitacionPublica',
               'CodigoOC',
               'FechaOC',
               'Cantidad_Item',
               'MontoTotal_Item',
               'PrecioUnit_Bruto',
               'CotProm',
               'Ahorro',
               'MontoAhorro')

Ahorro <- rbind(Ahorro_Comp %>% select(all_of(variables)),
                Ahorro_Aseo %>% select(all_of(variables)),
                Ahorro_Alim %>% select(all_of(variables)),
                Ahorro_Ferr %>% filter(IDProductoCM != 1634294) %>% select(all_of(variables)), # no se considera el producto overol portwest 
                Ahorro_Escr %>% select(all_of(variables)),
                Ahorro_Comb %>% filter(is.na(CotProm)==FALSE) %>% select(all_of(variables)),
                Ahorro_nueva_metodologia %>% filter(is.na(CotProm)==FALSE) %>% select(all_of(variables))
)  

# Generamos nuevas variables
Ahorro <- Ahorro %>%
  mutate(Convenio = 
           case_when(
             poiNroLicitacionPublica == "2239-5-LR19" ~ "Aseo",
             poiNroLicitacionPublica == "2239-2-LR18" ~ "Ferreteria",
             poiNroLicitacionPublica == "2239-6-LR19" ~ "Computadores",
             poiNroLicitacionPublica == "2239-20-LR20" ~ "Computadores",
             poiNroLicitacionPublica == "2239-10-LR21" ~ "Computadores",
             poiNroLicitacionPublica == "2239-17-LR20" ~ "Escritorio RM",
             poiNroLicitacionPublica == "2239-6-LR20" ~ "Combustible",
             poiNroLicitacionPublica == "2239-7-LR17" ~ "Alimentos RM",
             TRUE ~ "Otros"),
         Año = lubridate::year(FechaOC),
         Mes = lubridate::month(FechaOC)) 

# unimos el tipo de cambio
Ahorro <- merge(Ahorro,tipo_de_cambio,by.x = c('Año','Mes'), by.y = c('YEAR','MONTH'))

# transformamos a USD los montos
Ahorro <- Ahorro %>% 
  mutate(
    monto_ahorro_usd = MontoAhorro/tipo_de_cambio, 
    monto_total_item_usd = if_else(Convenio == 'Computadores' & FechaOC < '2021-08-01',MontoTotal_Item,MontoTotal_Item/tipo_de_cambio))

# Ahorro total
Ahorro <- Ahorro %>%
  group_by(Convenio,fecha = lubridate::floor_date(FechaOC,'month')) %>%
  mutate(PesoTrx = MontoTotal_Item / sum(MontoTotal_Item),
         AhorroPond = Ahorro*PesoTrx)

savings_all <- Ahorro#[Ahorro$fecha<report_date,]

rm(list = c("Ahorro_Ferr",
            "Ahorro_Aseo",
            "Ahorro_Alim",
            "Ahorro_Comp",
            "Ahorro_Comb",
            "Ahorro_Escr",
            "Ahorro_nueva_metodologia",
            "i","files_1","files_2",
            "filesAseo",
            "filesFerr",
            "filesAlim",
            "filesComb",
            "filesComp",
            "filesEscr",
            "variables",
            "myconn",
            "temp_data"))

saveRDS(savings_all,'./data/ahorros_historicos.rds')





