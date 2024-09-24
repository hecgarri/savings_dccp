#Quita archivos del WorkSpace ===========================================================
#
rm(list = ls())

#Fija el directorio de trabajo ==========================================================
#

wd_path = "C:/o/DCCP/Javier Guajardo - Traspaso/2. Cálculo de Ahorro/savings_dccp/data/productos 80-20"


setwd(wd_path)

#Carga de paquetes necesarios para el análisis ==========================================
#
load_pkg <- function(pack){
  create.pkg <- pack[!(pack %in% installed.packages()[, "Package"])]
  if (length(create.pkg))
    install.packages(create.pkg, dependencies = TRUE)
  sapply(pack, require, character.only = TRUE)
}

packages = c("tidyverse" #Conjunto integral de paquetes para manipular y analizar datos de manera coherente y eficiente.
             ,"DBI"
             , "RODBC" #facilita la conexión y manipulación de bases de datos a través de ODBC (Open Database Connectivity).
             , "plotly" #proporciona herramientas interactivas para la creación de gráficos dinámicos y visualizaciones interactivas
             , "data.table" #Paquete optimizado para manipulación eficiente de grandes conjuntos de datos, destacando por su velocidad y funcionalidades avanzadas.
             , "formattable"
             , "hutils"
             , "readr"
             , "VennDiagram"
             , "RColorBrewer"
             ,"openxlsx")

load_pkg(packages)

# #Establece conexiones a los diferentes servidores =======================================
# 

# Crea funciones para conectarse a la base de datos ===============================

establecer_conexion <- function(opcion) {
  switch(opcion,
         "dw" = dbConnect(odbc::odbc(), 
                          Driver = "ODBC Driver 17 for SQL Server", 
                          Server = "10.34.71.202", 
                          UID = "datawarehouse", 
                          PWD = "datawarehouse"),
         "aq" = dbConnect(odbc::odbc(), 
                          Driver = "ODBC Driver 17 for SQL Server", 
                          Server = "10.34.71.146\\AQUILES_CONSULTA", 
                          UID = "datawarehouse", 
                          PWD = "datawarehouse"),
         # Agregar mÃ¡s casos segÃºn sea necesario
         stop("Opción de conexión no válida.")
  )
}

# FunciÃ³n para cerrar conexiÃ³n
cerrar_conexion <- function(con) {
  dbDisconnect(con)
}

# #con = RODBC::odbcConnect("aquiles", uid = "datawarehouse", pwd = "datawarehouse") #TIVIT

con2 = establecer_conexion("aq") #Aquiles

con3 = establecer_conexion("dw") #Datawarehouse

grilla <- rbind(expand.grid(1:12,2023:2024))
grilla <- grilla[19,]
#grilla <- grilla[17,]


productos <- apply(grilla,1,function(row) dbGetQuery(con3, 
                                               paste0("
DECLARE @MONTH AS INT;
DECLARE @YEAR AS INT;
DECLARE @Window AS INT;

SET @MONTH = ",row[1],";
SET @YEAR = ",row[2],";
SET @Window = -6;

DECLARE @CURRENTMONTH datetime = DATETIMEFROMPARTS(@YEAR, @MONTH, 1, 0, 0, 0, 0);
DECLARE @startDate datetime = DATEADD(month, @Window, @CURRENTMONTH);
DECLARE @endDate datetime = DATEADD(month, 0, @CURRENTMONTH);

WITH TotalPR AS (
    SELECT
        DC.IdConvenioMarco,
        DC.NombreCM,
        PR.IdProductoCM,
        SUM(OL.MontoCLPsinIVA) AS MontoCLPsinIVA
    FROM [DM_Tienda].[dbo].[THOrdenesCompraLinea] AS OL
    INNER JOIN DM_Tienda.dbo.DimConvenioMarco AS DC ON OL.IdConvenioMarco = DC.IdConvenioMarco
    INNER JOIN DM_Tienda.dbo.DimProducto AS PR ON OL.IdProductoCM = PR.IdProductoCM
    INNER JOIN DM_Tienda.dbo.DimTiempo AS TP ON OL.IDFechaEnvioOC = TP.DateKey
    WHERE TP.Date >= CAST(@startDate AS DATE) AND TP.Date <= CAST(@endDate AS DATE)
    GROUP BY DC.IdConvenioMarco, DC.NombreCM, PR.IdProductoCM
),
TotalCM AS (
    SELECT 
        CAST(@endDate AS DATE) AS [Período],
        TC.IdConvenioMarco,
        TC.NombreCM,
        TC.IdProductoCM,
        TC.MontoCLPsinIVA,
        SUM(TC.MontoCLPsinIVA) OVER (PARTITION BY TC.IdConvenioMarco ORDER BY TC.MontoCLPsinIVA DESC) AS Suma_Acumulada,
        SUM(TC.MontoCLPsinIVA) OVER (PARTITION BY TC.IdConvenioMarco) AS Suma_Total,
		TC.MontoCLPsinIVA/SUM(TC.MontoCLPsinIVA) OVER (PARTITION BY TC.IdConvenioMarco) AS Pct,
        SUM(TC.MontoCLPsinIVA) OVER (PARTITION BY TC.IdConvenioMarco ORDER BY TC.MontoCLPsinIVA DESC) * 1.0 / SUM(TC.MontoCLPsinIVA) OVER (PARTITION BY TC.IdConvenioMarco) AS Pct_Acumulado
    FROM TotalPR AS TC
)
SELECT 
    TCM.Período,
    TCM.IdConvenioMarco,
    TCM.NombreCM,
    TCM.IdProductoCM as [id_producto],
    TCM.MontoCLPsinIVA,
    TCM.Suma_Acumulada,
    TCM.Suma_Total,
	TCM.Pct,
    TCM.Pct_Acumulado,
    CASE 
        WHEN TCM.Pct_Acumulado <= 0.80 THEN 1
		    WHEN TCM.Suma_Acumulada - TCM.MontoCLPsinIVA <= 0.80 * TCM.Suma_Total THEN 1
        ELSE 0
    END AS es_80p_gen
FROM TotalCM AS TCM
ORDER BY 
    TCM.IdConvenioMarco ASC,
    TCM.MontoCLPsinIVA DESC;
"))) %>% data.table::rbindlist() 
  
guardar_80_20 <- function(sheetName, data, file){
  wb <- createWorkbook()
  
  addWorksheet(wb, sheetName = sheetName)
  
  writeData(wb, sheet = sheetName, x = data)
  
  saveWorkbook(wb, file, overwrite = TRUE)  
}

periodos <- unique(productos$Período)

# Aplica la función a cada grupo
productos %>% 
  group_by(`Período`) %>% 
  group_split() %>% 
  imap(~ {
    sheet_name <- paste0("data", periodos[.y])
    guardar_80_20(sheetName = sheet_name, data = .x, file = paste0(periodos[.y]," productos 80-20.xlsx"))
  })
  
