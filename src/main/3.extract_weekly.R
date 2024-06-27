message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message(' 3) Comienza la extraccion de transacciones en tienda')
message('Cargando..........')

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
         stop("Opción de conexión no válida")
  )
}

# FunciÃ³n para cerrar conexiÃ³n
cerrar_conexion <- function(con) {
  dbDisconnect(con)
}

# conexiones
myconnAQ <- establecer_conexion("aq")

# parametros
if (is_final_week == 1) {
  
  extract_date_ini <- import_date - days(6) - days_left
  extract_date_fin <- import_date   
  
} else if (is_first_week == 1) {
  
  extract_date_ini <- import_date 
  extract_date_fin <- import_date + days_left
  
} else {
  
  extract_date_ini <- import_date - days(6)
  extract_date_fin <- import_date
}

#extract_date_ini <- '2022-02-01'

# # acotamos las transacciones al mes del reporte
# # inicio del mes
# ini_week <- floor_date(import_date,'week', week_start = 1)
# fin_week <- ceiling_date(import_date,'week', week_start = 0)
# 
# if (month(ini_week) != month(fin_week)) {
#   
#   extract_date_ini <- ceiling_date(ini_week,'month')
#     
# }
# 
# # fin del mes

transaction_qry <- paste(
  "
DECLARE 
@FechaActual DATE, 
@FechaAnterior DATE

SET @FechaActual = '",extract_date_fin,"'
SET @FechaAnterior = '",extract_date_ini,"'

;WITH oc_compu AS(
SELECT 
  IT.poiNroLicitacionPublica as id_licitacion
	,porCode AS CodigoOC
  ,CAST(porSendDate as date) AS FechaOC
  ,UPPER(I.entName) AS Organismo
  ,UPPER(PROV.orgLegalName) AS Proveedor
  ,CAST(SUBSTRING(IT.poiDescription,
  CHARINDEX('(',IT.poiDescription)+1,
  CHARINDEX(')',IT.poiDescription)-(CHARINDEX('(',IT.poiDescription)+1)) AS int) AS IDProductoCM
  ,IT.poiName 'NombreProducto'
  ,IT.poiQuantity 'Cantidad_Item'
  ,IT.poiTotalAmount 'MontoTotal_Item'
  ,(IT.poiTotalAmount/IT.poiQuantity) 'PrecioUnit_NETO'
  ,porCurrency 'MonedaOC'
FROM DCCPProcurement.dbo.prcPOHeader AS HEAD
INNER JOIN DCCPProcurement.dbo.prcPOItem AS IT ON IT.poiOrder=HEAD.porID
LEFT  JOIN DCCPPlatform.dbo.gblorganization AS C on HEAD.porBuyerOrganization=C.orgcode
LEFT  JOIN DCCPPlatform.dbo.gblorganization AS PROV on HEAD.porSellerOrganization=PROV.orgcode
LEFT  JOIN DCCPPlatform.dbo.gblenterprise AS I on C.orgenterprise=I.entcode 
LEFT  JOIN DCCPPlatform.dbo.gblenterprise AS E on PROV.orgenterprise=E.entcode 
WHERE 
  porJustifyType = 703 
  AND porIsIntegrated = 2 
  AND porBuyerStatus in (4,5,6,7,12)
  AND cast(porSendDate as date) BETWEEN @FechaAnterior AND @FechaActual
  AND IT.poiNroLicitacionPublica in ('2239-20-LR20',
                                     '2239-10-LR21',
                                     '2239-17-LR22')
),

oc_compu_final as(

  SELECT oc_compu.*,
		CASE WHEN  (MonedaOC) <> 'CLP' THEN (SELECT TOP (1) (PrecioUnit_NETO) * CA.exrValue 
											FROM DCCPPlatform.dbo.gblExchangeRate CA 
											WHERE  (MonedaOC) = CA.exrCurrency AND 
													CA.exrDateTo  <= FechaOC order by [exrDateFrom] desc)
													ELSE MontoTotal_Item END AS PrecioUnit_NETO_CLP
  FROM oc_compu  
 )

SELECT id_licitacion,CodigoOC,FechaOC,Organismo,Proveedor,
IDProductoCM,NombreProducto,Cantidad_Item,PrecioUnit_NETO_CLP*Cantidad_Item as MontoTotal_Item,PrecioUnit_NETO_CLP as PrecioUnit_NETO,
MonedaOC
FROM oc_compu_final

UNION

SELECT 
  IT.poiNroLicitacionPublica as id_licitacion
	,porCode AS CodigoOC
  ,CAST(porSendDate as date) AS FechaOC
  ,UPPER(I.entName) AS Organismo
  ,UPPER(PROV.orgLegalName) AS Proveedor
  ,CAST(SUBSTRING(IT.poiDescription,
  CHARINDEX('(',IT.poiDescription)+1,
  CHARINDEX(')',IT.poiDescription)-(CHARINDEX('(',IT.poiDescription)+1)) AS int) AS IDProductoCM
  ,IT.poiName 'NombreProducto'
  ,IT.poiQuantity 'Cantidad_Item'
  ,IT.poiTotalAmount 'MontoTotal_Item'
  ,(IT.poiTotalAmount/IT.poiQuantity) 'PrecioUnit_NETO'
  ,porCurrency 'MonedaOC'
FROM DCCPProcurement.dbo.prcPOHeader AS HEAD
INNER JOIN DCCPProcurement.dbo.prcPOItem AS IT ON IT.poiOrder=HEAD.porID
LEFT  JOIN DCCPPlatform.dbo.gblorganization AS C on HEAD.porBuyerOrganization=C.orgcode
LEFT  JOIN DCCPPlatform.dbo.gblorganization AS PROV on HEAD.porSellerOrganization=PROV.orgcode
LEFT  JOIN DCCPPlatform.dbo.gblenterprise AS I on C.orgenterprise=I.entcode 
LEFT  JOIN DCCPPlatform.dbo.gblenterprise AS E on PROV.orgenterprise=E.entcode 
WHERE 
  porJustifyType = 703 
  AND porIsIntegrated = 2 
  AND porBuyerStatus in (4,5,6,7,12)
  AND cast(porSendDate as date) BETWEEN @FechaAnterior AND @FechaActual
  AND IT.poiNroLicitacionPublica in ('2239-2-LR18',  -- Ferr
                                     '2239-17-LR20', -- Escr
                                     '2239-7-LR17',  -- Alim
                                     '2239-13-LR21', -- Insu
                                     '2239-5-LR19',  -- Aseo v1
                                     '2239-9-LR22',   -- Aseo v2
                                     '2239-13-LR23'   -- Ferr v4
                                     )
                             
                             
",
  sep=""
)

transaction <- dbGetQuery(myconnAQ,transaction_qry)

message('=============================')
message(paste0('====> transacciones extraidas entre el ',extract_date_ini,' y el ',extract_date_fin))
message('=============================')

# transacciones solo de la RM
transaction <- transaction %>%
  mutate(
    producto_rm = if_else(
      (id_licitacion == '2239-7-LR17' & str_detect(NombreProducto,' RM') & !str_detect(NombreProducto,'REGION')) |
        (id_licitacion == '2239-9-LR22' & str_detect(NombreProducto,' RM')) |
        (id_licitacion == '2239-17-LR20' & str_detect(NombreProducto,'MACROZONA RM')) |
        (id_licitacion == '2239-13-LR21' & str_detect(NombreProducto,'CENTRO')) |
        (id_licitacion %in% c('2239-5-LR19','2239-20-LR20','2239-10-LR21','2239-17-LR22','2239-2-LR18','2239-13-LR23')),
      1,0)
  )

transaction <- transaction[transaction$producto_rm == 1,] # mantenemos las transacciones ocurridas en la rm

# eliminamos los servicios 
servicios <- c(1587411, # instalaciones
               1587413, # instalaciones
               1587404, # instalaciones
               1587415, # instalaciones
               1587414, # instalaciones
               1587417  # servicio de pintura
               )

transaction <- transaction[!transaction$IDProductoCM %in% servicios,]

# aseguramos que la fecha de la oc sea tipo date
transaction$FechaOC <- as.Date(transaction$FechaOC)

rm(list = c('transaction_qry',
            'myconnAQ'))
