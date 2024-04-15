# conexiones
myconn <- RODBC::odbcConnect("AQ", uid="datawarehouse" , pwd="datawarehouse")

anio <- year(report_date)
mes  <- month(report_date)

message('Cargando todas las transacciones del mes.......')

# obtenemos las transacciones de los convenios 
# disponibles en la tienda externa
 
transaction_qry <- paste(
  
"
DECLARE 
@FechaActual DATE, 
@FechaAnterior DATE

SET @FechaActual = '",ceiling_date(as.Date(report_date),'month') - days(1),"'
SET @FechaAnterior = '",report_date - days(30),"'

----;WITH oc_compu AS(
----SELECT 
----  IT.poiNroLicitacionPublica as id_licitacion
----	,porCode AS CodigoOC
----  ,CAST(porSendDate as date) AS FechaOC
----  ,UPPER(I.entName) AS Organismo
----  ,UPPER(PROV.orgLegalName) AS Proveedor
----  ,CAST(SUBSTRING(IT.poiDescription,
----  CHARINDEX('(',IT.poiDescription)+1,
----  CHARINDEX(')',IT.poiDescription)-(CHARINDEX('(',IT.poiDescription)+1)) AS int) AS IDProductoCM
----  ,IT.poiName 'NombreProducto'
----  ,IT.poiQuantity 'Cantidad_Item'
----  ,IT.poiTotalAmount 'MontoTotal_Item'
----  ,(IT.poiTotalAmount/IT.poiQuantity) 'PrecioUnit_NETO'
----  ,porCurrency 'MonedaOC'
----FROM DCCPProcurement.dbo.prcPOHeader AS HEAD
----INNER JOIN DCCPProcurement.dbo.prcPOItem AS IT ON IT.poiOrder=HEAD.porID
----LEFT  JOIN DCCPPlatform.dbo.gblorganization AS C on HEAD.porBuyerOrganization=C.orgcode
----LEFT  JOIN DCCPPlatform.dbo.gblorganization AS PROV on HEAD.porSellerOrganization=PROV.orgcode
----LEFT  JOIN DCCPPlatform.dbo.gblenterprise AS I on C.orgenterprise=I.entcode 
----LEFT  JOIN DCCPPlatform.dbo.gblenterprise AS E on PROV.orgenterprise=E.entcode 
----WHERE 
----  porJustifyType = 703 
----  AND porIsIntegrated = 2 
----  AND porBuyerStatus in (4,5,6,7,12)
----  AND cast(porSendDate as date) BETWEEN @FechaAnterior AND @FechaActual
----  AND poiIdConvenioMarco IN (5800272,5800289)
----),

----oc_compu_final as(

----  SELECT oc_compu.*,
----		CASE WHEN  (MonedaOC) <> 'CLP' THEN (SELECT TOP (1) (PrecioUnit_NETO) * CA.exrValue 
----											FROM DCCPPlatform.dbo.gblExchangeRate CA 
----											WHERE  (MonedaOC) = CA.exrCurrency AND 
----													CA.exrDateTo  <= FechaOC order by [exrDateFrom] desc)
----													ELSE MontoTotal_Item END AS PrecioUnit_NETO_CLP
----  FROM oc_compu  
---- )

----SELECT id_licitacion,CodigoOC,FechaOC--,Organismo,Proveedor,
----IDProductoCM,NombreProducto,Cantidad_Item,PrecioUnit_NETO_CLP*Cantidad_Item as MontoTotal_Item,PrecioUnit_NETO_CLP as PrecioUnit_NETO,
----MonedaOC
----FROM oc_compu_final

----UNION

SELECT 
  IT.poiNroLicitacionPublica as id_licitacion
	,porCode AS CodigoOC
  ,CAST(porSendDate as date) AS FechaOC
  --,UPPER(I.entName) AS Organismo
  --,UPPER(PROV.orgLegalName) AS Proveedor
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
--LEFT  JOIN DCCPPlatform.dbo.gblorganization AS C on HEAD.porBuyerOrganization=C.orgcode
--LEFT  JOIN DCCPPlatform.dbo.gblorganization AS PROV on HEAD.porSellerOrganization=PROV.orgcode
--LEFT  JOIN DCCPPlatform.dbo.gblenterprise AS I on C.orgenterprise=I.entcode 
--LEFT  JOIN DCCPPlatform.dbo.gblenterprise AS E on PROV.orgenterprise=E.entcode 
WHERE 
  porJustifyType = 703 
  AND porIsIntegrated = 2 
  AND porBuyerStatus in (4,5,6,7,12)
  AND cast(porSendDate as date) BETWEEN @FechaAnterior AND @FechaActual
  --AND poiIdConvenioMarco in (5800266,
  --                           5800254,
  --                           5800280,
  --                           5800279)
"  
  , sep = ""
)

total_transaction <- RODBC::sqlQuery(myconn,transaction_qry)

# resumen de transacciones total - convenios de productos
total_ammount_product_cm <- total_transaction %>% 
  filter(!id_licitacion %in% c('2239-2-LR21','2239-4-LR20')) %>%
  group_by(fecha = floor_date(as.Date(FechaOC),'month'),id_licitacion) %>%
  summarise(total_transaction = sum(MontoTotal_Item)) %>%
  arrange(desc(total_transaction)) 

# convenios monitoreados a la fecha
monitored_cm <- c('2239-7-LR17',  # alimentos
                  '2239-5-LR19',  # aseo
                  '2239-2-LR18',  # ferreteria
                  '2239-17-LR20', # escritorio
                  #'2239-6-LR20',  # combustibles
                  '2239-20-LR20', # computadores
                  '2239-6-LR19'   # computadores
)

# generamos una var que indique si es transaccion de la rm
transaction_ammount_cm <- total_transaction %>%
  filter(id_licitacion %in% monitored_cm) %>%
  mutate(
    producto_rm = if_else(
      (id_licitacion == '2239-7-LR17' & stringr::str_detect(NombreProducto,' RM') & !stringr::str_detect(NombreProducto,'REGION')) |
        (id_licitacion == '2239-17-LR20' & stringr::str_detect(NombreProducto,'MACROZONA RM')) |
        (id_licitacion %in% monitored_cm & !id_licitacion %in% c('2239-7-LR17','2239-17-LR20')),
      1,0)
  )

# eliminamos los servicios 
servicios <- c(1587411, # instalaciones
               1587413, # instalaciones
               1587404, # instalaciones
               1587415, # instalaciones
               1587414  # instalaciones
)

transaction_ammount_cm <- transaction_ammount_cm[!transaction_ammount_cm$IDProductoCM %in% servicios,]

# resumimos las transacciones por convenio y mes
total_ammount_cm <- transaction_ammount_cm %>%
  filter(producto_rm == 1) %>%
  mutate(convenio = case_when(id_licitacion == "2239-5-LR19" ~ "Aseo",
                              id_licitacion == "2239-2-LR18" ~ "Ferreteria",
                              id_licitacion == "2239-6-LR19" ~ "Computadores",
                              id_licitacion == "2239-20-LR20" ~ "Computadores",
                              id_licitacion == "2239-17-LR20" ~ "Escritorio RM",
                              id_licitacion == "2239-6-LR20" ~ "Combustible",
                              id_licitacion == "2239-7-LR17" ~ "Alimentos RM",
                              TRUE ~ 'Otro')) %>%
  group_by(convenio,fecha = floor_date(as.Date(FechaOC),'month')) %>% 
  summarise(#monto_transado_usd = sum(monto_usd_neto),
            #monto_transado_clp = sum(monto_clp_neto)
            monto_transado = sum(MontoTotal_Item)) %>%
  arrange(convenio,fecha)

rm(list=c('all_files',
          'anio',
          'mes',
          'i',
          'transaction_qry',
          'temp_data'))