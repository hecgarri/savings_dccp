rm(list=ls())

library(dplyr)
library(lubridate)
library(stringr)

# conexiones
myconnAQ <- RODBC::odbcConnect("AQ", uid="datawarehouse" , pwd="datawarehouse")
dir <- "C:/O/OneDrive - DCCP/GitHub/ahorro_magento/api cne"  


# parametros
extract_date_ini <- '2023-06-01'
extract_date_fin <- '2023-06-30' 
fecha <- paste0(year(extract_date_ini),ifelse(month(extract_date_ini)<10,paste0(0,month(extract_date_ini)),month(extract_date_ini)))

# importar precio de mercado de gas
gas_files <- list.files(path=dir)

# archivos de precios de gas
gas_files <- gas_files[str_detect(gas_files,'.csv')]

market_price_gas <- data.frame()
for (file in gas_files) {
    
  temp_data <- read.csv2(paste0(dir,'/',file))
  temp_data$fecha_actualizacion <- as.Date(temp_data$fecha_actualizacion)
  temp_data <- temp_data[temp_data$tipo_gas != 'catalitico' & temp_data$tamano != 2,]
  temp_data$modelo <- paste0(str_to_upper(temp_data$tamano),' ',str_to_upper(temp_data$medida))
  names(temp_data)[names(temp_data)=='marca'] <- 'proveedor'
  market_price_gas <- bind_rows(market_price_gas,temp_data)
  
}

# calculamos el precio promedio semanal 
market_price_gas <- market_price_gas %>% 
  group_by(nombre_empresa,modelo,nombre_comuna) %>%
  mutate(inicio_semana = floor_date(fecha_actualizacion,'week',week_start = 1))

# ajuste de las regiones
market_price_gas <- market_price_gas %>%
  mutate(
    nombre_region = case_when(id_region == 1 ~ 'Tarapaca',
              id_region == 5 ~ 'Valparaiso',
              id_region == 6 ~ 'Ohiggins',
              id_region == 8 ~ 'Bio-Bio',
              id_region == 9 ~ 'Araucania',
              id_region == 11 ~ 'Aysen',
              id_region == 12 ~ 'Magallanes',
              id_region == 14 ~ 'De los Rios',
              id_region == 16 ~ '?uble',
              TRUE ~ nombre_region
                        )
  )

market_price_gas <- market_price_gas %>%
  mutate(
    region = case_when(
      nombre_region == 'Tarapaca' ~ 'I REGI?N',
      nombre_region == 'Antofagasta' ~ 'II REGI?N',
      nombre_region == 'Atacama' ~ 'III REGI?N',
      nombre_region == 'Coquimbo' ~ 'IV REGI?N',
      nombre_region == 'Valparaiso' ~ 'V REGI?N',
      nombre_region == 'Ohiggins' ~ 'VI REGI?N',
      nombre_region == 'Del Maule' ~ 'VII REGI?N',
      nombre_region == 'Bio-Bio' ~ 'VIII REGI?N',
      nombre_region == 'Araucania' ~ 'IX REGI?N',
      nombre_region == 'De los Lagos' ~ 'X REGI?N',
      nombre_region == 'Aysen' ~ 'XI REGI?N',
      nombre_region == 'Magallanes' ~ 'XII REGI?N',
      nombre_region == 'Metropolitana de Santiago' ~ 'REGI?N METROPOLITANA',
      nombre_region == 'De los Rios' ~ 'XIV REGI?N',
      nombre_region == 'Arica y Parinacota' ~ 'XV REGI?N',
      nombre_region == '?uble' ~ 'XVI REGI?N'
      )
  )

# precio promedio regional por proveedor
summary_gas_price <- market_price_gas %>%
  group_by(proveedor = str_to_upper(proveedor),modelo,region,nombre_region,inicio_semana) %>% 
  summarise(precio_capturado_prom = mean(precio, na.rm = TRUE),
            n_cotizaciones = n_distinct(id_comuna))


transaction_qry <- paste(
  "
DECLARE 
@FechaActual DATE, 
@FechaAnterior DATE

SET @FechaActual = '",extract_date_fin,"'
SET @FechaAnterior = '",extract_date_ini,"'

SELECT 
  IT.poiNroLicitacionPublica as id_licitacion
	,porCode AS CodigoOC
  ,CAST(porSendDate as date) AS FechaOC
  ,UPPER(I.entName) AS Organismo
  ,CASE PROV.orgCode WHEN 26592 THEN 'ABASTIBLE'
					 WHEN 27100 THEN 'GASCO'
					 ELSE 'LIPIGAS' END proveedor
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
  AND poiIdConvenioMarco in (
                             5800297
                             )
",
  sep=""
)

transaction_gas <- RODBC::sqlQuery(myconnAQ,transaction_qry)


# atributos de productos del convenio
gas_qry <- paste(
"
SELECT  P.[idProducto]
      , CASE [idTipoProducto] WHEN 9474 THEN 'CILINDRO'
							  WHEN 9475 THEN 'GRANEL'
							  ELSE 'Otro' END as tipo_producto
      ,[Nombre]
      ,[Modelo] as modelo
      ,[Medida] as region
  FROM [ChileCompra_CMII].[dbo].[INGCM_Producto] as P
  LEFT JOIN [ChileCompra_CMII].[dbo].[INGCM_ProductosConvenio] as PC on PC.idProducto = P.idProducto
  WHERE PC.idConvenioMarco in (5800297)
	AND [Medida] != '' 

"
)

attribute_gas <- RODBC::sqlQuery(myconnAQ,gas_qry)


# unimos con las cifras de gas
transaction_gas <- merge(transaction_gas,
                         attribute_gas,
                         by.x = 'IDProductoCM',
                         by.y = 'idProducto',
                         all.x = TRUE)

# creamos variable con inicio de semana para los precios transados
transaction_gas$inicio_semana <- floor_date(as.Date(transaction_gas$FechaOC),'week', week_start = 1)

# unimos con precios de mercado
savings_gas <- merge(transaction_gas %>% filter(tipo_producto == 'CILINDRO'),
                     summary_gas_price,
                     by = c('proveedor','modelo','region','inicio_semana'),
                     all.x = TRUE)

# calculamos el ahorro
descuento <- 0
savings_gas <- savings_gas %>%
  mutate(
    convenio = 'Gas',
    precio_unitario = PrecioUnit_NETO*1.19,
    ahorro_item = 1-(precio_unitario/(precio_capturado_prom-descuento)),
    monto_ahorro_item = ((precio_capturado_prom-descuento) - precio_unitario)*Cantidad_Item
    ) 

savings_gas %>%
  summarise(
    ahorro_promedio = weighted.mean(ahorro_item,MontoTotal_Item, na.rm = TRUE),
    monto_monitoreado = sum(MontoTotal_Item, na.rm = TRUE),
    monto_ahorrado = sum(monto_ahorro_item, na.rm = TRUE)
  )


writexl::write_xlsx(savings_gas,
  paste0('C:/Users/javier.guajardo/Documents/GitHub/eficiencia_nuevo_modelo_cm/savings_cm/output/weekly savings/ahorro_gas_',fecha,'.xlsx'))
















# monto transado total
total_transaction_ammount <- sum(transaction_gas$MontoTotal_Item[transaction_gas$tipo_producto=='CILINDRO'])

transaction[transaction$IDProductoCM == 1704021,c('Proveedor','IDProductoCM','PrecioUnit_NETO','Cantidad_Item')]



savings_gas %>%
  filter(region == 'REGI?N METROPOLITANA') %>%
  group_by(modelo) %>%
  summarise(
    ahorro_promedio = weighted.mean(ahorro_item,MontoTotal_Item, na.rm = TRUE),
    monto_ahorrado = sum(monto_ahorro_item, na.rm = TRUE),
    monto_transado = sum(MontoTotal_Item, na.rm = TRUE),
    precio_transado = mean(precio_unitario),
    precio_mercado  = mean(precio_capturado_prom)
  )












library(ggplot2)

summary_gas_price %>%
  filter(modelo == '5 KG' & region == 'REGI?N METROPOLITANA') %>%
  ggplot(aes(x = inicio_semana, y = avg_market_price, color = nombre_empresa)) +
  geom_line() +
  geom_point()
