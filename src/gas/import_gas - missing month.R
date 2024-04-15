rm(list=ls())

library(dplyr)
library(lubridate)
library(stringr)
library(forecast)

# conexiones
myconnAQ <- RODBC::odbcConnect("AQ", uid="datawarehouse" , pwd="datawarehouse")
dir <- "C:/O/OneDrive - DCCP/GitHub/ahorro_magento/api cne"  


# parametros
extract_date_ini <- '2024-02-01'
extract_date_fin <- '2024-02-29' 
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
              id_region == 16 ~ 'Ñuble',
              TRUE ~ nombre_region
                        )
  )

market_price_gas <- market_price_gas %>%
  mutate(
    region = case_when(
      nombre_region == 'Tarapaca' ~ 'I REGIÓN',
      nombre_region == 'Antofagasta' ~ 'II REGIÓN',
      nombre_region == 'Atacama' ~ 'III REGIÓN',
      nombre_region == 'Coquimbo' ~ 'IV REGIÓN',
      nombre_region == 'Valparaiso' ~ 'V REGIÓN',
      nombre_region == 'Ohiggins' ~ 'VI REGIÓN',
      nombre_region == 'Del Maule' ~ 'VII REGIÓN',
      nombre_region == 'Bio-Bio' ~ 'VIII REGIÓN',
      nombre_region == 'Araucania' ~ 'IX REGIÓN',
      nombre_region == 'De los Lagos' ~ 'X REGIÓN',
      nombre_region == 'Aysen' ~ 'XI REGIÓN',
      nombre_region == 'Magallanes' ~ 'XII REGIÓN',
      nombre_region == 'Metropolitana de Santiago' ~ 'REGIÓN METROPOLITANA',
      nombre_region == 'De los Rios' ~ 'XIV REGIÓN',
      nombre_region == 'Arica y Parinacota' ~ 'XV REGIÓN',
      nombre_region == 'Ñuble' ~ 'XVI REGIÓN'
      )
  )

# precio promedio regional por proveedor
summary_gas_price <- market_price_gas %>%
  group_by(proveedor = str_to_upper(proveedor),modelo,region,nombre_region,inicio_semana) %>% 
  summarise(precio_capturado_prom = mean(precio, na.rm = TRUE),
            n_cotizaciones = n_distinct(id_comuna))


# ------------------------------------------------------
# proyectamos la serie para los meses sin datos

data_inicial <- summary_gas_price %>% select(region,modelo,proveedor) %>% distinct()

# proveedor <- 'ABASTIBLE'
# region    <- 'Tarapaca'
# cilindro  <- '5 KG'

proj_weeks <- data.frame(inicio_semana = seq.Date(from = as.Date('2023-07-31'),to = as.Date('2024-02-29'),by = 'week'))

tabla <- data.frame(
  # proveedor = NA,
  # nombre_region = NA,
  # modelo = NA,
  # precio_capturado_prom = NA,
  # tipo_precio = 'proyeccion',
  # inicio_semana = NA
)

for (i in 1:nrow(data_inicial)) {
  # for (cilindro in cilindros) {
  #   for (proveedor in proveedores) {
      
      data_filt <- summary_gas_price$precio_capturado_prom[summary_gas_price$nombre_region == data_inicial$nombre_region[i] & 
                                                           summary_gas_price$modelo==data_inicial$modelo[i] & 
                                                           summary_gas_price$proveedor==data_inicial$proveedor[i]]
      tsdata <- ts(data_filt, frequency = 52)
      model <- auto.arima(tsdata)
      myforecast <- forecast(model, level=c(90), h=nrow(proj_weeks))
      plot(myforecast)
      
      new_data <- data.frame(
        proveedor = data_inicial$proveedor[i],
        region = data_inicial$region[i],
        nombre_region = data_inicial$nombre_region[i],
        modelo = data_inicial$modelo[i],
        precio_capturado_prom = myforecast$mean,
        tipo_precio = 'proyeccion'
      )
      
      new_data <- bind_cols(new_data,proj_weeks)
      
      tabla <- bind_rows(tabla,new_data)

  #   }  
  # }
}


# unimos las proyecciones con los datos historicos

summary_gas_price <- bind_rows(summary_gas_price %>% select(-n_cotizaciones),
                               tabla %>% select(proveedor,modelo,region,nombre_region,inicio_semana,precio_capturado_prom))



# ------------------------------------------------------

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
                             5800297,
                             5802336
                             )
",
  sep=""
)

transaction_gas <- RODBC::sqlQuery(myconnAQ,transaction_qry)


# atributos de productos del convenio
gas_qry <- paste(
"
SELECT  P.[idProducto]
      , PC.idConvenioMarco
      , CASE [idTipoProducto] WHEN 9474 THEN 'CILINDRO'
							  WHEN 10010 THEN 'CILINDRO'
							  WHEN 10012 THEN 'CILINDRO'
							  WHEN 9475 THEN 'GRANEL'
							  WHEN 10011 THEN 'GRANEL'
							  ELSE 'Otro' END as tipo_producto
      ,[Nombre]
      ,[Modelo] as modelo
      ,[Medida] as region
  FROM [ChileCompra_CMII].[dbo].[INGCM_Producto] as P
  LEFT JOIN [ChileCompra_CMII].[dbo].[INGCM_ProductosConvenio] as PC on PC.idProducto = P.idProducto
  WHERE PC.idConvenioMarco in (5800297,5802336)
	AND [Medida] != '' 

"
)

attribute_gas <- RODBC::sqlQuery(myconnAQ,gas_qry)

# para el periodo sin precios utilizaremos el promedio del ultimo mes disponible
# missing_summary_gas_price_jun <- market_price_gas %>%
#   filter(inicio_semana >= '2023-05-19') %>%
#   mutate(inicio_semana = '2023-06-01') %>% 
#   group_by(proveedor = str_to_upper(proveedor),modelo,region,nombre_region,inicio_semana) %>% 
#   summarise(precio_capturado_prom = mean(precio, na.rm = TRUE),
#             n_cotizaciones = n_distinct(id_comuna))


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
  paste0('C:/o/OneDrive - DCCP/Traspaso/2. Cálculo de Ahorro/output/weekly savings/ahorro_gas_',fecha,'.xlsx'))










