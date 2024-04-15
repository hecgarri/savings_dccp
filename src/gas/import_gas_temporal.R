rm(list=ls())

library(dplyr)
library(lubridate)

# conexiones
myconnAQ <- RODBC::odbcConnect("AQ", uid="datawarehouse" , pwd="datawarehouse")
dir <- "C:/O/OneDrive - DCCP/GitHub/ahorro_magento/api cne"  


# parametros
extract_date_ini <- '2023-10-01'
extract_date_fin <- '2023-10-31' 
fecha <- paste0(year(extract_date_ini),ifelse(month(extract_date_ini)<10,paste0(0,month(extract_date_ini)),month(extract_date_ini)))


setwd('C:/O/OneDrive - DCCP/Github/eficiencia_nuevo_modelo_cm/ahorro')

# datos extraidos de la api cne 
carpeta_gas <- list.files(path="./inputs/Gas Temporal")

# unimos todos los datos de precios
precios_gas <- data.frame()
for (i in 1:length(carpeta_gas)) {
  temp_data <- readxl::read_xlsx(paste0("./inputs/Gas Temporal/",carpeta_gas[i]))
  
  temp_data$fecha_consulta <- stringr::str_remove(stringr::str_remove(carpeta_gas[i],'\\).xlsx'),"ArchivoRegistro\\(")
  temp_data$fecha_actualizacion <-  as.Date(temp_data$`Ultima actualización`)
  temp_data$precio <- stringr::str_remove_all(temp_data$Precio,"\\.|\\$")
  temp_data$precio <- as.numeric(temp_data$precio)
  temp_data$tipo_gas <- 'normal'
  temp_data$modelo <- paste0(stringr::str_remove(temp_data$Tamaño,'Kg'),' KG')
  temp_data$Distribuidor <- ifelse(temp_data$Distribuidor=='#¡VALOR!','GASCO MAGALLANES',temp_data$Distribuidor)
  temp_data$Comuna <- ifelse(temp_data$Comuna=='#¡VALOR!','Cabo de Hornos',temp_data$Comuna)
  precios_gas <- bind_rows(precios_gas,
                           temp_data #%>% select(all_of(variables_necesarias))
                           )
  precios_gas <- precios_gas %>% filter(modelo != '2 KG')
  
}

precios_gas <- precios_gas %>% distinct() %>% 
  select(
    proveedor=Distribuidor,
    modelo,
    nombre_comuna = Comuna,
    nombre_region = REGIÓN,
    precio,
    fecha_actualizacion,
    fecha_consulta)


# calculamos el precio promedio semanal 
market_price_gas <- precios_gas %>% 
  group_by(proveedor,modelo,nombre_comuna) %>%
  mutate(inicio_semana = floor_date(fecha_actualizacion,'week',week_start = 1))

market_price_gas <- market_price_gas %>%
  mutate(
    region = case_when(
      nombre_region == 'Tarapacá' ~ 'I REGIÓN',
      nombre_region == 'Antofagasta' ~ 'II REGIÓN',
      nombre_region == 'Atacama' ~ 'III REGIÓN',
      nombre_region == 'Coquimbo' ~ 'IV REGIÓN',
      nombre_region == 'Valparaíso' ~ 'V REGIÓN',
      nombre_region == "Gral. Bernardo O'Higgins" ~ 'VI REGIÓN',
      nombre_region == 'Maule' ~ 'VII REGIÓN',
      nombre_region == 'Bío Bío' ~ 'VIII REGIÓN',
      nombre_region == 'Araucanía' ~ 'IX REGIÓN',
      nombre_region == 'Los Lagos' ~ 'X REGIÓN',
      nombre_region == 'Aysén Gral. C. Ibáñez del Campo' ~ 'XI REGIÓN',
      nombre_region == 'Magallanes y la Antártida Chilena' ~ 'XII REGIÓN',
      nombre_region == 'Metropolitana' ~ 'REGIÓN METROPOLITANA',
      nombre_region == 'Los Ríos' ~ 'XIV REGIÓN',
      nombre_region == 'Arica y Parinacota' ~ 'XV REGIÓN',
      nombre_region == 'Ñuble' ~ 'XVI REGIÓN'
    )
  )

# precio promedio regional por proveedor
summary_gas_price <- market_price_gas %>%
  group_by(proveedor,modelo,region,inicio_semana) %>% 
  summarise(precio_capturado_prom = mean(precio, na.rm = TRUE))

# validamos los precios por cilindro

# 5 KG
market_price_gas %>%
  filter(inicio_semana >= extract_date_ini & inicio_semana <= extract_date_fin & 
         modelo == '5 KG') %>%
  ggplot(aes(x = region, y = precio)) +
    geom_boxplot(outlier.colour="red",
                 outlier.shape=8,
                 outlier.size=4) +
    labs(title = 'Distribución de Precios de Gas',
         subtitle = 'Cilindro de 5 Kg',
         x = '',
         y = 'Precio por unidad') +
    coord_flip()

# 11 KG
market_price_gas %>%
  filter(inicio_semana >= extract_date_ini & inicio_semana <= extract_date_fin & 
           modelo == '11 KG') %>%
  ggplot(aes(x = region, y = precio)) +
  geom_boxplot(outlier.colour="red",
               outlier.shape=8,
               outlier.size=4) +
  labs(title = 'Distribución de Precios de Gas',
       subtitle = 'Cilindro de 11 Kg',
       x = '',
       y = 'Precio por unidad') +
  coord_flip()

# 15 KG
market_price_gas %>%
  filter(inicio_semana >= extract_date_ini & inicio_semana <= extract_date_fin & 
           modelo == '15 KG') %>%
  ggplot(aes(x = region, y = precio)) +
  geom_boxplot(outlier.colour="red",
               outlier.shape=8,
               outlier.size=4) +
  labs(title = 'Distribución de Precios de Gas',
       subtitle = 'Cilindro de 11 Kg',
       x = '',
       y = 'Precio por unidad') +
  coord_flip()



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
                    paste0('C:/O/OneDrive - DCCP/GitHub/eficiencia_nuevo_modelo_cm/savings_cm/output/weekly savings/ahorro_gas_',fecha,'.xlsx'))







