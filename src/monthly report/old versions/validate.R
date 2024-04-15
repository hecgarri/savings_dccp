message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message('----------------------------------')
message(' 2) Comienza el proceso de validacion de precios de mercado')

market_prices_old <- market_prices
market_prices     <- market_prices_old %>% distinct()   # elimina los duplicados totales
message(paste0('====> se eliminan ',nrow(market_prices_old) - nrow(market_prices),' capturas de precio duplicadas'))
rm(market_prices_old)

# elimina duplicados de una misma captura a distintas horas del dia 
# (mismo producto y proveedor)
market_prices_old <- market_prices
elimina_primera <- FALSE 

if (elimina_primera == TRUE) {
  # eliminamos la primera captura realizada
  market_prices <- market_prices %>% 
    arrange(id_producto,id_tienda,url,desc(fecha_captura)) %>%   # agrupamos por producto, proveedor y fecha descendente
    distinct(id_producto,id_tienda,url, .keep_all = TRUE)        # eliminamos la primera cotizacion capturada
} else {
  # eliminamos la ultima captura realizada
  market_prices <- market_prices %>% 
    arrange(id_producto,id_tienda,url,fecha_captura) %>%   # agrupamos por producto, proveedor y fecha ascendente
    distinct(id_producto,id_tienda,url,.keep_all = TRUE)  # eliminamos la ultima cotizacion capturada
}

message(paste0('====> se eliminan ',nrow(market_prices_old) - nrow(market_prices),' capturas de precio tomamos en una misma fecha'))
rm(market_prices_old)

# empresas no validas
empresa_no_valida <- c('insumos esami',
                       'puntopapelexpress',
                       'soin',
                       'coloma',
                       'dydvaldivia',
                       'market coupling',
                       'cobronce',
                       #'farmazon',
                       'mercadito saludable',
                       'ahorroexpress',
                       'olostocks')

market_prices_old <- market_prices
market_prices <- market_prices[market_prices$precio_capturado>0,]                # eliminamos las capturas con precio 0
market_prices <- market_prices[is.na(market_prices$tienda)==FALSE,]              # eliminamos las capturas sin un proveedor definido
market_prices <- market_prices[!market_prices$tienda %in% empresa_no_valida,]    # eliminamos capturas de tiendas no validas
message(paste0('====> se eliminan ',nrow(market_prices_old) - nrow(market_prices),' capturas de precio = 0, sin proveedor definido o de tiendas no validas'))
rm(market_prices_old)

# productos que contienen dos item distintos (ej: estufa + kit de instalacion)
# en estos casos se reemplaza el valor de cada parte por la suma total del producto y sus accesorios
multiproductos <- c(
  1634347,
  1634472,
  1634761,
  1635306,
  1635368,
  1702610,
  1702621
)

for (item in multiproductos) {
  message(unique(market_prices$producto[market_prices$id_producto == item]))
  message(paste0('se reemplaza el precio del componente: ',market_prices$precio_capturado[market_prices$id_producto == item],' '))
  market_prices$precio_capturado[market_prices$id_producto == item] <- sum(market_prices$precio_capturado[market_prices$id_producto == item])
}

# generamos una medida de las diferencias de precios por proveedor
# este es un proceso iterativo comenzando siempre desde 0.5 + y -
# de ahi en adelante vamos revisando hasta utilizar todas la cotizaciones
up_lim <- 60    # 100% de diferencias como maximo
lo_lim <- -60    
market_prices <- market_prices %>%
  group_by(id_producto) %>%
  mutate(sd_prom = (precio_capturado-mean(precio_capturado))/mean(precio_capturado)*100,
         revisar = if_else(sd_prom < lo_lim | sd_prom > up_lim,1,0))

price_outlier <- market_prices$id_producto[market_prices$revisar == 1]
price_outlier_tab <- market_prices[market_prices$id_producto %in% price_outlier,]
writexl::write_xlsx(price_outlier_tab,paste0('./savings_cm/output/validate/capturas_revision_',import_date_numeric,'.xlsx'))

market_prices_fin <- market_prices[!market_prices$id_producto %in% price_outlier,]
message(paste0('====> no se consideran ',nrow(market_prices) - nrow(market_prices_fin),' capturas de precio por posible error'))
market_prices <- market_prices_fin 

rm(list=c('elimina_primera',
          'up_lim',
          'lo_lim',
          'price_outlier',
          'price_outlier_tab',
          'market_prices_fin'#,
          #'import_date_numeric'
          ))

