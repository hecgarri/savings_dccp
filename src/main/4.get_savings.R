message('===========================')
message('unimos datos transaccionales y precios de referencia')

#cargamos productos 80-20 (archivo general agrupa todas las actualizaciones)
#Toma month_fist_day del script 2.data_processing
#Los archivos 80-20 se construyen con la rutina 0.es_80p_gen.R

month_first_day <- "2024-07-01"
most_value_prod <- readxl::read_xlsx(paste0('./data/productos 80-20/'
                                            ,month_first_day,' productos 80-20.xlsx'))

# obtenemos el promedio por id
avg_market_price <- market_prices %>% 
  group_by(id_producto,convenio) %>%
  summarise(
    n_cotizaciones = n_distinct(id_tienda),
    precio_capturado_prom = mean(precio_capturado))

# Ahorro del periodo
savings_old <- merge(transaction %>% rename(producto = NombreProducto),
                 avg_market_price %>% select(id_producto,precio_capturado_prom,n_cotizaciones,convenio),
                 by.x = "IDProductoCM",
                 by.y = "id_producto",
                 all.x = TRUE)

savings2 <- savings_old %>% 
  mutate(
    precio_unitario = if_else(convenio == 'Alimentos RM',PrecioUnit_NETO,PrecioUnit_NETO*1.19),
    ahorro_item = -1*((precio_unitario - precio_capturado_prom)/precio_capturado_prom),
    monto_ahorro_item = -1*(precio_unitario - precio_capturado_prom)*Cantidad_Item) 

# agregamos info de los 80-20 por convenio
savings <- merge(savings2 ,
                 most_value_prod %>% select(id_producto,es_80p_gen),
                 by.x = 'IDProductoCM',
                 by.y = 'id_producto',
                 all.x = TRUE)


# listado de productos con desahorro
savings %>% filter(ahorro_item < 0) %>% 
  group_by(IDProductoCM,producto) %>% 
  summarise(
    precio_minimo_transado = min(precio_unitario, na.rm = TRUE),
    precio_maximo_transado = max(precio_unitario, na.rm = TRUE),
    precio_capturado_prom = mean(precio_capturado_prom, na.rm = TRUE),
    ahorro_promedio = weighted.mean(ahorro_item,MontoTotal_Item, na.rm = TRUE)) %>% 
  arrange(ahorro_promedio)


# library(ggplot2)
# savings %>%
#   filter(ahorro_item > lim_inf & ahorro_item < lim_sup) %>%
#   ggplot(aes(x = ahorro_item, color = convenio, fill = convenio)) +
#   geom_density(alpha = 0.5) +
#   geom_vline(xintercept = 0, linetype = 'dashed', color = 'red') +
#   labs(title = 'Distribucion de ahorro por convenio') +
#   scale_x_continuous(labels = scales::percent) +
#   theme_bw()

# --------- getting overpriced --------- #

message('=======================')
message('Outlier de desahorro')
message('=======================')
outlier_overpriced <- savings %>% 
  filter(ahorro_item < 0) %>% 
  group_by(IDProductoCM,producto) %>% 
  summarise(
    'precio minimo transado' = min(precio_unitario, na.rm = TRUE),
    'precio maximo transado' = max(precio_unitario, na.rm = TRUE),
    'precio capturado promedio' = mean(precio_capturado_prom, na.rm = TRUE),
    'ahorro promedio' = weighted.mean(ahorro_item,MontoTotal_Item, na.rm = TRUE)) %>% 
  arrange(`ahorro promedio`)

lista_overpriced <- list('productos' = outlier_overpriced,
                         'cotizaciones' = market_prices %>% 
                           filter(id_producto %in% outlier_overpriced$IDProductoCM) %>%
                           select(-Ranking,-ProductoAlternativo) %>%
                           arrange(producto)
)

writexl::write_xlsx(lista_overpriced,paste0('./output/overpriced/sobreprecio_',import_date_numeric,'.xlsx'))

lim_sup_rev <- 0.6
message('=======================')
message('Outlier de ahorro')
message('=======================')
outlier_savings <- savings %>% 
  filter(ahorro_item > lim_sup_rev) %>%
  #select(IDProductoCM,producto,precio_capturado_prom,precio_unitario,ahorro_item) %>%
  group_by(IDProductoCM,producto) %>%
  summarise(
    'precio minimo transado' = min(precio_unitario, na.rm = TRUE),
    'precio maximo transado' = max(precio_unitario, na.rm = TRUE),
    'precio capturado promedio' = mean(precio_capturado_prom, na.rm = TRUE),
    'ahorro promedio' = weighted.mean(ahorro_item,MontoTotal_Item, na.rm = TRUE)) %>% 
  arrange(`ahorro promedio`)

writexl::write_xlsx(outlier_savings,paste0('./output/overpriced/outlier_ahorro_',import_date_numeric,'.xlsx'))