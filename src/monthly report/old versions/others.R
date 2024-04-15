tab_comparativa <- merge(savings_avg_table,
                         total_ammount_cm,
                         by = c('Convenio','fecha'),
                         all.x = TRUE
)

tab_comparativa <- tab_comparativa %>% mutate(cobertura = monto_transado_ahorro_usd/monto_transado_usd,
                                              ahorro_ratio = monto_ahorro_usd/monto_transado_usd)


tabla <- list(
  'indices' = tab_comparativa %>%                         
    group_by(fecha) %>%
    summarise(
      ind_1_monit = weighted.mean(indicador,monto_transado_ahorro_usd),
      ind_2_trans = weighted.mean(indicador,monto_transado_usd),
      ind_3_ratio_trans = sum(monto_ahorro_usd)/sum(monto_transado_usd),
      ind_4_ratio_monit = sum(monto_ahorro_usd)/sum(monto_transado_ahorro_usd)), 
  'montos' = tab_comparativa %>%                         
    group_by(fecha) %>%
    summarise(
      transado = sum(monto_transado_usd),
      monitoreado = sum(monto_transado_ahorro_usd),
      ahorrado = sum(monto_ahorro_usd)),
  'montos_cm' = tab_comparativa %>%
    select(fecha,Convenio,monto_transado_usd) %>%
    arrange(fecha) %>%
    tidyr::pivot_wider(names_from = fecha, values_from = monto_transado_usd),
  'detalle_mes' = savings_all %>% filter(year(fecha) == max(fecha) & month(fecha) == max(fecha))
)


tab_comparativa %>%                         
  group_by(fecha) %>%
  summarise(
    transado = sum(monto_transado_usd),
    monitoreado = sum(monto_transado_ahorro_usd),
    ahorrado = sum(monto_ahorro_usd)) %>%
  
  tidyr::pivot_longer(!fecha,names_to = 'tipo_monto',values_to = 'monto') %>%
  ggplot(aes(x = fecha, y = monto, fill = tipo_monto)) +
  geom_bar(stat = 'identity', position = position_stack())



# unimos con montos transados
savings_avg_table <- merge(savings_avg_table,
                           total_ammount_cm ,
                           by = c('fecha','Convenio'),
                           all.x = TRUE)

savings_avg_table %>%
  select(fecha,monto_transado_clp,monto_ahorro_clp,monto_transado_ahorro) %>%
  group_by(fecha) %>%
  summarise(#total_transado = sum(monto_transado_clp),
    #total_ahorrado = sum(monto_ahorro_clp),
    total_medido = sum(monto_transado_ahorro)) %>%
  tidyr::pivot_wider(names_from = fecha, values_from = total_medido)

monthly_savings <- savings_avg_table %>%
  filter(year(fecha) == 2021) %>%
  group_by(fecha) %>%
  summarise(ahorro_promedio = weighted.mean(indicador,monto_transado_usd),
            total_ahorro_clp = sum(monto_ahorro_clp),
            total_transado_clp = sum(monto_transado_clp),
            total_ahorro_usd = sum(monto_ahorro_usd),
            total_transado_usd = sum(monto_transado_usd)
  )

monthly_savings %>% mutate(ahorro_dipres = total_ahorro_usd/total_transado_usd)


