


library(ggplot2)

# monthly total savings
 monthly_savings %>%
  group_by(convenio) %>%
  summarise(monto_ahorrado = sum(monto_ahorro)) %>%
  ggplot(aes(x = reorder(convenio,-monto_ahorrado), y = monto_ahorrado, fill = convenio)) +
    geom_bar(stat = 'identity', colour = "black") +
    geom_text(aes(label = paste0(format_number(monto_ahorrado/1000000, digit = 0, is_percent = FALSE),'MM')
                  ), vjust = 1.5) +
    labs(title = 'Ahorro Total Convenios Marco',
         subtitle = 'Septiembre 2021',
         x = '',
         y = '',
         fill = '') +
    theme(axis.text.y = element_blank(),
          axis.ticks = element_blank(),
          legend.position = "none") +
    scale_fill_brewer(palette = "Pastel1")
 
# monthly savings distribution
 monthly_savings %>%
   ggplot(aes(x = ahorro_promedio, color = convenio, fill = convenio)) +
   geom_density(alpha = 0.5) +
   geom_vline(xintercept = 0, linetype = 'dashed', color = 'red') +
   labs(title = 'Distribucion de ahorro por convenio',
        subtitle = 'Septiembre 2021') +
   scale_x_continuous(labels = scales::percent) +
   theme_bw()