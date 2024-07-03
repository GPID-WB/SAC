
bench <- microbenchmark::microbenchmark(
  times = 100,
  SAC = {
    new_value = Means_pipeline_sac(cache_inventory, cache_sac, dl_aux)
    },
  Nested = {
    old_value = Means_pipeline_tar(cache_inventory, cache, dl_aux)
    }
)
if (requireNamespace("highcharter")) {
  hc_dt <- highcharter::data_to_boxplot(bench,
                                        time,
                                        expr,
                                        add_outliers = FALSE,
                                        name = "Time in milliseconds")

  highcharter::highchart() |>
    highcharter::hc_xAxis(type = "category") |>
    highcharter::hc_chart(inverted=TRUE) |>
    highcharter::hc_add_series_list(hc_dt) |>
    highcharter::hc_title(text = "Comparison SAC vs Nested (Means)")

} else {
  boxplot(bench, outline = FALSE)
}


bench_dist <- microbenchmark::microbenchmark(
  times = 100,
  SAC = {
    new_value = Dist_stats_sac(cache_tb,
                               means_out_sac)
  },
  Nested = {
    old_value = Dist_stats_tar(cache_ls,
                               means_out_tar,
                               dl_aux,
                               cache_ids, 
                               py,
                               cache_inventory)
  })


if (requireNamespace("highcharter")) {
  hc_dt <- highcharter::data_to_boxplot(bench_dist,
                                        time,
                                        expr,
                                        add_outliers = FALSE,
                                        name = "Time in milliseconds")
  
  highcharter::highchart() |>
    highcharter::hc_xAxis(type = "category") |>
    highcharter::hc_chart(inverted=TRUE) |>
    highcharter::hc_add_series_list(hc_dt) |>
    highcharter::hc_title(text = "Comparison SAC vs Nested (Dist Stats)")
  
} else {
  boxplot(bench, outline = FALSE)
} 

bench_dist <- microbenchmark::microbenchmark(
  times = 100,
  wbpip = {
    new_value = wbpip:::md_compute_dist_stats(dt$welfare_ppp,
                                              dt$weight)
  },
  wbpip_wpr = {
    old_value = wrp_md_dist_stats(dt$welfare_ppp,
                                  dt$weight)
  })


if (requireNamespace("highcharter")) {
  hc_dt <- highcharter::data_to_boxplot(bench_dist,
                                        time,
                                        expr,
                                        add_outliers = FALSE,
                                        name = "Time in milliseconds")
  
  highcharter::highchart() |>
    highcharter::hc_xAxis(type = "category") |>
    highcharter::hc_chart(inverted=TRUE) |>
    highcharter::hc_add_series_list(hc_dt) |>
    highcharter::hc_title(text = "Comparison wbpip and wbpip wrapper (Dist Stats)")
  
} else {
  boxplot(bench, outline = FALSE)
}

bench <- microbenchmark::microbenchmark(
  times = 100,
  collapse = {
    new_value = rowbind(md_id_area, md_id_national, gd_ag_area, ag_national)
  },
  datatable = {
    old_value = rbindlist(list(md_id_area, md_id_national,
                               gd_ag_area, ag_national), use.names = TRUE)
  }
)
if (requireNamespace("highcharter")) {
  hc_dt <- highcharter::data_to_boxplot(bench,
                                        time,
                                        expr,
                                        add_outliers = FALSE,
                                        name = "Time in milliseconds")
  
  highcharter::highchart() |>
    highcharter::hc_xAxis(type = "category") |>
    highcharter::hc_chart(inverted=TRUE) |>
    highcharter::hc_add_series_list(hc_dt) |>
    highcharter::hc_title(text = "Comparison rowbind and rbindlist")
  
} else {
  boxplot(bench, outline = FALSE)
}

bench <- microbenchmark::microbenchmark(
  times = 100,
  datatable = {
    dt[dt$area=="","area"] <- "national"
  },
  collapse = {
    setv(dt2$area,"", "national")
  }
)
if (requireNamespace("highcharter")) {
  hc_dt <- highcharter::data_to_boxplot(bench,
                                        time,
                                        expr,
                                        add_outliers = FALSE,
                                        name = "Time in milliseconds")
  
  highcharter::highchart() |>
    highcharter::hc_xAxis(type = "category") |>
    highcharter::hc_chart(inverted=TRUE) |>
    highcharter::hc_add_series_list(hc_dt) |>
    highcharter::hc_title(text = "Comparison setv")
  
} else {
  boxplot(bench, outline = FALSE)
}

# ---- After Andres function

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

get_cache_tb <- function(cache) {
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # computations   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  cache_tb <- rowbind(cache, fill = TRUE)
  
  cache_tb <- cache_tb |>
    fselect(welfare, welfare_ppp, weight, survey_id, cache_id, country_code,
            surveyid_year, survey_acronym, survey_year, welfare_type,
            distribution_type, gd_type, imputation_id, cpi_data_level,
            ppp_data_level, gdp_data_level, pce_data_level,
            cpi, ppp)
  
  # Unlist in two lists
  
  dt_ls <- list()
  
  dt_ls[["micro_imputed"]] <- cache_tb|>
    fsubset(distribution_type %in% c("micro","imputed"))
  
  dt_ls[["group_aggregate"]] <- cache_tb|>
    fsubset(distribution_type %in% c("group","aggregate"))
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(dt_ls)
  
}



mb( 
  firts_rb = {get_cache_tb(cache)}, 
  last_rb = {get_cache(cache)}, 
  times = 100L
)