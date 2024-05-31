
bench <- microbenchmark::microbenchmark(
  times = 100,
  SAC = {
    new_value = Means_pipeline_sac(cache_inventory, cache, dl_aux)
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
  dt = {
    new_value = mp_dl_dist_stats_sac(dt = cache_tb, 
                                     mean_table = svy_mean_ppp_table_sac)
  },
  collapse = {
    old_value = mp_dl_dist_stats(dt         = cache,
                                 mean_table = svy_mean_ppp_table_tar,
                                 pop_table  = dl_aux$pop,
                                 cache_id   = cache_ids, 
                                 ppp_year   = py)
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
    highcharter::hc_title(text = "Comparison dist_stats")
  
} else {
  boxplot(bench, outline = FALSE)
}

