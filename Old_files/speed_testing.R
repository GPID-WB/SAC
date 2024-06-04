
bench <- microbenchmark::microbenchmark(
  times = 100,
  SAC = {
    new_value = Means_pipeline_sac(cache_inventory, cache_tb, dl_aux)
    },
  Nested = {
    old_value = Means_pipeline_tar(cache_inventory, cache_ls, dl_aux)
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

