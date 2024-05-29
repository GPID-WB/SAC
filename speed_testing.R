
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

