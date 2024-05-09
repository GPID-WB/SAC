
bench <- microbenchmark::microbenchmark(
  times = 100,
  index = {
    new_value = get_groupdata_means(cache_inventory = cache_inventory, gdm = dl_aux$gdm)
    },
  old = {
    old_value = get_groupdata_means_sac(cache_inventory = cache_inventory, gdm = dl_aux$gdm)
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
    highcharter::hc_title(text = "Comparison get_groupdata_means")

} else {
  boxplot(bench, outline = FALSE)
}

