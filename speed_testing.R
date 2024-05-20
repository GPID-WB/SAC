
bench <- microbenchmark::microbenchmark(
  times = 100,
  dt = {
    new_value = db_compute_survey_mean_sac_dt(cache_tb = cache_tb, gd_mean = gd_mean)
    },
  collapse = {
    old_value = db_compute_survey_mean_sac_col(cache_tb = cache_tb, gd_mean = gd_mean)
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
    highcharter::hc_title(text = "Comparison db_compute_survey_mean_sac")

} else {
  boxplot(bench, outline = FALSE)
}

