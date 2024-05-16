# Data.table version ----
## 4. National Data ----
md_dt_national<- cache_dt[
  distribution_type == 'micro',  
  .(res = list(wbpip:::md_compute_dist_stats(  
    welfare = welfare_ppp,
    weight = weight,
    mean = unique(survey_mean_ppp)
  ))),
  by = .(cache_id, country_code, surveyid_year)][, c(.SD, .(
    Statistic = names(unlist(res)), 
    Value = unlist(res))),
    by = .(cache_id, country_code, surveyid_year)] |>
  dcast(cache_id + country_code + surveyid_year ~ Statistic, 
        value.var = "Value")|>
  fmutate(area = 'national')

## 5. Imputed Data ----
id_dt_national<- cache_dt[
  distribution_type == 'imputed',  
  .(res = list(wbpip:::md_compute_dist_stats(  
    welfare = welfare_ppp,
    weight = weight,
    mean = unique(survey_mean_ppp)
  ))),
  by = .(cache_id, country_code, surveyid_year, imputation_id)][, c(.SD, .(
    Statistic = names(unlist(res)), 
    Value = unlist(res))),
    by = .(cache_id, country_code, surveyid_year, imputation_id)] |>
  dcast(cache_id + country_code + surveyid_year + imputation_id ~ Statistic, 
        value.var = "Value")|> # using collapse because grouping should be faster
  fgroup_by(cache_id, country_code, surveyid_year)|>
  fsummarise(across(gini:quantiles9, fmean))