
  # SET-UP
  cache_tb <- pipload::pip_load_cache("NGA", version = '20240326_2017_01_02_PROD') 
  mean_table <- svy_mean_ppp_table_sac
  pop_table <- dl_aux$pop
  ppp_year <- py
  
  # 1. Unique means ----
  mean_table <- mean_table |>
    fselect(cache_id, surveyid_year, distribution_type, reporting_level, area, 
            survey_mean_ppp, reporting_pop)
  
  # 2. Joyn with means ----  
  cache_dt <- cache_tb |>
    fselect(cache_id, country_code, surveyid_year, distribution_type, imputation_id,
            reporting_level, pop_data_level, area, welfare, weight, welfare_ppp) |>
    joyn::joyn(mean_table,
               by=c("cache_id", "surveyid_year", "reporting_level", "area"), # by "area" needs to be added back here 
               y_vars_to_keep = "survey_mean_ppp",
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left")
  
  
  # 3. Micro Data ----
  ## 3.1 Area Dist Estimates ----
  # welfare needs to be ordered at the area level:
  
  # Dist calculation
  md_dt_area<- 
    cache_dt |>
    fselect(cache_id, country_code, surveyid_year, distribution_type,
            reporting_level, pop_data_level, area, welfare, weight, welfare_ppp) |>
    fsubset(distribution_type == 'micro') |>
    joyn::joyn(mean_table |> fsubset(distribution_type == 'micro'),
               by=c("cache_id", "surveyid_year", "reporting_level", "area"), 
               y_vars_to_keep = c("survey_mean_ppp"),
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left")|>
    roworder(cache_id, country_code, surveyid_year, distribution_type,
             reporting_level, area, welfare_ppp) |>
    fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level, area)|>
    fsummarise(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = unique(survey_mean_ppp))))
  
  ## 3.2 National Dist Estimates ----
  md_dt_national<- 
    cache_dt |>
    fselect(cache_id, country_code, surveyid_year, distribution_type,
            reporting_level, pop_data_level, area, welfare, weight, welfare_ppp) |>
    fsubset(distribution_type == 'micro') |>
    joyn::joyn(mean_table |> fsubset(distribution_type == 'micro'),
               by=c("cache_id", "surveyid_year", "reporting_level", "area"), # by "area" needs to be added back here 
               y_vars_to_keep = c("survey_mean_ppp"),
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left") |>
    # welfare needs to be ordered (but not by area)
    roworder(cache_id, country_code, surveyid_year, distribution_type, 
             reporting_level, welfare_ppp) |>
    fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level)|> # no area so dist stat are the sum of both
    fsummarise(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = unique(survey_mean_ppp))))
    
  
  # 5. Imputed Data ----
  id_dt<-
    cache_dt |>
    fselect(cache_id, country_code, surveyid_year, distribution_type, imputation_id,
            reporting_level, pop_data_level, area, welfare, weight, welfare_ppp) |>
    fsubset(distribution_type == 'imputed') |>
    joyn::joyn(mean_table |> fsubset(distribution_type == 'imputed'),
               by=c("cache_id", "surveyid_year", "reporting_level"),
               y_vars_to_keep = c("survey_mean_ppp"),
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left") |>
    roworder(cache_id, country_code, surveyid_year, distribution_type, 
             reporting_level, imputation_id, welfare_ppp) |>
    fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level, imputation_id)|> # no area so dist stat are the sum of both
    fsummarise(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = unique(survey_mean_ppp))))|>
      _[, c(.SD, .( # using _ because we are using native pipe
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, country_code, surveyid_year, imputation_id)] |>
    dcast(cache_id + country_code + surveyid_year + imputation_id ~ Statistic, 
          value.var = "Value")|> # using collapse because grouping should be faster
    fgroup_by(cache_id, country_code, surveyid_year)|>
    fsummarise(across(gini:quantiles9, fmean))
    

  
  # 5. Group Data -----
  ## Add pop table ----
  gd_dt <- 
    cache_dt |>
    fselect(cache_id, country_code, surveyid_year, distribution_type, imputation_id,
            reporting_level, pop_data_level, area, welfare, weight, welfare_ppp) |>
    fsubset(distribution_type == 'group') |>
    joyn::joyn(mean_table |> fsubset(distribution_type == 'imputed'),
               by=c("cache_id", "surveyid_year", "reporting_level"),
               y_vars_to_keep = c("survey_mean_ppp", "reporting_pop"),
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left") |>
    roworder(cache_id, country_code, surveyid_year, distribution_type, 
             reporting_level, welfare) |>
    fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level)|> 
    fsummarise(res = list(wbpip:::gd_compute_dist_stats(  
      welfare = welfare,
      population = weight,
      mean = unique(survey_mean_ppp))))
    

  

  
  
  
  
  
  
 