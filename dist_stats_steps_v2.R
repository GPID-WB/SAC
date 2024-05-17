# 1. Arguments ----
cache_tb <- pipload::pip_load_cache("NGA", version = '20240326_2017_01_02_PROD') # micro, group, imputed
cache_CHN <- pipload::pip_load_cache("CHN", version = '20240326_2017_01_02_PROD') # aggregate
mean_table <- svy_mean_ppp_table_sac
# pop_table <- dl_aux$pop reporting_pop in mean_table
ppp_year <- py


# 2. Check which distribution types and which areas (levels) in the set
distributions <- funique(cache_tb$distribution_type)
areas <- funique(cache_tb$area)

# 3. Fill area -----
# Fill area with national when empty and reporting_level for aggregate distribution
cache_tb <- ftransform(cache_tb, area = ifelse(distribution_type == "aggregate",
                                               as.character(reporting_level),
                                               ifelse(as.character(area) == "",
                                                      "national", as.character(area))))

#cache_CHN <- ftransform(cache_CHN, area = ifelse(distribution_type == "aggregate",
#                                               as.character(reporting_level),
#                                               ifelse(as.character(area) == "",
#                                                      "national", as.character(area))))

if ("micro" %in% distributions){
  # Micro data have national and level data estimations:
  if ("rural" | "urban" %in% areas) {
    # 3. Micro and Imputed Data ----
    ## 3.1 Micro Level Estimates ----
    md_dt_area<- 
      cache_tb |>
      fselect(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level, area, welfare, weight, welfare_ppp) |>
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
        mean = funique(survey_mean_ppp))))|>
      _[, c(.SD, .( # using _ because we are using native pipe 
        Statistic = names(unlist(res)), 
        Value = unlist(res))),
        by = .(cache_id, country_code, surveyid_year, distribution_type,
               reporting_level, area)] |>
      dcast(cache_id + country_code + surveyid_year + distribution_type + 
              reporting_level + area ~ Statistic, 
            value.var = "Value")
  }
  
  ## 3.2 Micro National Dist Estimates ----
  md_dt_national<- 
    cache_tb |>
    fselect(cache_id, country_code, surveyid_year, distribution_type,
            reporting_level, area, welfare, weight, welfare_ppp) |>
    fsubset(distribution_type == 'micro') |>
    joyn::joyn(mean_table |> fsubset(distribution_type == 'micro' 
                                     & area == "national"),
               by=c("cache_id", "surveyid_year", "reporting_level"), 
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
      mean = funique(survey_mean_ppp))))|>
    _[, c(.SD, .( # using _ because we are using native pipe 
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, country_code, surveyid_year, distribution_type,
             reporting_level)] |>
    dcast(cache_id + country_code + surveyid_year + distribution_type + 
            reporting_level ~ Statistic, 
          value.var = "Value")
  
}

if ("imputed" %in% distributions){
  
  id_dt_national<- 
    cache_tb |>
    fselect(cache_id, country_code, surveyid_year, distribution_type,
            reporting_level, imputation_id, area, welfare, weight, welfare_ppp) |>
    fsubset(distribution_type %in% c("imputed", "micro")) |>
    joyn::joyn(mean_table |> fsubset(distribution_type %in% c("imputed", "micro") 
                                     & area == "national"),
               by=c("cache_id", "surveyid_year", "reporting_level"), # by "area" needs to be added back here 
               y_vars_to_keep = c("survey_mean_ppp"),
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left") |>
    # welfare needs to be ordered (but not by area)
    roworder(cache_id, country_code, surveyid_year, distribution_type, 
             reporting_level, imputation_id, welfare_ppp) |>
    fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level, imputation_id)|> # no area so dist stat are the sum of both
    fsummarise(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = funique(survey_mean_ppp))))|>
    _[, c(.SD, .( # using _ because we are using native pipe 
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, country_code, surveyid_year, distribution_type,
             reporting_level, imputation_id)] |>
    dcast(cache_id + country_code + surveyid_year + distribution_type + 
            reporting_level + imputation_id ~ Statistic, 
          value.var = "Value") |>
    fgroup_by(cache_id, country_code, surveyid_year)|>
    fsummarise(across(gini:quantiles9, fmean))
    
}

if ("group" %in% distributions){
  
  gd_dt_national<- 
    cache_tb |>
    fselect(cache_id, country_code, surveyid_year, distribution_type,
            reporting_level, area, welfare, weight, welfare_ppp) |>
    fsubset(distribution_type == 'group') |>
    joyn::joyn(mean_table |> fsubset(distribution_type == 'group' 
                                     & area == "national"),
               by=c("cache_id", "surveyid_year", "reporting_level"), # by "area" needs to be added back here 
               y_vars_to_keep = c("survey_mean_ppp"),
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left") |>
    # welfare needs to be ordered (but not by area)
    roworder(cache_id, country_code, surveyid_year, distribution_type, 
             reporting_level, area, welfare_ppp) |>
    fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level, area)|> # no area so dist stat are the sum of both
    fsummarise(res = list(wbpip:::gd_compute_dist_stats(  
      welfare = welfare,
      population = weight,
      mean = funique(survey_mean_ppp))))|>
    _[, c(.SD, .( # using _ because we are using native pipe 
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, country_code, surveyid_year, distribution_type,
             reporting_level, area)] |>
    dcast(cache_id + country_code + surveyid_year + distribution_type + 
            reporting_level + area ~ Statistic, 
          value.var = "Value")

  if ("aggregate" %in% distributions){
    # Aggregate data have national, and level data estimations. 
    # National levels are calculated with the synthetic distribution
    # Level data are calculated with th
    
    
    
  }  
}