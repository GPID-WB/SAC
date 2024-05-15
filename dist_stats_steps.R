
  # SET-UP
  cache_tb <- pipload::pip_load_cache("NGA", version = '20240326_2017_01_02_PROD') 
  mean_table <- svy_mean_ppp_table_sac
  pop_table <- dl_aux$pop
  ppp_year <- py
  
  # 1. Unique means ----
  mean_table <- mean_table |>
    fselect(cache_id, surveyid_year, reporting_level, survey_mean_ppp)|>
    fgroup_by(cache_id, surveyid_year, reporting_level)|>
    fsummarise(survey_mean_ppp = first(survey_mean_ppp))
  
  # 2. Joyn with means ----  
  cache_dt <- cache_tb |>
    fselect(cache_id, country_code, surveyid_year, distribution_type, imputation_id,
            reporting_level, pop_data_level, area, welfare, weight, welfare_ppp) |>
    joyn::joyn(mean_table,
               by=c("cache_id", "surveyid_year", "reporting_level"), # by "area" needs to be added back here 
               y_vars_to_keep = "survey_mean_ppp",
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left")
  
  
  # 3. Micro Data ----
  # welfare needs to be ordered
  data.table::setorder(cache_dt, cache_id, country_code, surveyid_year, distribution_type, 
                       imputation_id, reporting_level, area, welfare_ppp)
  
  md_dt_area<- cache_dt[
    distribution_type == 'micro',  
    .(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = unique(survey_mean_ppp)
    ))),
    by = .(cache_id, country_code, surveyid_year, area)][, c(.SD, .(
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, country_code, surveyid_year, area)] |>
    dcast(cache_id + country_code + surveyid_year + area ~ Statistic, 
          value.var = "Value")
  
  # welfare needs to be ordered (not by area for national)
  data.table::setorder(cache_dt, cache_id, country_code, surveyid_year, distribution_type, 
                       imputation_id, reporting_level, welfare_ppp)
  
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
  
  # 5. Imputed Data ----
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

  
  # 5. Group Data -----
  ## Add pop table ----
  cache_dt <- cache_dt |>
    fselect(cache_id, country_code, surveyid_year, distribution_type, imputation_id,
            reporting_level, pop_data_level, area, welfare, weight, welfare_ppp, survey_mean_ppp) |>
    fmutate(year = surveyid_year) |>
    joyn::joyn(pop_table,
               by=c("country_code", "year","pop_data_level"), # by "area" needs to be added back here 
               y_vars_to_keep = "pop",
               match_type = "m:1",
               keep = "left",
               reportvar = FALSE)

  
  gd_dt <- cache_dt[
    distribution_type == 'group',
    .(synth = list(wbpip:::sd_create_synth_vector(welfare = welfare_ppp,
                                             population = weight,
                                             mean = funique(survey_mean_ppp),
                                             pop = funique(pop)))),
    
      by = .(cache_id, country_code, surveyid_year, reporting_level)]
  
  
  
  
  
  
  
  
  # identify procedure
  # source     <- gsub("(.*_)([A-Z]+$)", "\\2", cache_id) # distribution level already in dataset
  data_level <- gsub("(.*_)(D[123])(.+$)", "\\2", cache_id) # not sure what this is
  
  # Extract PPP means with reporting_level
  ci    <- cache_id
  mean_ppp  <- mean_table[cache_id == ci,
                          survey_mean_ppp ]
  
  names(mean) <- mean_table[cache_id == ci,
                            reporting_level]
  
  
  # Order by population data level
  data.table::setorder(, pop_data_level, welfare_ppp)
  pop_level <- unique(as.character(dt[[1]]$pop_data_level)) # it might be multiple levels
  
  mean_table |>
    fselect(cache_id, country_code, survey_year, survey_mean_ppp, reporting_level)->mean_table_extract
  
  # CACHE_TB
  # LEVEL RESULTS ----
  ## Equivalent of get_dist_stats_by_level
  ## GROUP DATA ----
  gd_dt <- cache_tb[
    distribution_type == 'group',
    .(res = list(wbpip:::gd_compute_dist_stats(  
      welfare = welfare,
      #weight = weight,
      mean = mean_ppp  
    ))),
    by = .(country_code, surveyid_year, imputation_id, reporting_level, area)]
  ]
    
mean_ppp <- 2.09  
  
  ## MICRO DATA ----
  md_dt<- cache_tb[
    distribution_type == 'micro',  
    .(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = mean_ppp  # this needs to be picked up from another table according to cache_id (which is a variable in cache_tb)
    ))),
    by = .(country_code, surveyid_year, imputation_id, area)][, c(.SD, .(
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(country_code, surveyid_year, imputation_id, area)] |>
    dcast(country_code + surveyid_year + imputation_id + area ~ Statistic, value.var = "Value")
  
  ## GROUP DATA ----
  
  
  # MICRO DATA ----
  ## national reporting_level ----
  
  ### dt ----
  md_dt<- cache_tb[
    distribution_type == 'micro' & reporting_level == 'national',  
    .(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = mean_ppp  
    ))),
    by = .(country_code, surveyid_year, imputation_id, area)][, c(.SD, .(
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
    by = .(country_code, surveyid_year, imputation_id, area)] |>
    dcast(country_code + surveyid_year + imputation_id + area ~ Statistic, value.var = "Value")
  
  
  
  

  
  
  
  
  # get estimates by level
  res <- purrr::map(
    .x = pop_level,
    .f = ~ get_dist_stats_by_level(dt, 
                                   mean, 
                                   source, 
                                   level = .x, 
                                   ppp_year = ppp_year)
  )
  
  names(res) <- pop_level
  
  if (data_level != "D1" && length(pop_level) > 1) { # Urban/rural or subnat level
    
    if (source == "GROUP") { # Group data
      
      # create synthetic vector
      wf <- purrr::map(
        .x = pop_level,
        .f = ~ get_synth_vector(dt, pop_table, mean, level = .x)
      ) |> 
        rbindlist()
      # data.table::setDT(wf)
      wf[,
         welfare_ppp := welfare
      ][, 
        imputation_id := ""]
      
    } else { # microdata
      
      wf <- data.table::copy(dt)
    }
    
    n_imid <- collapse::fnunique(wf$imputation_id) # Number of imputations id
    
    data.table::setorder(wf, imputation_id, welfare_ppp) # Data must be sorted
    if (n_imid == 1) {
      # national mean
      res_national <- md_dist_stats(wf, ppp_year = ppp_year)
    } else {
      # national mean
      res_national <- id_dist_stats(wf, ppp_year = ppp_year)
    }
    
    
    res <- append(list(res_national), res)
    names(res) <- c("national", pop_level)
  }
  
  return(res)
}
