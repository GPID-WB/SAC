
db_compute_survey_mean_sac_pipe_transform <- function(cache_tb, gd_mean = NULL) {
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # computations   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # Select variables
  dt <- cache_tb[, .(welfare, weight, survey_id, cache_id, country_code, 
                     surveyid_year, survey_acronym, survey_year, welfare_type,
                     distribution_type, gd_type, imputation_id, cpi_data_level, 
                     ppp_data_level, gdp_data_level, pce_data_level, 
                     pop_data_level, reporting_level, area)]
  
  keep_vars <- c("survey_id", "country_code", "surveyid_year", 
                 "survey_acronym","survey_year", "welfare_type", 
                 "distribution_type","gd_type","cpi_data_level",
                 "ppp_data_level", "gdp_data_level", 
                 "pce_data_level", "pop_data_level")
  
  # For micro data
  dt_m <- dt %>%
    fsubset(distribution_type == "micro")%>%
    fgroup_by(cache_id, reporting_level, area)%>%
    fsummarize(across(keep_vars, funique), 
               survey_mean_lcu_a = fmean(welfare, w = weight, na.rm = TRUE),
               weight_a = fsum(weight))%>%
    fungroup()|>
    fgroup_by(cache_id, reporting_level) 
  
  # Debug: print intermediate dt_m
  #print(dt_m)
  
  dt_m <- dt_m %>% ftransform(survey_mean_lcu = fmean(survey_mean_lcu_a,
                                                     g = GRP(.),
                                                     w = weight_a,
                                                     TRA = 'replace_fill'))
  
  # For imputations
  imp_mean <- dt |>
    fsubset(distribution_type=="imputed")|> 
    fgroup_by(cache_id, reporting_level, 
              imputation_id)|> # Should I include area here?
    fsummarise(survey_mean_imp = fmean(welfare, w = weight, na.rm = TRUE))
  
  dt_i <- dt %>%
    joyn::joyn(imp_mean,
               by = c(
                 "cache_id","reporting_level",
                 "imputation_id"
               ),
               y_vars_to_keep = "survey_mean_imp",
               match_type = "m:1", reportvar = FALSE)%>%
    fsubset(distribution_type == "imputed")%>%
    fgroup_by(cache_id, reporting_level, area)%>%
    fsummarize(across(keep_vars, funique),
               survey_mean_lcu_a = fmean(survey_mean_imp, na.rm = TRUE),
               weight_a = fsum(weight))%>%
    fungroup()%>%
    fgroup_by(cache_id, reporting_level)%>% 
    ftransform(survey_mean_lcu = fmean(survey_mean_lcu_a,
                                                     g = GRP(.),
                                                     w = weight_a,
                                                     TRA = 'replace_fill'))
  
  
  dt_g <- dt |>
    fsubset(distribution_type == "group" | distribution_type == "aggregate")|>
    joyn::joyn(gd_means_sac,
               by = c(
                 "cache_id"
               ),
               y_vars_to_keep = "survey_mean_lcu",
               match_type = "m:1", keep = "left", 
               reportvar = FALSE)|>
    fmutate(survey_mean_lcu_a = survey_mean_lcu)|>
    fsummarize(across(c(keep_vars, "cache_id", "reporting_level",
                        "area", "survey_mean_lcu" ,"survey_mean_lcu_a"), funique),
               weight_a = fsum(weight))|>
    fungroup()
  
  dt_c <- rbind(dt_m, dt_i, dt_g)
  # Note: We can eliminate dt_g if needed.
  
  # Order columns
  data.table::setcolorder(
    dt_c, c(
      "survey_id", "country_code", "surveyid_year", "survey_acronym",
      "survey_year", "welfare_type", "survey_mean_lcu"
    )
  )
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(dt_c)
}

result_pipe_tranform <- db_compute_survey_mean_sac_pipe_transform(cache_tb = cache_tb,
                                                                 gd_mean = gd_means_sac )