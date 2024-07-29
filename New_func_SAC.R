db_stats_sac <- function(cache, 
                         gd_mean) {

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Means LCU   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # Select variables for metadata
  metadata_vars <- c("cache_id", "reporting_level", 
                     #"area",
                     "survey_id", "country_code", "surveyid_year", 
                     "survey_acronym","survey_year", "welfare_type", 
                     "distribution_type","gd_type","cpi_data_level",
                     "ppp_data_level", "gdp_data_level", 
                     "pce_data_level", "pop_data_level",
                     "cpi", "ppp")
  
  # ---- Micro data no urban/rural -----
  
  dt_c <- cache[["micro_imputed"]] |>
    fgroup_by(cache_id, reporting_level,
              imputation_id)|> 
    #collapg(custom = list(fmean = c(survey_mean_lcu = "welfare")), w = weight)|>
    fsummarise(survey_mean_lcu = fmean(welfare, w = weight), 
               weight          = fsum(weight)) |>
    fgroup_by(cache_id, reporting_level)|>
    #collapg(custom = list(fmean = c(survey_mean_lcu = "survey_mean_lcu")), w = weight)|>
    fsummarise(survey_mean_lcu = fmean(survey_mean_lcu, w = weight)) |>
    fungroup()
  
  dt_meta_vars <- cache[["micro_imputed"]] |>
    get_vars(metadata_vars) |>
    funique()
  
  dt_c <- joyn::joyn(dt_meta_vars, dt_c,
                     by = c("cache_id", "reporting_level"),
                     match_type = "m:1",
                     reportvar = FALSE)
  
  #  ------ Group data -----
  
  if(nrow(cache[["group_aggregate"]])!=0){
    
    dt_g <- cache[["group_aggregate"]] |>
      joyn::joyn(gd_mean[!is.na(survey_mean_lcu)],
                 by = c(
                   "cache_id", "pop_data_level"
                 ),
                 y_vars_to_keep = "survey_mean_lcu",
                 match_type = "m:1", 
                 keep = "left", 
                 reportvar = FALSE, 
                 sort = FALSE)
    
    dt_c <- collapse::rowbind(dt_c, dt_g) 
    
  }

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(TRUE)

}