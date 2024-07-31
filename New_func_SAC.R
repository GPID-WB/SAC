db_stats_sac <- function(cache, 
                         gd_mean,
                         pop_table) {

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
  # Means PPP   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  #--------- Deflate welfare mean ---------
  
  dt_c <- fmutate(dt_c, survey_mean_ppp = survey_mean_lcu / ppp / cpi)
  
  # ---- Merge with PFW ----
  
  # Select columns and merge LCU table with PFW (left join)
  dt_c <- joyn::joyn(dt_c, pfw_table|>
                     fselect(wb_region_code, pcn_region_code,
                             country_code, survey_coverage,
                             surveyid_year, survey_acronym,
                             reporting_year, survey_comparability,
                             display_cp, survey_time),
                   by = c(
                     "country_code",
                     "surveyid_year",
                     "survey_acronym"
                   ),
                   match_type = "m:1"
  )
  
  if (nrow(dt_c[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure PFW table is up to date"
    rlang::abort(c(
      msg,
      i = hint,
      i = "Make sure .dta data is up to date by running pipdp"
    ),
    class = "pipdm_error"
    )
  }
  
  dt_c <- dt_c|>
    fsubset(.joyn != "y")|>
    fselect(-.joyn)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Population   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  #--------- Merge with POP ---------
  
  pop_table$pop_domain <- NULL 
  
  # --- Reporting_pop ----
  
  dt_c <- joyn::joyn(dt_c, pop_table,
                   by = c("country_code",
                          "reporting_year = year",
                          #"area = pop_data_level"
                          "pop_data_level"
                   ),
                   match_type = "m:1"
                   #keep = "left"
  )
  
  #There is an error for the area level (see if it affects later on)
  #if (nrow(dt_c[(.joyn == "x" & reporting_level==area)]) > 0) { 
  if (nrow(dt_c[.joyn == "x"]) > 0) { 
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure POP data includes all the countries and pop data levels"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }
  
  dt_c <- dt_c|>
    fsubset(.joyn != "y")|>
    fselect(-.joyn)|>
    setnames("pop", "reporting_pop")
  
  # ---- Survey_pop ----
  
  dt_svy_pop <- dt_c|>
    fsubset(survey_year != floor(survey_year)) |>
    rowbind(dt_c|> fsubset(survey_year != floor(survey_year)), idcol = "id")|>
    fmutate(year_rnd = case_when(id == 1 ~ ceiling(survey_year),
                                 id == 2 ~ floor(survey_year),
                                 .default = NA_integer_),
            diff = 1 - abs(survey_year-year_rnd))|>
    joyn::joyn(pop_table, 
               by = c("country_code", 
                      "year_rnd = year",
                      #"area = pop_data_level"
                      "pop_data_level"
               ),
               match_type = "m:1",
               keep = "left"
    )
  
  
  if (nrow(dt_svy_pop[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure POP data includes all the countries and pop data levels"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }
  
  dt_svy_pop <- dt_svy_pop|>
    fgroup_by(survey_id, country_code, survey_year,
              #reporting_level, area)|>
              reporting_level)|>
    collapg(custom = list(fmean = "pop"), w = diff)|>
    frename(survey_pop = pop)|>
    fungroup()
  
  dt_c <- joyn::joyn(dt_c, dt_svy_pop,
                   by = c("survey_id", 
                          "country_code", 
                          "survey_year",
                          "reporting_level"
                          #"area"
                   ),
                   match_type = "m:1",
                   keep = "left"
  )
  
  dt_c <- dt_c|>
    fsubset(.joyn != "y")|>
    fselect(-.joyn)
  
  # ---- Finalize table ----
  
  dt_c <- dt_c |>
    ftransform(survey_pop = fifelse(is.na(survey_pop),
                                    reporting_pop, survey_pop))|>
    ftransform(reporting_pop = survey_pop)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Distributional Stats   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # 1. Select variables and subset for Micro data----
  
  dt_m <- cache[["micro_imputed"]] |>
    fselect(cache_id, distribution_type, 
            cpi_data_level, ppp_data_level,
            gdp_data_level, pce_data_level,
            pop_data_level, reporting_level,
            imputation_id, weight, welfare_ppp)
  
  # 2. Micro Data: Level Estimation  ----
  
  md_id_level <- dt_m |>
    roworder(cache_id, pop_data_level, welfare_ppp) |>
    _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp,
                                  weight  = weight,
                                  mean = NULL)),
      by = .(cache_id, imputation_id, 
             pop_data_level, reporting_level)]|>
    fgroup_by(cache_id, 
              pop_data_level, reporting_level)|>
    collapg(fmean, cols = c("mean","median","gini",
                            "polarization","mld",
                            paste0("decile",1:10)))|>
    fungroup()|>
    frename(survey_median_ppp = median)|>
    fmutate(reporting_level = as.character(reporting_level),
            pop_data_level = as.character(pop_data_level))
  
  
  # 3. Micro and Imputed Data: National Estimation ----
  
  md_id_national <- dt_m |>
    # Gc Note: this is equivalent to having pop_data_level > 1 and D2 in cache_id:
    fsubset(reporting_level != 'national' & ppp_data_level != 'national') |>
    roworder(cache_id, imputation_id, welfare_ppp)|>
    _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp, weight = weight)),
      by = .(cache_id, imputation_id)]|>
    fgroup_by(cache_id)|>
    collapg(fmean, cols = c("mean","median","gini",
                            "polarization","mld",
                            paste0("decile",1:10)))|>
    fungroup()|>
    frename(survey_median_ppp = median)|>
    fmutate(reporting_level = as.character("national"),
            pop_data_level = as.character("national")) 
  
  if(nrow(cache[["group_aggregate"]])!=0){
    
    # Select variables, subset and join mean table
    dt_jn <- cache[["group_aggregate"]] |>
      fselect(cache_id, distribution_type, imputation_id, 
              pop_data_level, reporting_level, weight, welfare) |>
      collapse::join(mean_table |> 
                       fselect(cache_id, 
                               pop_data_level, reporting_level, 
                               survey_mean_ppp, reporting_pop),
                     on=c("cache_id", 
                          "pop_data_level", "reporting_level"), 
                     # GC Note: it is actually over-identified at this stage as well.
                     validate = "m:1",
                     verbose = 0,
                     overid = 2,
                     column = list(".joyn", c("x", "y", "x & y"))) # immediate
    
    # MISSING WARNING MESSAGE
    
    dt_jn <- dt_jn|>
      fsubset(.joyn != "y")|>
      fselect(-.joyn)
    
    
    # 4. Group and Aggregate Data: Level and Area Estimation -----
    
    gd_ag_level <- dt_jn |>
      roworder(cache_id, 
               pop_data_level, reporting_level, welfare) |>
      _[, as.list(safe_wrp_gd_dist_stats(welfare = welfare,
                                         population = weight,
                                         mean = funique(survey_mean_ppp))),
        by = .(cache_id, 
               pop_data_level, reporting_level)]|>
      frename(survey_median_ppp = median)|>
      fmutate(reporting_level = as.character(reporting_level),
              pop_data_level = as.character(pop_data_level))
    
    
    setrename(gd_ag_level, gsub("deciles", "decile", names(gd_ag_level)))
    
    # 4. Aggregate Data: National estimation (synth needed) ----
    
    ag_syn <- dt_jn |>
      fsubset(distribution_type %in% c("aggregate")) |>
      roworder(cache_id, 
               pop_data_level, reporting_level, welfare) |>
      fgroup_by(cache_id, 
                pop_data_level, reporting_level)|>
      fsummarise(welfare =  wbpip:::sd_create_synth_vector(
        welfare = welfare,
        population = weight,
        mean = funique(survey_mean_ppp),
        pop = funique(reporting_pop))$welfare,
        weight = funique(reporting_pop)/100000) 
    
    # Aggregate to national
    
    ag_national <- ag_syn |> 
      fsubset(!is.na(welfare))|> # Patch to eliminate NA from IDN error
      roworder(cache_id, welfare)|>
      _[, as.list(wrp_md_dist_stats(welfare = welfare, weight = weight)),
        by = .(cache_id)]|>
      fgroup_by(cache_id)|>
      collapg(fmean, cols = c("mean","median","gini",
                              "polarization","mld",
                              paste0("decile",1:10)))|>
      fungroup()|>
      frename(survey_median_ppp = median)|>
      fmutate(reporting_level = as.character("national"),
              pop_data_level = as.character("national")) 
    
    # 5. Row bind and return ----
    
    dists_final <- rowbind(md_id_level, md_id_national, gd_ag_level, ag_national)
    
  }
  
  dists_final <- rowbind(md_id_level, md_id_national)
  
  dt_clean <- dists_final |>
    collapse::join(dt_c ,
                   on=c("cache_id", "reporting_level", "pop_data_level"),
                   validate = "1:1",
                   how = "left",
                   verbose = 0,
                   overid = 2)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(dt_clean)

}