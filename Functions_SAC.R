
# ---------------------------- FUNCTIONS SAC -------------------------

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
#  1. Survey_means  ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1 gd_means --------
## 
## Objective: Fetch Group Data survey means and convert them to daily values

get_groupdata_means_sac <- function(cache_inventory = cache_inventory, gdm = dl_aux$gdm){
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # computations   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  dt. <- joyn::joyn(x          = cache_inventory,
                    y          = gdm,
                    by         = c("survey_id", "welfare_type"),
                    match_type = "1:m",
                    y_vars_to_keep = c("survey_mean_lcu", "pop_data_level"),
                    keep       = "left")
  
  data.table::setorder(dt., cache_id, pop_data_level)
  gd_means        <- dt.[,.(cache_id, pop_data_level, survey_mean_lcu)]
  gd_means        <- gd_means[,survey_mean_lcu:= survey_mean_lcu*(12/365)]
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(gd_means)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2 svy_mean_lcu --------
##
## Objective:  Local Currency Unit survey mean list

db_compute_survey_mean_sac <- function(cache_tb, gd_mean = NULL) {
  
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
  
  # Modify the area variable for imputed or group
  
  levels(dt$area)[levels(dt$area)==""] <- "national"
  
  # Need to fix this if not same as reference_level (aggregate)
  
  # dt[, area := ifelse(reporting_level!=area, reporting_level, area)] 
  
  # For micro data
  
  dt_m <- dt |>
    fsubset(distribution_type=="imputed" | distribution_type == "micro")|> 
    fgroup_by(cache_id, reporting_level, area, imputation_id)|> 
    fsummarise(survey_mean_lcu = fmean(welfare, w = weight, na.rm = TRUE),
               weight = fsum(weight),
               across(keep_vars, funique))|>
    fgroup_by(cache_id, reporting_level, area)|>
    fsummarize(across(c(keep_vars), funique), 
               survey_mean_lcu = fmean(survey_mean_lcu, w = weight, na.rm = TRUE),
               weight = fsum(weight))|>
    fungroup()
  
  # For national
  
  dt_nat <- dt_m |>
    fsubset(area != "national" & reporting_level == "national")|>
    fgroup_by(cache_id, reporting_level)|>
    fsummarise(survey_mean_lcu = fmean(survey_mean_lcu, w = weight, na.rm = TRUE),
               weight = fsum(weight),
               across(c(keep_vars), funique))|>
    fungroup()|>
    fmutate(area = factor("national"))
  
  # For group
  
  dt_g <- dt |>
    fsubset(distribution_type == "group" | distribution_type == "aggregate")|>
    joyn::joyn(gd_mean[!is.na(survey_mean_lcu)],
               by = c(
                 "cache_id", "pop_data_level"
               ),
               y_vars_to_keep = "survey_mean_lcu",
               match_type = "m:1", keep = "left", 
               reportvar = FALSE, sort = FALSE)|>
    fgroup_by(cache_id, reporting_level, pop_data_level)|>
    fsummarize(across(c(keep_vars[!(keep_vars %in% "pop_data_level")], "area",  
                        "survey_mean_lcu"), funique),
               weight = fsum(weight))
  
  dt_c <- collapse::rowbind(dt_m, dt_nat, dt_g)
  # Note: We can eliminate dt_g if needed.
  
  dt_c <- dt_c|>
    ftransform(area = factor(as.character(area), levels = union(levels(area), levels(reporting_level))),
               reporting_level = factor(as.character(reporting_level), levels = union(levels(area), levels(reporting_level)))) 
  
  # Order rows
  dt_c <- dt_c|>
    roworder(survey_id, country_code, surveyid_year, survey_acronym,
             survey_year, welfare_type)
  
  # Order columns
  data.table::setcolorder(
    dt_c, c(
      "survey_id", "country_code", "surveyid_year", "survey_acronym",
      "survey_year", "welfare_type"
    )
  )
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(dt_c)
  
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.3 svy_mean_lcu_table --------
##
## Objective: Add auxiliary data (pwf and pop)

db_create_lcu_table_sac <- function(dt, pop_table, pfw_table) {
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # computations   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # ---- Merge with PFW ----
  
  # Select columns
  pfw_table <-
    pfw_table[, c(
      "wb_region_code", "pcn_region_code",
      "country_code", "survey_coverage",
      "surveyid_year", "survey_acronym",
      "reporting_year", "survey_comparability",
      "display_cp", "survey_time"
    )]
  
  # Merge LCU table with PFW (left join)
  dt <- joyn::joyn(dt, pfw_table,
                   by = c(
                     "country_code",
                     "surveyid_year",
                     "survey_acronym"
                   ),
                   match_type = "m:1"
  )
  
  if (nrow(dt[.joyn == "x"]) > 0) {
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
  
  dt <- dt[
    .joyn != "y" 
  ][, .joyn := NULL]
  
  #--------- Merge with POP ---------
  
  pop_table$pop_domain <- NULL 
  
  # --- Reporting_pop ----
  
  dt <- joyn::joyn(dt, pop_table,
                   by = c("country_code", 
                          "reporting_year = year",
                          "area = pop_data_level"
                   ),
                   match_type = "m:1",
                   keep = "left"
  )
  
  if (nrow(dt[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure POP data includes all the countries and pop data levels"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }
  
  dt <- dt[
    .joyn != "y" ][, .joyn := NULL]
  
  data.table::setnames(dt, "pop", "reporting_pop")
  
  # ---- Survey_pop ----
  
  dt_svy_pop <- dt[survey_year != floor(survey_year),] |>
    rowbind(dt[survey_year != floor(survey_year),], idcol = "id")|>
    fmutate(year_rnd = case_when(id == 1 ~ ceiling(survey_year),
                                 id == 2 ~ floor(survey_year),
                                 .default = NA_integer_),
            diff = 1 - abs(survey_year-year_rnd))|>
    joyn::joyn(pop_table, # Need warning in case join == x
               by = c("country_code", 
                      "year_rnd = year",
                      "area = pop_data_level"
               ),
               match_type = "m:1",
               keep = "left"
    ) |>
    fgroup_by(survey_id, country_code, survey_year,
              reporting_level, area)|>
    fsummarize(survey_pop = fmean(pop, w = diff))|>
    fungroup()
  
  dt <- joyn::joyn(dt, dt_svy_pop,
                   by = c("survey_id", 
                          "country_code", 
                          "survey_year",
                          "reporting_level",
                          "area"
                   ),
                   match_type = "m:1",
                   keep = "left"
  )
  
  dt <- dt[
    .joyn != "y" ][, .joyn := NULL]
  
  dt <- ftransform(dt, survey_pop = fifelse(is.na(survey_pop), 
                                            reporting_pop, survey_pop))
  
  # ---- Finalize table ----
  
  # Sort rows
  data.table::setorder(dt, country_code, surveyid_year, survey_acronym)
  
  # Order columns
  data.table::setcolorder(
    dt, c(
      "survey_id", "cache_id", "country_code", "surveyid_year", "survey_acronym",
      "survey_year", "welfare_type", "survey_mean_lcu", "survey_pop",
      "reporting_pop"
    )
  )
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(dt)
  
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.4 svy_mean_ppp_table --------
##
## Objective: Deflated survey mean (DSM) table (merge with CPI and PPP table)

db_create_dsm_table_sac <- function(lcu_table, cpi_table, ppp_table) {
  
  
  #--------- Merge with CPI ---------
  
  # Select CPI columns
  cpi_table <-
    cpi_table[, .SD,
              .SDcols =
                c(
                  "country_code", "survey_year", "survey_acronym",
                  "cpi_data_level", "cpi"
                )
    ]
  
  # Merge survey table with CPI (left join)
  dt <- joyn::joyn(lcu_table, cpi_table,
                   by = c(
                     "country_code", "survey_year",
                     "survey_acronym", "cpi_data_level"
                   ),
                   match_type = "m:1"
  )
  
  if (nrow(dt[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure CPI table is up to date"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }
  
  dt <- dt[
    .joyn != "y" 
  ][, .joyn := NULL]
  
  #--------- Merge with PPP ---------
  
  # Select default PPP values
  ppp_table <- ppp_table[ppp_default == TRUE]
  
  # Select PPP columns
  ppp_table <-
    ppp_table[, .SD,
              .SDcols =
                c("country_code", "ppp_data_level", "ppp")
    ]
  
  # Merge survey table with PPP (left join)
  jn <- joyn::joyn(dt, ppp_table,
                   by = c("country_code", "ppp_data_level"),
                   match_type = "m:1"
  )
  
  if (nrow(jn[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure PPP table is up to date"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }
  
  dt <- jn[
    .joyn != "y" # Countries in PPP table for which we don't have data
  ][, .joyn := NULL]
  
  #--------- Deflate welfare mean ---------
  
  dt <- dt|>
    fmutate(survey_mean_ppp = survey_mean_lcu / ppp / cpi)
  
  
  #--------- Add comparable spell --------- ## 
  
  dt[, comparable_spell := ifelse(.N == 1,
                                  as.character(reporting_year),
                                  sprintf("%s - %s",
                                          data.table::first(reporting_year),
                                          data.table::last(reporting_year))),
     by = c("country_code", "area", "survey_comparability")
  ] 
  
  #--------- Finalize table ---------
  
  # Add is_interpolated column
  dt$is_interpolated <- FALSE
  
  # Add is_used_for_line_up column
  
  dt_lu <- dt[area=="national" | reporting_level == area]
  
  dt_lu <- create_line_up_check(dt_lu)
  
  dt <- joyn::joyn(dt, dt_lu,
                   by = c("cache_id", "reporting_level", "area"),
                   match_type = "m:1",
                   y_vars_to_keep = "is_used_for_line_up"
  )
  
  dt <- dt[is.na(is_used_for_line_up),
           is_used_for_line_up := FALSE]
  
  dt <- dt[
    .joyn != "y" 
  ][, .joyn := NULL]
  
  # Add is_used_for_aggregation column
  dt[, n_rl := .N, by = cache_id]
  dt[, is_used_for_aggregation := ifelse((dt$reporting_level %in% 
                                            c("urban", "rural") & 
                                            dt$n_rl == 2), TRUE, FALSE)]
  dt$n_rl <- NULL
  
  # Select and order columns
  dt <- dt[, .SD,
           .SDcols =
             c(
               "survey_id", "cache_id", "wb_region_code", "pcn_region_code",
               "country_code", "survey_acronym", "survey_coverage",
               "survey_comparability", "comparable_spell",
               "surveyid_year", "reporting_year",
               "survey_year", "survey_time", "welfare_type",
               "survey_mean_lcu", "survey_mean_ppp", #' survey_pop',
               "reporting_pop", "ppp", "cpi", "pop_data_level",
               "gdp_data_level", "pce_data_level",
               "cpi_data_level", "ppp_data_level", "reporting_level",
               "area",
               "distribution_type", "gd_type",
               "is_interpolated", "is_used_for_line_up",
               "is_used_for_aggregation", "display_cp"
             )
  ]
  
  # Add aggregated mean for surveys split by Urban/Rural 
  
  if(any(dt$is_used_for_aggregation==TRUE)){
    
    # Select rows w/ non-national pop_data_level
    dt_sub <- dt[is_used_for_aggregation == TRUE]
    
    # Compute aggregated mean (weighted population average)
    dt_agg <- dt_sub[, ":=" (reporting_pop           = fsum(reporting_pop),
                             survey_mean_lcu = fmean(x = survey_mean_lcu,
                                                     w = reporting_pop),
                             survey_mean_ppp = fmean(x = survey_mean_ppp,
                                                     w = reporting_pop),
                             ppp                     = NA,  
                             cpi                     = NA,
                             area                    = "national",
                             pop_data_level          = "national", 
                             gdp_data_level          = "national",
                             pce_data_level          = "national",
                             cpi_data_level          = "national",
                             ppp_data_level          = "national",
                             reporting_level         = "national",
                             is_interpolated         = FALSE,
                             is_used_for_line_up     = FALSE,
                             is_used_for_aggregation = FALSE),
                     by = .(survey_id, cache_id) ]
    
    
    dt_agg <- dt_agg |>
      fgroup_by(survey_id, cache_id)|>
      fsummarise(across(names(dt)[!names(dt) %in% c("survey_id", "cache_id")], funique))|>
      fungroup()
    
    dt <- rbind(dt_agg, dt)
  }
  
  # Sort rows
  data.table::setorder(dt, survey_id, cache_id)
  
  # change factors to characters
  nn <- names(dt[, .SD, .SDcols = is.factor])
  dt[, (nn) := lapply(.SD, as.character),
     .SDcols = nn]
  
  return(dt)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Dist_stats   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.0 wrp_md_dist_stats (not used) -----
##
## Objective: Wrapper for wbpip function `wbpip::md_compute_dist_stats` and 
## transform results to data.table: 
## (Diana comment: If this works it could be implemented in wbpip)

# wrp_md_dist_stats <- function(welfare,
#                               weight) {
# 
#   #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # computations   ---------
#   #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   dl <- wbpip:::md_compute_dist_stats(
#     welfare = welfare,
#     weight = weight)
#   dt <- unlist2d(dl, DT= TRUE)
#   colnames(dt) <- c("name", paste0("decile", 1:10) )
#   dt <- data.table::transpose(dt, make.names = "name", keep.names = "id")
#   dt <- collapse::pivot(setnafill(dt, "locf", cols = 2:6),
#                         ids = 2:6, names = "id", how = "w", values = "quantiles", na.rm = TRUE)
# 
#   #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # Return   ---------
#   #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   return(dt)
# 
# }

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1 db_dist_stats_sac --------
##
## Objective: Calculate distributional statistics at the national and area level
## Note: This function is missing the warnings from the old pipeline

db_dist_stats_sac <- function(dt, mean_table){
  
  # 1. Fill area with national when empty and select variables ----

  dt <- ftransform(dt, area = ifelse(as.character(area) == "", # if empty
                                     "national", # it gets national
                                     as.character(area)))|> # else it keeps area
    fselect(cache_id, distribution_type, reporting_level, imputation_id,
            area, weight, welfare_ppp, welfare)
    
    # 2. Micro and Imputed Data: Level & Area Estimation  ----
  
    md_id_area <- dt |>
      fsubset(distribution_type %in% c("micro", "imputed")) |>
      fselect(-c(distribution_type,welfare))|>
      roworder(cache_id, imputation_id, reporting_level, area, welfare_ppp) |>
      fgroup_by(cache_id, imputation_id, reporting_level, area)|>
      fsummarise(res = list(wbpip:::md_compute_dist_stats(
        welfare = welfare_ppp,
        weight = weight)))|>
      _[, c(.SD, .(
        Statistic = names(unlist(res)),
        Value = unlist(res))),
        by = .(cache_id, imputation_id, reporting_level, area)]|>
      fselect(-res)|>
      pivot(ids = 1:5, how="w", values = "Value", names = "Statistic") |>
      fgroup_by(cache_id, reporting_level, area)|>
      collapg(custom= list(fmean= 6:20))|> # If we don't use windows we could use `parallel = TRUE`
      fungroup()|>
      frename(survey_median_ppp = median)|>
      fmutate(reporting_level = as.character(reporting_level))

    setrename(md_id_area, gsub("quantiles", "decile", names(md_id_area)))

      # # Version using wrapper function for wbpip::md_compute_dist_stats
      #
      # md_id_area <- dt |>
      #   fsubset(distribution_type %in% c("micro", "imputed")) |>
      #   fselect(-c(distribution_type, welfare))|>
      #   roworder(cache_id, imputation_id, reporting_level, area, welfare_ppp)|>
      #   _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp,
      #                                         weight = weight)),
      #             by = .(cache_id, imputation_id, reporting_level, area)]|>
      #   fgroup_by(cache_id, reporting_level, area)|>
      #   collapg(custom= list(fmean= 5:19))|>
      #   fungroup()|>
      #   frename(survey_median_ppp = median)|>
      #   fmutate(reporting_level = as.character(reporting_level))
    
    # 4. Micro and Imputed Data: National Estimation ----
      
    md_id_national <- dt |>
      fsubset(distribution_type %in% c("micro", "imputed")
              & reporting_level == 'national' & area != "national") |>
      fselect(-c(distribution_type,welfare))|>
      roworder(cache_id, imputation_id, welfare_ppp) |>
      fgroup_by(cache_id, imputation_id)|>
      fsummarise(res = list(wbpip:::md_compute_dist_stats(
        welfare = welfare_ppp,
        weight = weight)))|>
      _[, c(.SD, .(
        Statistic = names(unlist(res)),
        Value = unlist(res))),
        by = .(cache_id, imputation_id)] |>
      fselect(-res)|>
      pivot(ids = 1:3, how="w", values = "Value", names = "Statistic") |>
      fgroup_by(cache_id)|>
      collapg(custom= list(fmean= 4:18))|>
      fungroup()|>
      frename(survey_median_ppp = median) |>
      fmutate(reporting_level = as.character("national"),
              area = as.character("national"))

    setrename(md_id_national, gsub("quantiles", "decile", names(md_id_national)))
    
    # # Version using wrapper function for wbpip::md_compute_dist_stats
    #
    # md_id_national <- dt |>
    #     fsubset(distribution_type %in% c("micro", "imputed")
    #           & reporting_level == 'national' & area != "national") |>
    #     fselect(-c(distribution_type, welfare))|>
    #     roworder(cache_id, imputation_id, welfare_ppp)|>
    #   _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp, weight = weight)),
    #                                  by = .(cache_id, imputation_id)]|>
    #   fgroup_by(cache_id)|>
    #   collapg(custom= list(fmean= 3:17))|>
    #   fungroup()|>
    #   frename(survey_median_ppp = median)|>
    #   fmutate(reporting_level = as.character("national"),
    #             area = as.character("national"))
     
    if(any(dt$distribution_type %in% c("group", "aggregate"))){
      
      # 5. Group and Aggregate Data: Level and Area Estimation -----
      gd_ag_area <- dt |>
        fsubset(distribution_type %in% c("group", "aggregate")) |>
        fselect(-c(distribution_type, welfare_ppp))|>
        collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp),
                       on=c("cache_id", "reporting_level", "area"), 
                       validate = "m:1",
                       how = "left",
                       verbose = 0) |>
        roworder(cache_id, reporting_level, area, welfare) |>
        fgroup_by(cache_id, reporting_level, area)|>
        fsummarise(res = list(wbpip:::gd_compute_dist_stats(  
          welfare = welfare,
          population = weight,
          mean = funique(survey_mean_ppp))))|>
        _[, c(.SD, .( # using _ because we are using native pipe 
          Statistic = names(unlist(res)), 
          Value = unlist(res))),
          by = .(cache_id, reporting_level, area)] |>
        fselect(-res)|>
        pivot(ids = 1:3, how="w", values = "Value", names = "Statistic")|>
        frename(survey_median_ppp = median)|>
        fmutate(reporting_level = as.character(reporting_level))
      
      setrename(gd_ag_area, gsub("deciles", "decile", names(gd_ag_area)))
      
      # 6. Aggregate Data: National estimation (synth needed) ----
      ag_national <- dt |>
        fsubset(distribution_type %in% c("aggregate")) |>
        fselect(-c(distribution_type,welfare_ppp))|>
        collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp, 
                                             reporting_pop), 
                       # using reporting_pop as it is the same as the one in the pop_table
                       on=c("cache_id", "reporting_level", "area"), 
                       validate = "m:1",
                       how = "left",
                       verbose = 0) |>
        roworder(cache_id, reporting_level, area, welfare) |>
        fgroup_by(cache_id, reporting_level, area)|>
        fsummarise(welfare =  wbpip:::sd_create_synth_vector(
          welfare = welfare,
          population = weight,
          mean = funique(survey_mean_ppp),
          pop = funique(reporting_pop)
        )$welfare,
        weight = funique(reporting_pop)/100000) |> 
        roworder(cache_id, welfare) |>
        fgroup_by(cache_id) |>
        fsummarise(res = list(wbpip:::md_compute_dist_stats(  
          welfare = welfare,
          weight = weight)))|>
        _[, c(.SD, .( 
          Statistic = names(unlist(res)), 
          Value = unlist(res))),
          by = .(cache_id)] |>
        fselect(-res)|>
        pivot(ids = 1, how="w", values = "Value", names = "Statistic")|>
        fmutate(reporting_level = as.character("national"), 
                area = as.character("national")) |>
        frename(survey_median_ppp = median)
      
      setrename(ag_national, gsub("quantiles", "decile", names(ag_national)))
      
      # 7. Rbindlist and return ----
      final <- rbindlist(list(md_id_area, md_id_national, 
                              gd_ag_area, ag_national), use.names = TRUE)
      
      return(final)
      
    }
    
    # 7. Rbindlist and return ----
    final <- rbindlist(list(md_id_area, md_id_national), 
                       use.names = TRUE)
  
  return(final)
  
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2 db_create_dist_table_sac --------
##
## Objective: Clean dist_stats table by adding/removing variables 
## and create median in LCU
## Note: This function is missing the warnings from the old pipeline/targets code

db_create_dist_table_sac <- function(dt, dsm_table){
  
  dt_clean <- dt |>
    collapse::join(dsm_table|>
                     fselect("survey_id", "cache_id", "wb_region_code", "pcn_region_code",
                             "country_code", "surveyid_year", "survey_year",
                             "reporting_year", "survey_acronym", "welfare_type",
                             "cpi", "ppp", "pop_data_level", "reporting_level", "area"),
                   on=c("cache_id", "reporting_level", "area"), 
                   validate = "1:1",
                   how = "left",
                   verbose = 0)|>
    fmutate(survey_median_lcu = survey_median_ppp*ppp*cpi,
            survey_id = toupper(survey_id))|>
    fselect(-ppp, -cpi)|>
    colorder(survey_id, cache_id, wb_region_code, pcn_region_code, country_code,
             survey_acronym, surveyid_year, survey_year, reporting_year, welfare_type,
             reporting_level, area, survey_median_lcu, survey_median_ppp, decile1:decile10,
             mean, gini, mld, polarization, pop_data_level)
  
  return(dt_clean)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Prod_svy_estimation   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.1 db_create_svy_estimation_table_sac --------
##
## Objective: Combine survey_means and dist_stats in one 
## table and merge gdp and pce data
 
db_create_svy_estimation_table_sac <- function(dsm_table, dist_table, gdp_table, pce_table) {
  
  # TEMP FIX: TO BE REMOVED (Diana: Do we still need it?)
  dist_table$survey_id <- toupper(dist_table$survey_id)
  dsm_table$survey_id <- toupper(dsm_table$survey_id)
  
  # Remove cols 
  dist_table$reporting_year <- NULL
  gdp_table$gdp_domain <- NULL
  pce_table$pce_domain <- NULL
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Merge tables --------
  
  # Merge DSM table w/ dist stat table (full join)
  dt <- joyn::joyn(dsm_table, 
                   dist_table, 
                   match_type = "1:1",
                   by = c("cache_id","pop_data_level","reporting_level","area"),
                   reportvar = FALSE)
  
  # Merge with GDP
  dt <- data.table::merge.data.table(
    dt, gdp_table,
    all.x = TRUE,
    by.x = c("country_code", "reporting_year", "gdp_data_level"),
    by.y = c("country_code", "year", "gdp_data_level")
  )
  
  # Merge with PCE
  
  
  dt <- data.table::merge.data.table(
    dt, pce_table,
    all.x = TRUE,
    by.x = c("country_code", "reporting_year", "pce_data_level"),
    by.y = c("country_code", "year", "pce_data_level")
  )
  
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Warnings --------
  
  # Remove rows with missing survey_mean_ppp
  # This shouldn't be the case
  # A problem with PHL 2009
  if (anyNA(dt$survey_mean_ppp)) {
    rlang::warn(c(
      sprintf(
        "Removing %s rows with missing `survey_mean_ppp`: ",
        sum(is.na(dt$survey_mean_ppp))
      ),
      unique(dt[is.na(survey_mean_ppp)]$cache_id)
    ))
    dt <- dt[!is.na(survey_mean_ppp), ]
  }
  
  # Remove rows with missing ppp
  # CHN, IDN, why?
  if (anyNA(dt$ppp)) {
    rlang::warn(c(
      sprintf(
        "Removing %s rows with missing `ppp`:",
        sum(is.na(dt$ppp))
      ),
      unique(dt[is.na(ppp)]$cache_id)
    ))
    dt <- dt[!is.na(ppp), ]
  }
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Finalize table --------
  
  # Fix and add columns
  dt$estimation_type <- "survey"
  dt$predicted_mean_ppp <- numeric(0)
  dt <- data.table::setnames(dt, 
                             c("gdp", "pce", "pcn_region_code"),
                             c("reporting_gdp", "reporting_pce", "region_code")
  )
  
  # Order final columns
  cols <- c(
    "survey_id", "cache_id", "region_code", "wb_region_code",
    "country_code", "reporting_year", "surveyid_year",
    "survey_year", "survey_time", "survey_acronym", "survey_coverage",
    "survey_comparability", "comparable_spell", "welfare_type",
    "reporting_level", "area",
    "survey_mean_lcu", "survey_mean_ppp",
    "survey_median_ppp", "survey_median_lcu",
    "predicted_mean_ppp", "ppp", "cpi",
    "reporting_pop", "reporting_gdp",
    "reporting_pce", "pop_data_level",
    "gdp_data_level", "pce_data_level",
    "cpi_data_level", "ppp_data_level",
    "distribution_type", "gd_type",
    "is_interpolated",
    "is_used_for_line_up", "is_used_for_aggregation",
    "estimation_type",
    "display_cp"
  )
  dt <- dt[, .SD, .SDcols = cols]
  
  return(dt)
}






