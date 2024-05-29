
# ---------------------------- FUNCTIONS SAC -------------------------

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
#  1. Survey_means  ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## gd_means --------
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
## svy_mean_lcu --------
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
## svy_mean_lcu_table --------
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
## svy_mean_ppp_table --------
##
## Objective: Deflated survey mean (DSM) table

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





