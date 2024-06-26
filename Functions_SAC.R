
# ---------------------------- FUNCTIONS SAC -------------------------

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 0. Cache   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

get_cache <- function(cache) {
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # computations   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # Select variables and modify the area variable for imputed or group
  dt <- cache |>
    fselect(welfare, welfare_ppp, weight, survey_id, cache_id, country_code, 
            surveyid_year, survey_acronym, survey_year, welfare_type,
            distribution_type, gd_type, imputation_id, cpi_data_level, 
            ppp_data_level, gdp_data_level, pce_data_level, 
            pop_data_level, reporting_level, 
            #area,
            cpi, ppp)
  # |>
  #   ftransform(area = as.character(area))
  
  # setv(dt$area,"", "national") 
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(dt)
  
}

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
  
  gd_means <- joyn::joyn(x          = cache_inventory,
                         y          = gdm,
                         by         = c("survey_id", "welfare_type"),
                         match_type = "1:m",
                         y_vars_to_keep = c("survey_mean_lcu", "pop_data_level"),
                         keep       = "left")
  
  
  gd_means <- gd_means |>
    setorderv(c("cache_id", "pop_data_level"))|>
    fselect(cache_id, pop_data_level, survey_mean_lcu)|>
    fmutate(survey_mean_lcu = survey_mean_lcu*(12/365))
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(gd_means)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2 svy_mean_lcu --------
##
## Objective:  Local Currency Unit survey mean list

db_compute_survey_mean_sac <- function(cache, 
                                       gd_mean) {
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # computations   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # ------ Prepare data -----
  
  # Select variables for metadata
  metadata_vars <- c("cache_id", "reporting_level", 
                     #"area",
                     "survey_id", "country_code", "surveyid_year", 
                     "survey_acronym","survey_year", "welfare_type", 
                     "distribution_type","gd_type","cpi_data_level",
                     "ppp_data_level", "gdp_data_level", 
                     "pce_data_level", "pop_data_level",
                     "cpi", "ppp")
  
  # ------ Micro data urban/rural -----
  
  # dt <- cache_sac |>
  #   fsubset(distribution_type %in% c("imputed", "micro"))
  # 
  # dt_m <- dt |>
  #   fgroup_by(cache_id, reporting_level, area, imputation_id)|> 
  #   collapg(custom = list(fmean = c(survey_mean_lcu = "welfare")), w = weight)|>
  #   fgroup_by(cache_id, reporting_level, area)|>
  #   collapg(custom = list(fmean = c(survey_mean_lcu = "survey_mean_lcu")), w = weight)|>
  #   fungroup()
  # 
  # dt_meta_vars <- dt |>
  #   get_vars(metadata_vars) |> 
  #   funique(cols = c("cache_id", "reporting_level", "area")) 
  # 
  # add_vars(dt_m) <- dt_meta_vars|>
  #   fselect(-c(cache_id, reporting_level, area))
  
  # ----- Micro data national -----
  
  # tst7 <- cache_sac[, .(ind = length(unique(welfare))), by = c("cache_id","reporting_level", "area")]
  # 
  # dt <- dt_m |>
  #   _[, ind := length(unique(area)), by = c("cache_id","reporting_level")]|>
  #   fsubset(area != "national" & reporting_level == "national")
  # 
  # dt_nat <- dt |>
  #   fgroup_by(cache_id, reporting_level)|>
  #   collapg(custom = list(fmean = c(survey_mean_lcu = "survey_mean_lcu")), w = weight)|>
  #   fungroup()|>
  #   fmutate(area = "national")
  # 
  # dt_meta_vars <- dt |> 
  #   get_vars(metadata_vars) |> 
  #   funique(cols = c("cache_id", "reporting_level")) 
  # 
  # add_vars(dt_nat) <- dt_meta_vars|>
  #   fselect(-c(cache_id, reporting_level,area))
  
  dt <- cache |>
    fsubset(distribution_type %in% c("imputed", "micro"))
  
  dt_c <- dt |>
    fgroup_by(cache_id, cpi_data_level, ppp_data_level,
              gdp_data_level, pce_data_level,
              pop_data_level, reporting_level, 
              imputation_id)|> 
    collapg(custom = list(fmean = c(survey_mean_lcu = "welfare")), w = weight)|>
    fgroup_by(cache_id, cpi_data_level, ppp_data_level,
              gdp_data_level, pce_data_level,
              pop_data_level, reporting_level)|>
    collapg(custom = list(fmean = c(survey_mean_lcu = "survey_mean_lcu")), w = weight)|>
    fungroup()
  
  dt_meta_vars <- dt |>
    get_vars(metadata_vars) |> 
    funique(cols = c("cache_id", "cpi_data_level", "ppp_data_level",
                     "gdp_data_level", "pce_data_level",
                     "pop_data_level", "reporting_level")) 
  
  add_vars(dt_c) <- dt_meta_vars|>
    fselect(-c(cache_id, cpi_data_level, ppp_data_level,
               gdp_data_level, pce_data_level,
               pop_data_level, reporting_level))
  
  # All micro and imputed data
  
  #dt_c <- collapse::rowbind(dt_m, dt_nat)
  
  #  ------ Group data -----
  
  if(any(cache$distribution_type %in% c("group", "aggregate"))){
    
    dt_g <- cache |>
      fsubset(distribution_type %in% c("group", "aggregate"))|>
      fselect(-c(welfare, imputation_id)) |> 
      fgroup_by(metadata_vars)|>
      collapg(custom = list(fsum = "weight"))|>
      joyn::joyn(gd_mean[!is.na(survey_mean_lcu)],
                 by = c(
                   "cache_id", "pop_data_level"
                 ),
                 y_vars_to_keep = "survey_mean_lcu",
                 match_type = "1:1", keep = "left", 
                 reportvar = FALSE, sort = FALSE)
    
    dt_c <- collapse::rowbind(dt_c, dt_g) 
    
  }
  
  # ----- Finalize table -----
  
  sort_vars <- c("survey_id",
                 "country_code",
                 "surveyid_year", 
                 "survey_acronym",
                 "survey_year", 
                 "welfare_type")
                 #"area")
  
  setorderv(dt_c, sort_vars) # Order rows
  
  setcolorder(dt_c, sort_vars) # Order columns
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  dt_c
  
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
  
  # Select columns and merge LCU table with PFW (left join)
  dt <- joyn::joyn(dt, pfw_table|>
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
  
  dt <- dt|>
    fsubset(.joyn != "y")|>
    fselect(-.joyn)
  
  #--------- Merge with POP ---------
  
  pop_table$pop_domain <- NULL 
  
  # --- Reporting_pop ----
  
  dt <- joyn::joyn(dt, pop_table,
                   by = c("country_code",
                          "reporting_year = year",
                          #"area = pop_data_level"
                          "pop_data_level"
                   ),
                   match_type = "m:1",
                   keep = "left"
  )
  
  #if (nrow(dt[(.joyn == "x" & reporting_level==area)]) > 0) { #There is an error for the area level (see if it affects later on)
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
  
  dt <- dt|>
    fsubset(.joyn != "y")|>
    fselect(-.joyn)|>
    setnames("pop", "reporting_pop")
  
  # ---- Survey_pop ----
  
  dt_svy_pop <- dt|>
    fsubset(survey_year != floor(survey_year)) |>
    rowbind(dt|> fsubset(survey_year != floor(survey_year)), idcol = "id")|>
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
  
  dt <- joyn::joyn(dt, dt_svy_pop,
                   by = c("survey_id", 
                          "country_code", 
                          "survey_year",
                          "reporting_level"
                          #"area"
                   ),
                   match_type = "m:1",
                   keep = "left"
  )
  
  dt <- dt|>
    fsubset(.joyn != "y")|>
    fselect(-.joyn)
  
  # ---- Finalize table ----
  
  dt <- dt |>
    ftransform(survey_pop = fifelse(is.na(survey_pop),
                                    reporting_pop, survey_pop))
  
  setorderv(dt, c("country_code", "surveyid_year", "survey_acronym"))
  
  setcolorder(dt, c("survey_id", "cache_id" , "country_code", 
                    "surveyid_year", "survey_acronym", "survey_year", 
                    "welfare_type", "survey_mean_lcu", "survey_pop",
                    "reporting_pop"))
  
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(dt)
  
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.4 svy_mean_ppp_table --------
##
## Objective: Deflated survey mean (DSM) table, add other 
## logical variables, and calculate aggregated means

db_create_dsm_table_sac <- function(lcu_table) {
  
  #--------- Deflate welfare mean ---------
  
  dt <- fmutate(lcu_table, survey_mean_ppp = survey_mean_lcu / ppp / cpi)
  
  #--------- Add comparable spell --------- ## 
  
  dt[, comparable_spell := ifelse(.N == 1,
                                  as.character(reporting_year),
                                  sprintf("%s - %s",
                                          data.table::first(reporting_year),
                                          data.table::last(reporting_year))),
     #by = c("country_code", "area", "survey_comparability")
     by = c("country_code", "survey_comparability")
  ] 
  
  #--------- Finalize table ---------
  
  # Add is_interpolated column
  dt$is_interpolated <- FALSE
  
  # Add is_used_for_line_up column
  
  # dt <- dt |>
  #   joyn::joyn(create_line_up_check(dt[area=="national" | reporting_level == area]),
  #                  by = c("cache_id", "reporting_level", "area"),
  #                  match_type = "m:1",
  #                  y_vars_to_keep = "is_used_for_line_up")
  
  dt <- create_line_up_check(dt)
  
  #dt[is.na(dt$is_used_for_line_up), "is_used_for_line_up"] <- FALSE
  
  # setv(dt$is_used_for_line_up,is.na(dt$is_used_for_line_up), FALSE)
  # 
  # dt <- dt|>
  #   fsubset(.joyn != "y")|>
  #   fselect(-.joyn)
  
  # Add is_used_for_aggregation column
  dt[, n_rl := .N, by = cache_id]
  
  dt$is_used_for_aggregation <- (dt$reporting_level %in% 
                                   c("urban", "rural") & 
                                   dt$n_rl == 2)
  
  dt$n_rl <- NULL
  
  # Select and order columns
  
  data_vars <- c("survey_id", "cache_id", "wb_region_code",
                 "pcn_region_code", "country_code", "survey_acronym",
                 "survey_coverage", "survey_comparability", "comparable_spell",
                 "surveyid_year", "reporting_year", "survey_year", 
                 "survey_time", "welfare_type", "survey_mean_lcu",
                 "survey_mean_ppp", "reporting_pop", "ppp",
                 "cpi", "pop_data_level", "gdp_data_level",
                 "pce_data_level", "cpi_data_level", "ppp_data_level", 
                 #"reporting_level", "area", "distribution_type",
                 "reporting_level", "distribution_type",
                 "gd_type", "is_interpolated", "is_used_for_line_up",
                 "is_used_for_aggregation", "display_cp")
  
  dt <- dt |>
    fselect(data_vars)
  
  # Add aggregated mean for surveys split by Urban/Rural 
  
  if(any(dt$is_used_for_aggregation==TRUE)){
    
    # Select rows w/ non-national pop_data_level
    
    dt_sub <- dt |>
      fsubset(is_used_for_aggregation == TRUE)
    
    # Compute aggregated mean (weighted population average)
    
    dt_agg <- dt_sub |>
      fgroup_by(survey_id, cache_id) |>
      collapg(custom = list(fmean = "survey_mean_lcu",
                            fmean = "survey_mean_ppp"),
              w = reporting_pop)|>
      fmutate(ppp                     = NA,  
              cpi                     = NA,
              #area                    = "national",
              pop_data_level          = "national", 
              gdp_data_level          = "national",
              pce_data_level          = "national",
              cpi_data_level          = "national",
              ppp_data_level          = "national",
              reporting_level         = "national",
              is_interpolated         = FALSE,
              is_used_for_line_up     = FALSE,
              is_used_for_aggregation = FALSE)|>
      fungroup()
    
    dt_meta_vars <- dt_sub |>
      get_vars(c(names(dt_sub)[!names(dt_sub) %in% names(dt_agg)],"survey_id", "cache_id"))|>
      funique(cols = c("survey_id", "cache_id"))
    
    add_vars(dt_agg) <- dt_meta_vars|>
      fselect(-c(survey_id, cache_id))
    
    dt <- collapse::rowbind(dt_agg, dt)
  }
  
  # Sort rows
  setorderv(dt, c("survey_id", "cache_id"))
  
  # change factors to characters
  dt <- dt |>
    fcomputev(is.factor, as.character, keep = names(dt))
  
  return(dt)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Dist_stats   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

source("wrp_wbpip.R")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1 db_dist_stats_sac --------
##
## Objective: Calculate distributional statistics at the national and area level
## Note: This function is missing the warnings from the old pipeline

db_dist_stats_sac <- function(cache, 
                              mean_table){
  
  # 1. Select variables and subset for Micro data----
  
  dt_m <- cache |>
    fselect(cache_id, distribution_type, cpi_data_level, ppp_data_level,
            gdp_data_level, pce_data_level,
            pop_data_level, reporting_level, 
            imputation_id, weight, welfare_ppp) |>
    fsubset(distribution_type %in% c("micro", "imputed"))
  
  # 2. Micro Data: Level Estimation  ----
  
  md_id_level <- dt_m |>
    roworder(cache_id, pop_data_level, welfare_ppp) |>
    _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp,
                                  weight  = weight,
                                  mean = NULL)),
      by = .(cache_id, imputation_id, cpi_data_level, ppp_data_level,
             gdp_data_level, pce_data_level,
             pop_data_level, reporting_level)]|>
    fgroup_by(cache_id, cpi_data_level, ppp_data_level,
              gdp_data_level, pce_data_level,
              pop_data_level, reporting_level)|>
    collapg(fmean, cols = c("mean","median","gini",
                            "polarization","mld",
                            paste0("decile",1:10)))|>
    fungroup()|>
    frename(survey_median_ppp = median)|>
    fmutate(reporting_level = as.character(reporting_level),
            pop_data_level = as.character(pop_data_level))|>
    fselect(-c(cpi_data_level, ppp_data_level,
               gdp_data_level, pce_data_level))
  
  
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
            pop_data_level = as.character("national")) # < 1 minute
  
  if(any(cache$distribution_type %in% c("group", "aggregate"))){
    
    # Select variables, subset and join mean table
    dt_jn <- cache |>
      fselect(cache_id, distribution_type, imputation_id, cpi_data_level, ppp_data_level,
              gdp_data_level, pce_data_level,
              pop_data_level, reporting_level, weight, welfare) |>
      fsubset(distribution_type %in% c("group", "aggregate"))|>
      collapse::join(mean_table |> 
                       fselect(cache_id, cpi_data_level, ppp_data_level,
                               gdp_data_level, pce_data_level,
                               pop_data_level, reporting_level, 
                               survey_mean_ppp, reporting_pop),
                     on=c("cache_id", "cpi_data_level", "ppp_data_level",
                          "gdp_data_level", "pce_data_level",
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
      roworder(cache_id, cpi_data_level, ppp_data_level,
               gdp_data_level, pce_data_level,
               pop_data_level, reporting_level, welfare) |>
      _[, as.list(safe_wrp_gd_dist_stats(welfare = welfare,
                                         population = weight,
                                         mean = funique(survey_mean_ppp))),
        by = .(cache_id, cpi_data_level, ppp_data_level,
               gdp_data_level, pce_data_level,
               pop_data_level, reporting_level)]|>
      frename(survey_median_ppp = median)|>
      fmutate(reporting_level = as.character(reporting_level),
              pop_data_level = as.character(pop_data_level))|>
      fselect(-c(cpi_data_level, ppp_data_level,
                 gdp_data_level, pce_data_level)) # immediate
    
    
    setrename(gd_ag_level, gsub("deciles", "decile", names(gd_ag_level)))
    
    # 4. Aggregate Data: National estimation (synth needed) ----
    
    ag_syn <- dt_jn |>
      fsubset(distribution_type %in% c("aggregate")) |>
      roworder(cache_id, cpi_data_level, ppp_data_level,
               gdp_data_level, pce_data_level,
               pop_data_level, reporting_level, welfare) |>
      fgroup_by(cache_id, cpi_data_level, ppp_data_level,
                gdp_data_level, pce_data_level,
                pop_data_level, reporting_level)|>
      fsummarise(welfare =  wbpip:::sd_create_synth_vector(
        welfare = welfare,
        population = weight,
        mean = funique(survey_mean_ppp),
        pop = funique(reporting_pop))$welfare,
        weight = funique(reporting_pop)/100000) # immediate
    
    # Aggregate to national
    
    ag_national <- ag_syn |> 
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
              pop_data_level = as.character("national")) # immediate
    
    # 5. Row bind and return ----
    
    final <- rowbind(md_id_level, md_id_national, gd_ag_level, ag_national)
    
    return(final)
    
  }
  
  final <- rowbind(md_id_level, md_id_national)
  
  return(final)
  
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2 db_create_dist_table_sac --------
##
## Objective: Clean dist_stats table by adding/removing variables 
## and create median in LCU
## Note: This function is missing the warnings from the old pipeline/targets code

db_create_dist_table_sac <- function(dt, dsm_table, cache_inventory){
  
  dt_clean <- dt |>
    collapse::join(dsm_table|>
                     fselect("survey_id", "cache_id", "wb_region_code", "pcn_region_code",
                             "country_code", "surveyid_year", "survey_year",
                             "reporting_year", "survey_acronym", "welfare_type",
                             "cpi", "ppp", "pop_data_level", "reporting_level"),
                   on=c("cache_id", "reporting_level", "pop_data_level"), 
                   validate = "1:1",
                   how = "left",
                   verbose = 0,
                   overid = 2)
  
  dt_clean[cache_inventory, 
           # I think this is a patch but it does not check if We
           # are using the last version of dataliweb.
     on = "cache_id",
     survey_id := i.survey_id
  ]
  
  dt_clean <- dt_clean |>
    fmutate(survey_median_lcu = survey_median_ppp*ppp*cpi,
            survey_id = toupper(survey_id))|>
    fselect(-ppp, -cpi)|>
    colorder(survey_id, cache_id, wb_region_code, pcn_region_code, country_code,
             survey_acronym, surveyid_year, survey_year, reporting_year, welfare_type,
             reporting_level, survey_median_lcu, survey_median_ppp, decile1:decile10,
             mean, gini, polarization, mld, pop_data_level)  
  
  # change factors to characters
  dt_clean <- dt_clean |>
    fcomputev(is.factor, as.character, keep = names(dt_clean))
  
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
                   #by = c("cache_id","pop_data_level","reporting_level","area"),
                   by = c("cache_id","pop_data_level","reporting_level"),
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
        fsum(is.na(dt$survey_mean_ppp))
      ),
      funique(dt[is.na(survey_mean_ppp)]$cache_id)
    ))
    dt <- dt[!is.na(survey_mean_ppp), ]
  }
  
  # Remove rows with missing ppp
  # CHN, IDN, why?
  if (anyNA(dt$ppp)) {
    rlang::warn(c(
      sprintf(
        "Removing %s rows with missing `ppp`:",
        fsum(is.na(dt$ppp))
      ),
      funique(dt[is.na(ppp)]$cache_id)
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
    #"reporting_level", "area",
    "reporting_level",
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
  
  dt <- fselect(dt, cols)
  
  return(dt)
}






