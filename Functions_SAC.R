# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project:       SAC Functions for PIP pipeline
# Author:        Giorgia Cecchinato and Diana C. Garcia Rojas
# Dependencies:  The World Bank
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Creation Date:    May 2024
# References:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Install packages   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# remotes::install_github("PIP-Technical-Team/pipload@dev", dependencies = FALSE)
# remotes::install_github("PIP-Technical-Team/wbpip", dependencies = FALSE)

# pak::pak("PIP-Technical-Team/pipfun@ongoing", ask = FALSE)
# pak::pak("PIP-Technical-Team/pipload@ongoing", ask = FALSE)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Defaults   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

py                 <- 2017  # PPP year
branch             <- "DEV"
release            <- "20240326"  
identity           <- "PROD"
max_year_country   <- 2022
max_year_aggregate <- 2022

#base_dir <- fs::path("E:/01.personal/wb622077/pip_ingestion_pipeline")
base_dir <- fs::path("E:/01.personal/wb535623/PIP/pip_ingestion_pipeline")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 0. Load Packages and Data  ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

withr::with_dir(new = base_dir, 
                code = {
                  # source("./_packages.R")
                  
                  # Load R files
                  purrr::walk(fs::dir_ls(path = "./R", 
                                         regexp = "\\.R$"), source)
                  
                  # Read pipdm functions
                  purrr::walk(fs::dir_ls(path = "./R/pipdm/R", 
                                         regexp = "\\.R$"), source)
                })

base_dir |>
  fs::path("_common.R") |>
  source(echo = FALSE)

## Change gls outdir:

#gls$CACHE_SVY_DIR_PC <- fs::path("E:/01.personal/wb622077/cache")
gls$CACHE_SVY_DIR_PC <- fs::path("E:/01.personal/wb535623/PIP/Cache")

## Cache data for Nigeria --------

cache_inventory <- pipload::pip_load_cache_inventory(version = '20240326_2017_01_02_PROD')
cache_inventory <- cache_inventory[cache_inventory$cache_id %like% "NGA",]
 
cache <- pipload::pip_load_cache("NGA", version = '20240326_2017_01_02_PROD') 
cache_ids <- get_cache_id(cache_inventory) 

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
  gd_means        <- dt.[,.(cache_id, survey_mean_lcu)]
  gd_means        <- gd_means[,survey_mean_lcu:= survey_mean_lcu*(12/365)]
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(gd_means)
}

gd_means_sac <- get_groupdata_means_sac(cache_inventory = cache_inventory, gdm = dl_aux$gdm)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## svy_mean_lcu --------
##
## Objective:  Local Currency Unit survey mean list

db_compute_survey_mean_sac <- function(cache, gd_mean = NULL) {
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # computations   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # Select variables
  dt <- cache[, .(welfare, weight, survey_id, cache_id, country_code, 
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
    fsubset(area != "national")|>
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

svy_mean_lcu_sac <- db_compute_survey_mean_sac(cache = cache, gd_mean = gd_means_sac)

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
  
  # Create nested POP table
  pop_table$pop_domain <- NULL
  pop_nested <- pop_table %>%
    tidyfast::dt_nest(country_code, pop_data_level, .key = "data")
  
  # Merge dt with pop_nested (add survey_pop)
  dt <- joyn::joyn(dt, pop_nested,
                   by = c("country_code", "pop_data_level"),
                   match_type = "m:1"
  )
  #NOTE DC: Should we use area to include rural/urban population?
  
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
    .joyn != "y"
  ][, .joyn := NULL]
  
  # Transform survey_year to numeric
  dt[
    ,
    survey_year := as.numeric(survey_year)
  ]
  
  # Adjust population values for surveys spanning two calender years
  dt$survey_pop <-
    purrr::map2_dbl(dt$survey_year, dt$data,
                    adjust_aux_values,
                    value_var = "pop"
    )
  # Note DC: check how to optimize so we don't need to use lists.
  
  # Remove nested data column
  dt$data <- NULL
  
  # Merge with pop_table (add reporting_pop)
  dt <- joyn::joyn(dt, pop_table,
                   by = c(
                     "country_code",
                     "reporting_year = year",
                     "pop_data_level"
                   ),
                   match_type = "m:1"
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
    .joyn != "y" # All country/years for which we don't have data... its ox.
  ][, .joyn := NULL]
  
  
  data.table::setnames(dt, "pop", "reporting_pop")
  
  
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

svy_mean_lcu_table_sac <- db_create_lcu_table_sac(dt = svy_mean_lcu_sac,
                                                  pop_table = dl_aux$pop,
                                                  pfw_table = dl_aux$pfw) 

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
  
  
  cdt <- dt[, unique(country_code)]
  cppp <- jn[.joyn == "y", unique(country_code)]
  
  dt <- jn[
    .joyn != "y" # Countries in PPP table for which we don't have data
  ][, .joyn := NULL]
  
  #--------- Deflate welfare mean ---------
  
  # svy_mean_ppp = survey_mean_lcu / cpi / ppp
  # dt$survey_mean_ppp <-
  #   wbpip::deflate_welfare_mean(
  #     welfare_mean = dt$survey_mean_lcu, ppp = dt$ppp, cpi = dt$cpi
  #   )
  # Note: Do we need the function? Faster without it?
  # Example: 
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
  dt_lu <- dt[area=="national"][, n_rl := .N, by = cache_id]
  
  dt_lu <- create_line_up_check(dt_lu)
  
  dt <- joyn::joyn(dt, dt_lu,
                   by = c("cache_id", "reporting_level", "area"),
                   match_type = "m:1",
                   y_vars_to_keep = "is_used_for_line_up"
  )
  
  dt[is.na(is_used_for_line_up),is_used_for_line_up := FALSE]
  
  dt <- dt[
    .joyn != "y" 
  ][, .joyn := NULL]
  
  # Add is_used_for_aggregation column
  dt[, n_rl := .N, by = cache_id]
  dt[, is_used_for_aggregation := ifelse((dt$reporting_level %in% c("urban", "rural") & dt$n_rl == 2), TRUE, FALSE)]
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
                             pop_data_level          = "national", 
                             gdp_data_level          = "national",
                             pce_data_level          = "national",
                             cpi_data_level          = "national",
                             ppp_data_level          = "national",
                             reporting_level         = "national"),
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

svy_mean_ppp_table_sac <- db_create_dsm_table_sac(lcu_table = svy_mean_lcu_table_sac,
                                                  cpi_table = dl_aux$cpi,
                                                  ppp_table = dl_aux$ppp)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Dist_stats   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Objective: Calculate distributional statistics at the national and area level

mp_dl_dist_stats_sac <- function(dt, 
                                 mean_table){
  
  # 1. Fill area with national when empty ----
  dt <- ftransform(dt, area = ifelse(as.character(area) == "", # if empty
                                           "national", # it gets national
                                            as.character(area))) # else it keeps area
  
  # 3. Micro and Imputed Data: Level & Area Estimation  ----
  md_id_area <- dt |>
    fselect(cache_id, distribution_type, reporting_level, imputation_id, 
            area, weight, welfare_ppp) |>
    fsubset(distribution_type %in% c("micro", "imputed")) |>
    collapse::join(mean_table |> 
                     fselect(cache_id, reporting_level, area, survey_mean_ppp),
                   on=c("cache_id", "reporting_level", "area"), 
                   validate = "m:1",
                   how = "left",
                   verbose = 0) |>
    roworder(cache_id, imputation_id, reporting_level, area, welfare_ppp) |>
    fgroup_by(cache_id, imputation_id, reporting_level, area)|> 
    fsummarise(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = funique(survey_mean_ppp))),
      weight = fsum(weight))|>
    _[, c(.SD, .( 
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, imputation_id, reporting_level, area, weight)] |>
    fselect(-res)|>
    pivot(ids = 1:5, how="w", values = "Value", names = "Statistic") |>
    fgroup_by(cache_id, reporting_level, area)|>
    fsummarise(across(weight:quantiles10, fmean))|> 
    fungroup()|>
    frename(survey_median_ppp = median)|>
    fmutate(reporting_level = as.character(reporting_level))
  
  setrename(md_id_area, gsub("quantiles", "decile", names(md_id_area)))
 
  # 4. Micro and Imputed Data: National Estimation ----
  md_id_national <- dt |>
    fselect(cache_id, distribution_type, reporting_level, imputation_id, 
            area, weight, welfare_ppp) |>
    fsubset(distribution_type %in% c("micro", "imputed") 
            & reporting_level == 'national' & area != "national") |>
    collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp) |>
                     fsubset(area == 'national'),
                   on=c("cache_id", "reporting_level"), 
                   validate = "m:1",
                   how = "left",
                   verbose = 0) |>
    roworder(cache_id, imputation_id, welfare_ppp) |>
    fgroup_by(cache_id, imputation_id)|> 
    fsummarise(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = funique(survey_mean_ppp))))|>
    _[, c(.SD, .(  
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, imputation_id)] |>
    fselect(-res)|>
    pivot(ids = 1:3, how="w", values = "Value", names = "Statistic") |>
    fgroup_by(cache_id)|>
    fsummarise(across(mean:quantiles10, fmean))|> 
    fungroup()|>
    frename(survey_median_ppp = median) |>
    fmutate(reporting_level = as.character("national"), 
            area = as.character("national"))
  
  setrename(md_id_national, gsub("quantiles", "decile", names(md_id_national)))
  
  # 5. Group and Aggregate Data: Level and Area Estimation -----
  gd_ag_area <- dt |>
    fselect(cache_id, distribution_type, reporting_level, imputation_id, 
            area, welfare, weight) |>
    fsubset(distribution_type %in% c("group", "aggregate")) |>
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
    fselect(cache_id, distribution_type, reporting_level, area, welfare, welfare_ppp, weight) |>
    fsubset(distribution_type %in% c("aggregate")) |>
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
    mutate(reporting_level = as.character("national"), 
           area = as.character("national")) |>
    frename(survey_median_ppp = median)
  
  setrename(ag_national, gsub("quantiles", "decile", names(ag_national)))
  

  # 7. Rbindlist and return ----
  final <- rbindlist(list(md_id_area |> fselect(-weight), md_id_national, 
                          gd_ag_area, ag_national), use.names = TRUE)
  
  return(final)
  
}

## How to Run it:
#dl_dist_stats_sac <- mp_dl_dist_stats_sac(dt = cache_tb, 
#                                          mean_table = mean_table)


db_create_dist_table_sac <- function(dt,
                                     dsm_table){
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
    fmutate(survey_median_lcu = survey_median_ppp*ppp*cpi)|>
    fselect(-ppp, -cpi)|>
    colorder(survey_id, cache_id, wb_region_code, pcn_region_code, country_code,
             survey_acronym, surveyid_year, survey_year, reporting_year, welfare_type,
             reporting_level, area, survey_median_lcu, survey_median_ppp, decile1:decile10,
             mean, gini, mld, polarization, pop_data_level)
  
  return(dt_clean)
}

## How to Run it:
#dt_dist_stats_sac <- db_create_dist_table_sac(dt = dl_dist_stats_sac,
#                                              dsm_table = svy_mean_ppp_table_sac)







