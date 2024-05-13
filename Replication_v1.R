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
release            <- "20240326"  # I have to changed it because I messed up the Y folder :(
identity           <- "PROD"
max_year_country   <- 2022
max_year_aggregate <- 2022

#base_dir <- fs::path("E:/01.personal/wb622077/pip_ingestion_pipeline")
base_dir <- fs::path("E:/01.personal/wb535623/pip_ingestion_pipeline")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Load Packages and Data  ---------
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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Run common R code   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

base_dir |>
  fs::path("_common.R") |>
  source(echo = FALSE)

## Change gls outdir:

#gls$CACHE_SVY_DIR_PC <- fs::path("E:/01.personal/wb622077/cache")
gls$CACHE_SVY_DIR_PC <- fs::path("E:/01.personal/wb535623/PIP/Cache")

# base_dir |>
#   fs::path("_cache_loading_saving.R") |>
#   source(echo = FALSE)

# filter for testing --------
cache_inventory <- pipload::pip_load_cache_inventory(version = '20240326_2017_01_02_PROD')
cache_inventory <- cache_inventory[cache_inventory$cache_id %like% "NGA",]
cache <- pipload::pip_load_cache("NGA", type="list", version = '20240326_2017_01_02_PROD') 
cache_tb <- pipload::pip_load_cache("NGA", version = '20240326_2017_01_02_PROD') 
  
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Check targets nested pipeline   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#tar_visnetwork()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
#  1. Survey_means  ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# It depends of:
# svy_mean_ppp_table <- svy_mean_lcu_table <- svy_mean_lcu <- gd_means

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Gd_means --------

# Objective: Fetch Group Data survey means and convert them to daily values
#
# tar_target(
#   gd_means, 
#   get_groupdata_means(cache_inventory = cache_inventory, 
#                       gdm            = dl_aux$gdm), 
#   iteration = "list"
# )

gd_means_tar <- get_groupdata_means(cache_inventory = cache_inventory, gdm = dl_aux$gdm)

## New function:

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

# Objective:  Local Currency Unit survey mean list 
# 
# tar_target(
#   svy_mean_lcu,
#   mp_svy_mean_lcu(cache, gd_means)
# ),
#
# Functions used to calculate this:
# mp_svy_mean_lcu <- db_compute_survey_mean <- compute_survey_mean
# compute_survey_mean uses: md_compute_survey_mean, gd_compute_survey_mean,
#                           id_compute_survey_mean

svy_mean_lcu_tar <- mp_svy_mean_lcu(cache, gd_means_tar) 

# New function (version with list):

db_compute_survey_mean_sac <- function(dt, gd_mean = NULL) {
  tryCatch(
    expr = {
      
      # Get distribution type
      dist_type <- as.character(unique(dt$distribution_type))
      
      # Get group mean
      #if (dist_type=="group"|dist_type=="imputed"){ # For efficiency (?)
      gd_mean  <- as.character(gd_mean[cache_id==dt$cache_id[1],survey_mean_lcu])
      #}
      
      # Calculate weighted welfare mean
      dt <- compute_survey_mean[[dist_type]](dt, gd_mean)
      
      # Order columns
      data.table::setcolorder(
        dt, c(
          "survey_id", "country_code", "surveyid_year", "survey_acronym",
          "survey_year", "welfare_type", "survey_mean_lcu"
        )
      )
      
      return(dt)
    }, # end of expr section
    
    error = function(e) {
      cli::cli_alert_danger("Survey mean caluclation failed. Returning NULL.")
      
      return(NULL)
    } # end of error
  ) # End of trycatch
}

svy_mean_lcu_sac <- cache |>
  purrr::map(\(x) db_compute_survey_mean_sac(dt = x, gd_mean = gd_means_sac))

# Replicate outcome on table format:
svy_mean_lcu_tb <- data.table::rbindlist(svy_mean_lcu_tar, use.names = TRUE)

# New function (version with cache as table):
# Note: two versions with data.table and pipe.

db_compute_survey_mean_sac_dt <- function(cache_tb, gd_mean = NULL) {

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # computations   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Select variables
  dt <- cache_tb[, .(welfare, weight, survey_id, cache_id, country_code, 
                     surveyid_year, survey_acronym, survey_year, welfare_type,
                     distribution_type, gd_type, imputation_id, cpi_data_level, 
                     ppp_data_level, gdp_data_level, pce_data_level, 
                     pop_data_level, reporting_level, area)]
  
  # Previous step for imputed distribution type
  
  dt[, survey_mean_imp := fmean(welfare, w = weight, na.rm = TRUE),
     by = .(cache_id, reporting_level, area, imputation_id)]
  
  # Mean calculations 
  
  dt[, ':=' (survey_mean_lcu_a = ifelse(distribution_type == "micro",
                                        fmean(welfare, w = weight, na.rm = TRUE),
                                        ifelse((distribution_type == "group" |
                                                  distribution_type == "aggregate"),
                                               as.numeric(gd_mean[cache_id==cache_id[1],
                                                                  survey_mean_lcu]),
                                               ifelse(distribution_type == "imputed",
                                                      fmean(survey_mean_imp, na.rm = TRUE),
                                                      survey_mean_lcu))),
             weight_a = fsum(weight)),
     by = .(cache_id,reporting_level, area)]
  
  # Group variables
  
  keep_vars <- c("survey_id", "country_code", "surveyid_year", 
                 "survey_acronym","survey_year", "welfare_type", 
                 "distribution_type","gd_type","cpi_data_level",
                 "ppp_data_level", "gdp_data_level", 
                 "pce_data_level", "pop_data_level")
  
  dt_c <- dt |>
    fgroup_by(cache_id, reporting_level, area)|>
    fsummarise(across(c(keep_vars, "survey_mean_lcu_a", "weight_a"), funique))|>
    fungroup()
  
  # National mean
  
  dt_c[, survey_mean_lcu := fmean(survey_mean_lcu_a, w = weight_a),
        by = .(cache_id,reporting_level)]
  
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

db_compute_survey_mean_sac_pipe <- function(cache_tb, gd_mean = NULL) {

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
  
  dt_m <- dt |>
    fsubset(distribution_type == "micro")|>
    fgroup_by(cache_id, reporting_level, area)|>
    fsummarize(across(keep_vars, funique), 
               survey_mean_lcu_a = fmean(welfare, w = weight, na.rm = TRUE),
               weight_a = fsum(weight))|>
    fungroup()|>
    fgroup_by(cache_id, reporting_level)|>
    fmutate(survey_mean_lcu = fmean(survey_mean_lcu_a, w = weight_a))
  
  # For imputations
  
  imp_mean <- dt |>
    fsubset(distribution_type=="imputed")|> 
    fgroup_by(cache_id, reporting_level, 
              imputation_id)|> # Should I include area here?
    fsummarise(survey_mean_imp = fmean(welfare, w = weight, na.rm = TRUE))
  
  dt_i <- dt |>
    joyn::joyn(imp_mean,
               by = c(
      "cache_id","reporting_level",
      "imputation_id"
    ),
    y_vars_to_keep = "survey_mean_imp",
    match_type = "m:1", reportvar = FALSE)|>
    fsubset(distribution_type == "imputed")|>
    fgroup_by(cache_id, reporting_level, area)|>
    fsummarize(across(keep_vars, funique),
               survey_mean_lcu_a = fmean(survey_mean_imp, na.rm = TRUE),
               weight_a = fsum(weight))|>
    fungroup()|>
    fgroup_by(cache_id, reporting_level)|>
    fmutate(survey_mean_lcu = fmean(survey_mean_lcu_a, w = weight_a))
  
  dt_g <- dt |>
    fsubset(distribution_type == "group" | distribution_type == "aggregate")|>
    joyn::joyn(gd_mean,
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

svy_mean_lcu_sac_dt <- db_compute_survey_mean_sac_dt(cache_tb = cache_tb, gd_mean = gd_means_sac)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## svy_mean_lcu_table --------

# Objective:  Local Currency Unit survey mean table 
# 
# tar_target(
#   svy_mean_lcu_table,
#   db_create_lcu_table(
#     dl        = svy_mean_lcu,
#     pop_table = dl_aux$pop,
#     pfw_table = dl_aux$pfw)
# )
#
# Functions used to calculate this:
# adjust_aux_values
#
# Packages needed: tidyfast


svy_mean_lcu_table_tar <- db_create_lcu_table(dl = svy_mean_lcu_tar,
                                              pop_table = dl_aux$pop,
                                              pfw_table = dl_aux$pfw)


# New function to add auxiliary data (pwf and pop) :
# Note: Not many changes for now... (only no rbind at the beginning)

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

svy_mean_lcu_table_sac <- db_create_lcu_table_sac(dt = svy_mean_lcu_sac_dt,
                                              pop_table = dl_aux$pop,
                                              pfw_table = dl_aux$pfw) 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## svy_mean_ppp_table --------

# Objective: Deflated survey mean (DSM) table
#
# tar_target(svy_mean_ppp_table,
#            db_create_dsm_table(
#              lcu_table = svy_mean_lcu_table,
#              cpi_table = dl_aux$cpi,
#              ppp_table = dl_aux$ppp))
#
# Function used to calculate this:
# deflate_welfare_mean

svy_mean_ppp_table_tar <- db_create_dsm_table(lcu_table = svy_mean_lcu_table_tar,
                                              cpi_table = dl_aux$cpi,
                                              ppp_table = dl_aux$ppp)

# New function:

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
  dt$survey_mean_ppp <-
    wbpip::deflate_welfare_mean(
      welfare_mean = dt$survey_mean_lcu, ppp = dt$ppp, cpi = dt$cpi
    )
  # Note: Do we need the function? Faster without it?
  # Example: 
  # dt <- dt|>
  #   fmutate(survey_mean_ppp = survey_mean_lcu / ppp / cpi)
  
  
  #--------- Add comparable spell --------- ## 
  
  dt[, comparable_spell := ifelse(.N == 1,
                                        as.character(reporting_year),
                                        sprintf("%s - %s",
                                                data.table::first(reporting_year),
                                                data.table::last(reporting_year))),
            by = c("country_code", "survey_comparability")
  ]
  
  #--------- Finalize table ---------
  
  # Add is_interpolated column
  dt$is_interpolated <- FALSE
  
  # Add is_used_for_line_up column
  dt <- create_line_up_check(dt)
  
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

# It depends of:
# dl_dist_stats <- svy_mean_ppp_table
#
# Functions needed to calculate this:
# compute_dist_stats, get_dist_stats_by_level,
# gd_dist_stats, id_dist_stats, md_dist_stats,
# get_synth_vector, mean_over_id
#
# Missing cache_ids!

dl_dist_stats_tar <- mp_dl_dist_stats(dt         = cache,
                                      mean_table = svy_mean_ppp_table,
                                      pop_table  = dl_aux$pop,
                                      cache_id   = cache_ids, 
                                      ppp_year   = py)


dt_dist_stats_tar <- db_create_dist_table(dl        = dl_dist_stats,
                                          dsm_table = svy_mean_ppp_table, 
                                          crr_inv   = cache_inventory)
  
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Prod_svy_estimation   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# It depends of: 
# dt_dist_stats

dt_prod_svy_estimation_tar <- db_create_svy_estimation_table(dsm_table = svy_mean_ppp_table, 
                                                             dist_table = dt_dist_stats,
                                                             gdp_table = dl_aux$gdp,
                                                             pce_table = dl_aux$pce) 

