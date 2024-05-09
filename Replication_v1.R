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

base_dir <- fs::path("E:/01.personal/wb535623/PIP/pip_ingestion_pipeline")

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

gls$CACHE_SVY_DIR_PC <- fs::path("E:/01.personal/wb535623/PIP/Cache")

# base_dir |>
#   fs::path("_cache_loading_saving.R") |>
#   source(echo = FALSE)

# filter for testing --------
cache_inventory <- pipload::pip_load_cache_inventory(version = '20240326_2017_01_02_PROD')
cache_inventory <- cache_inventory[cache_inventory$cache_id %like% "PRY",]
cache <- pipload::pip_load_cache("PRY", type="list", version = '20240326_2017_01_02_PROD') 
cache_tb <- pipload::pip_load_cache("PRY", version = '20240326_2017_01_02_PROD') 

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
  dt. <- joyn::joyn(x          = cache_inventory,
                    y          = gdm,
                    by         = c("survey_id", "welfare_type"),
                    match_type = "1:m",
                    y_vars_to_keep = c("survey_mean_lcu", "pop_data_level"),
                    keep       = "left")
  
  data.table::setorder(dt., cache_id, pop_data_level)
  gd_means        <- dt.[,.(cache_id, survey_mean_lcu)]
  gd_means        <- gd_means[,survey_mean_lcu:= survey_mean_lcu*(12/365)]
  
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
#                           gd_compute_survey_mean, id_compute_survey_mean

svy_mean_lcu_tar <- mp_svy_mean_lcu(cache, gd_means_tar) 

# New function:

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

svy_mean_lcu_sac <- cache|>
  purrr::map(\(x) db_compute_survey_mean_sac(dt = x, gd_mean = gd_means_sac))

# Version with cache as table:

svy_mean_lcu_sac_tb <- cache_tb|>
  collapse::fgroup_by(cache_id, reporting_level, area)|> 
  collapse::fsummarize(survey_mean_lcu = 
                         fmean(welfare, w = weight, na.rm = TRUE))|>
  collapse::fungroup()


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

svy_mean_lcu_table_sac <- db_create_lcu_table(dl = svy_mean_lcu_sac,
                                              pop_table = dl_aux$pop,
                                              pfw_table = dl_aux$pfw) 


# Note: We might not need to optimize this function if we start with a table

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

db_create_dsm_table_sac <- function(lcu_table,
                                    cpi_table,
                                    ppp_table) {
  
  
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
    .joyn != "y" # This is unnecessary data in cpi table... should we have it?
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
  
  
  #--------- Add comparable spell --------- ## Change this. 
  
  dl <- split(dt, list(dt$country_code, dt$survey_comparability))
  dl <- lapply(dl, function(x) {
    if (nrow(x) == 1) {
      x$comparable_spell <- x$reporting_year
    } else {
      x$comparable_spell <-
        sprintf(
          "%s - %s",
          x$reporting_year[1],
          x$reporting_year[length(x$reporting_year)]
        )
    }
    return(x)
  })
  dt <- data.table::rbindlist(dl)
  
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
  dt <- add_aggregated_mean(dt)
  
  # Sort rows
  data.table::setorder(dt, survey_id)
  
  # change factors to characters
  nn <- names(dt[, .SD, .SDcols = is.factor])
  dt[, (nn) := lapply(.SD, as.character),
     .SDcols = nn]
  
  return(dt)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Dist_stats   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# It depends of:
# dl_dist_stats <- svy_mean_ppp_table


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Prod_svy_estimation   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# It depends of: 
# dt_dist_stats
