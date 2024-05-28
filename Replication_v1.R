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

base_dir <- fs::path("E:/01.personal/wb622077/pip_ingestion_pipeline")
#base_dir <- fs::path("E:/01.personal/wb535623/PIP/pip_ingestion_pipeline")

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


gls$CACHE_SVY_DIR_PC <- fs::path("E:/01.personal/wb622077/cache")
#gls$CACHE_SVY_DIR_PC <- fs::path("E:/01.personal/wb535623/PIP/Cache")

# base_dir |>
#   fs::path("_cache_loading_saving.R") |>
#   source(echo = FALSE)

# filter for testing --------
cache_inventory <- pipload::pip_load_cache_inventory(version = '20240326_2017_01_02_PROD')
cache_inventory <- cache_inventory[(cache_inventory$cache_id %like% "CHN" | 
                                      cache_inventory$cache_id %like% "BOL" |
                                      cache_inventory$cache_id %like% "NGA"),]
cache <- pipload::pip_load_cache(c("BOL","CHN","NGA"), type="list", version = '20240326_2017_01_02_PROD') 
cache_tb <- pipload::pip_load_cache(c("BOL","CHN","NGA"), version = '20240326_2017_01_02_PROD') 
cache_ids <- get_cache_id(cache_inventory) 


# Alternative:
# cache_dir <- get_cache_files(cache_inventory)
# cache_ids <- names(cache_dir)
  
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
    gd_means        <- dt.[,.(cache_id, pop_data_level, survey_mean_lcu)]
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

# Replicate outcome on table format:
svy_mean_lcu_tb <- data.table::rbindlist(svy_mean_lcu_tar, use.names = TRUE)

# New function (version with cache as table):
# Note: two versions with data.table and pipe.

# db_compute_survey_mean_sac_dt <- function(cache_tb, gd_mean = NULL) {
# 
#   #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # computations   ---------
#   #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # Select variables
#   dt <- cache_tb[, .(welfare, weight, survey_id, cache_id, country_code, 
#                      surveyid_year, survey_acronym, survey_year, welfare_type,
#                      distribution_type, gd_type, imputation_id, cpi_data_level, 
#                      ppp_data_level, gdp_data_level, pce_data_level, 
#                      pop_data_level, reporting_level, area)]
#   
#   keep_vars <- c("survey_id", "country_code", "surveyid_year", 
#                  "survey_acronym","survey_year", "welfare_type", 
#                  "distribution_type","gd_type","cpi_data_level",
#                  "ppp_data_level", "gdp_data_level", 
#                  "pce_data_level", "pop_data_level")
#   
#   # Modify the area variable for imputed or group
#   
#   levels(dt$area)[levels(dt$area)==""] <- "national" 
#   
#   # Need to fix this if not same as reference_level (aggregate)
#   
#   # dt[, area := ifelse(reporting_level!=area, reporting_level, area)] 
#   
#   # Previous step for imputed distribution type
#   
#   dt[, survey_mean_imp := fmean(welfare, w = weight, na.rm = TRUE),
#      by = .(cache_id, reporting_level, area, imputation_id)]
#   
#   # Mean calculations 
#   
#   dt[, ':=' (survey_mean_lcu = ifelse(distribution_type == "micro" | 
#                                         distribution_type == "imputed",
#                                         fmean(survey_mean_imp, na.rm = TRUE),
#                                         ifelse((distribution_type == "group" |
#                                                   distribution_type == "aggregate"),
#                                                as.numeric(gd_mean[cache_id==cache_id[1],
#                                                                   survey_mean_lcu]),
#                                                       NA_integer_)),
#              weight = fsum(weight)),
#      by = .(cache_id,reporting_level, area)]
#   
#   # Note: need to make a check for no NA values
#   
#   # Group variables
#   
#   dt_c <- dt |>
#     fgroup_by(cache_id, reporting_level, area)|>
#     fsummarise(across(c(keep_vars, "survey_mean_lcu", "weight"), funique))|>
#     fungroup()
#   
#   # National mean
#   
#   dt_nat <- dt_c |>
#     fsubset(area != "national")|>
#     fgroup_by(cache_id, reporting_level)|>
#     fsummarise(mean_nat = fmean(survey_mean_lcu, w = weight),
#                weight = fsum(weight),
#                across(c(keep_vars), funique))|>
#     fungroup()|>
#     rename(survey_mean_lcu = mean_nat)|>
#     fmutate(area = factor("national"))
#               
#   dt_c <- rowbind(dt_c, dt_nat)
#   
#   # Order rows
#   dt_c <- dt_c|>
#     roworder(survey_id, country_code, surveyid_year, survey_acronym,
#              survey_year, welfare_type)
#   
#   # Order columns
#   data.table::setcolorder(
#     dt_c, c(
#       "survey_id", "country_code", "surveyid_year", "survey_acronym",
#       "survey_year", "welfare_type"
#     )
#   )
# 
#   #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # Return   ---------
#   #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   return(dt_c)
# 
# }


db_compute_survey_mean_sac_col <- function(cache_tb, gd_mean = NULL) {

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

#svy_mean_lcu_sac_dt <- db_compute_survey_mean_sac_dt(cache_tb = cache_tb, gd_mean = gd_means_sac)


svy_mean_lcu_sac_col <- db_compute_survey_mean_sac_col(cache_tb = cache_tb, gd_mean = gd_means_sac)

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

# New function to add auxiliary data (pwf and pop) :

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

  # Create reporting_pop
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

svy_mean_lcu_table_sac <- db_create_lcu_table_sac(dt = svy_mean_lcu_sac_col,
                                              pop_table = dl_aux$pop,
                                              pfw_table = dl_aux$pfw) 

# to_compare <- svy_mean_lcu_table_sac |> # It does not work (?)
#   fsubset(area == "national" | reporting_level == area)|>
#   fselect(-c(area,weight))

to_compare <- svy_mean_lcu_table_sac[
  svy_mean_lcu_table_sac$area == "national" | 
    svy_mean_lcu_table_sac$reporting_level == svy_mean_lcu_table_sac$area,
  -c("area","weight")]

setkey(svy_mean_lcu_table_tar, "country_code")
setkey(to_compare, "country_code")

all.equal(svy_mean_lcu_table_tar,
          to_compare[, colnames(svy_mean_lcu_table_tar), with = FALSE])

waldo::compare(svy_mean_lcu_table_tar,
               to_compare[, colnames(svy_mean_lcu_table_tar), with = FALSE], tolerance = 1e7)

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

svy_mean_ppp_table_sac <- db_create_dsm_table_sac(lcu_table = svy_mean_lcu_table_sac,
                                              cpi_table = dl_aux$cpi,
                                              ppp_table = dl_aux$ppp)

to_compare <- svy_mean_ppp_table_sac[
  svy_mean_ppp_table_sac$area == "national" | 
    svy_mean_ppp_table_sac$reporting_level == svy_mean_ppp_table_sac$area,
  -c("area")]

to_compare <- as.data.table(lapply(to_compare, function(x) { attributes(x) <- NULL; return(x) }))

data.table::setorder(to_compare, survey_id, cache_id, reporting_level)
data.table::setorder(svy_mean_ppp_table_tar, survey_id, cache_id, reporting_level)

all.equal(svy_mean_ppp_table_tar,to_compare)

waldo::compare(svy_mean_ppp_table_tar,to_compare, tolerance = 1e-7)

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

dl_dist_stats_tar <- mp_dl_dist_stats(dt         = cache,
                                      mean_table = svy_mean_ppp_table_tar,
                                      pop_table  = dl_aux$pop,
                                      cache_id   = cache_ids, 
                                      ppp_year   = py)

# New function:
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
    _[, c(.SD, .( # using _ because we are using native pipe 
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, imputation_id, reporting_level, area, weight)] |>
    fselect(-res)|>
    pivot(ids = 1:5, how="w", values = "Value", names = "Statistic") |>
    fgroup_by(cache_id, reporting_level, area)|>
    fsummarise(across(weight:quantiles10, fmean))|> 
    # weight can be removed if we confirm that we don't use the weighted approach for national
    fungroup()|>
    frename(survey_median_ppp = median)|>
    fmutate(reporting_level = as.character(reporting_level))
  
  setrename(md_id_area, gsub("quantiles", "decile", names(md_id_area)))
  
  # 4. Micro and Imputed Data: National Estimation ----
  ## Note: Not using the weighted approach for now
  md_id_national_complete <- 
    dt |>
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
    _[, c(.SD, .( # using _ because we are using native pipe 
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, imputation_id)] |>
    fselect(-res)|>
    pivot(ids = 1:3, how="w", values = "Value", names = "Statistic") |>
    fgroup_by(cache_id)|>
    fsummarise(across(mean:quantiles10, fmean))|> # weight mean as it should be flattened and it removes it from keys
    fungroup()|>
    frename(survey_median_ppp = median) |>
    fmutate(reporting_level = as.character("national"), 
            area = as.character("national"))
  
  setrename(md_id_national_complete, gsub("quantiles", "decile", 
                                          names(md_id_national_complete)))
  
  
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
    collapse::join(mean_table |> # re-joining national mean for micro imputation
                     fsubset(area == "national") |>
                     fselect(cache_id, survey_mean_ppp),
                   on=c("cache_id"), 
                   validate = "m:1",
                   how = "left",
                   verbose = 0) |>
    roworder(cache_id, welfare) |>
    fgroup_by(cache_id) |>
    fsummarise(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare,
      weight = weight,
      mean = funique(survey_mean_ppp))))|>
    _[, c(.SD, .( # using _ because we are using native pipe 
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
  final <- rbindlist(list(md_id_area |> fselect(-weight), md_id_national_complete, 
                          gd_ag_area, ag_national), use.names = TRUE)
  
  return(final)
  
}

dl_dist_stats_sac <- mp_dl_dist_stats_sac(dt = cache_tb,
                                          mean_table = svy_mean_ppp_table_sac)


dt_dist_stats_tar <- db_create_dist_table(dl        = dl_dist_stats_tar,
                                          dsm_table = svy_mean_ppp_table_tar, 
                                          crr_inv   = cache_inventory)

# New function:



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Prod_svy_estimation   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# It depends of: 
# svy_mean_ppp_table, dt_dist_stats

# Note: No need of SAC

dt_prod_svy_estimation_tar <- db_create_svy_estimation_table(dsm_table = svy_mean_ppp_table, 
                                                             dist_table = dt_dist_stats,
                                                             gdp_table = dl_aux$gdp,
                                                             pce_table = dl_aux$pce) 

