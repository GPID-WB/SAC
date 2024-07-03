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

# install.packages(c("conflicted", "dotenv", "targets", "tarchetypes",
# "bs4Dash", "clustermq", "future", "gt", "pingr", "shinycssloaders",
# "shinyWidgets", "visNetwork", "fastverse", "tidyfast", "tidyr",
# "assertthat", "config"))

# remotes::install_github("PIP-Technical-Team/pipload@dev", dependencies = FALSE)
# remotes::install_github("PIP-Technical-Team/wbpip", dependencies = FALSE)

# remotes::install_github("PIP-Technical-Team/pipfun@ongoing")
# remotes::install_github("PIP-Technical-Team/pipload@ongoing")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Defaults   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

py                 <- 2017  # PPP year
branch             <- "DEV"
release            <- "20240326"  
identity           <- "PROD"
max_year_country   <- 2022
max_year_aggregate <- 2022

## filter creation of synth data
cts <- yrs <- NULL

## save data
force_create_cache_file         <- FALSE
save_pip_update_cache_inventory <- FALSE
force_gd_2_synth                <- FALSE
save_mp_cache                   <- FALSE

config <- config::get(config = Sys.info()['user'])
base_dir <- config$base_dir

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Load Packages and Data  ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Load packages
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

source("_common_SAC_old.R", echo = FALSE) 

# base_dir |> 
#   fs::path("_cache_loading_saving.R") |> 
#   source(echo = FALSE)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Load test data   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## The following file is run in the pipeline but we skipped it to save time
#
# base_dir |>
#   fs::path("_cache_loading_saving.R") |>
#   source(echo = FALSE)

### Cache inventory ---------

cache_inventory <- pipload::pip_load_cache_inventory(version = gls$vintage_dir)

# Eliminate duplicates on cache_inventory
cache_inventory <- cache_inventory[!duplicated(cache_inventory,by = c("survey_id","welfare_type")),]
cache_ids <- get_cache_id(cache_inventory)
cache_dir <- get_cache_files(cache_inventory)



### Full Cache ---------

# In list format:

cache <- pipload::pip_load_cache(type="list", version = gls$vintage_dir)

# remove all the surveys that are not available in the PFW ----

source("PFW_fix.R")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Load SAC Functions   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Note: Each function is better described in Functions_SAC.R 

source("Functions_SAC.R")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 0. Create cache for SAC  ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

cache_sac <- get_cache(cache)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Survey Means    ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1 SAC --------


Means_pipeline_sac <- function(cache_inventory, 
                               cache, 
                               dl_aux){
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Group data means --------
  
  gd_means_sac <- get_groupdata_means_sac(cache_inventory = cache_inventory, 
                                          gdm = dl_aux$gdm)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Means in LCU --------
  
  svy_mean_lcu_sac <- db_compute_survey_mean_sac(cache = cache, 
                                                 gd_mean = gd_means_sac)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Append Population and PFW --------
  
  svy_mean_lcu_table_sac <- db_create_lcu_table_sac(dt = svy_mean_lcu_sac,
                                                    pop_table = dl_aux$pop,
                                                    pfw_table = dl_aux$pfw)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Means in PPP and other changes --------
  
  svy_mean_ppp_table_sac <- db_create_dsm_table_sac(lcu_table = svy_mean_lcu_table_sac)
  
  return(svy_mean_ppp_table_sac)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2 Target  --------

Means_pipeline_tar <- function(cache_inventory, 
                               cache, 
                               dl_aux){
  
  gd_means_tar <- get_groupdata_means(cache_inventory = cache_inventory, 
                                      gdm = dl_aux$gdm)
  
  svy_mean_lcu_tar <- mp_svy_mean_lcu(cache,  
                                      gd_means_tar) 
  
  svy_mean_lcu_table_tar <- db_create_lcu_table(dl = svy_mean_lcu_tar,
                                                pop_table = dl_aux$pop,
                                                pfw_table = dl_aux$pfw)
  
  svy_mean_ppp_table_tar <- db_create_dsm_table(lcu_table = svy_mean_lcu_table_tar,
                                                cpi_table = dl_aux$cpi,
                                                ppp_table = dl_aux$ppp)
  
  return(svy_mean_ppp_table_tar)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Replication Survey Means  ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Load output:

means_out_sac <- Means_pipeline_sac(cache_inventory, 
                                    cache_sac, 
                                    dl_aux)

means_out_tar <- Means_pipeline_tar(cache_inventory, 
                                    cache, 
                                    dl_aux)


# Filter without new area-level calculations
# compare_sac <- means_out_sac[means_out_sac$area == "national" | 
#                                means_out_sac$reporting_level == means_out_sac$area, -c("area")]

# Eliminate attributes 
compare_sac <- as.data.table(lapply(means_out_sac, function(x) { attributes(x) <- NULL; return(x) }))

# Order rows
data.table::setorder(compare_sac, survey_id, cache_id, reporting_level)
data.table::setorder(means_out_tar, survey_id, cache_id,reporting_level)

# Set similar keys
setkey(means_out_tar, "country_code")
setkey(compare_sac, "country_code")

# Order columns
compare_sac <- compare_sac[, colnames(means_out_tar), with = FALSE]

# Comparison
all.equal(means_out_tar,compare_sac)

waldo::compare(means_out_tar,compare_sac, tolerance = 1e-6)

rm(compare_sac)

# Filter without survey_mean_ppp, cpi and ppp
compare_sac <- means_out_sac[, -c("survey_mean_ppp","ppp","cpi")]
compare_tar <- means_out_tar[, -c("survey_mean_ppp","ppp","cpi")]

# Eliminate attributes
compare_sac <- as.data.table(lapply(compare_sac, function(x) { attributes(x) <- NULL; return(x) }))

# Order rows
data.table::setorder(compare_sac, survey_id, cache_id, reporting_level,pop_data_level)
data.table::setorder(compare_tar, survey_id, cache_id,reporting_level, pop_data_level)

# Set similar keys
setkey(compare_tar, "country_code")
setkey(compare_sac, "country_code")

# Order columns
compare_sac <- compare_sac[, colnames(compare_tar), with = FALSE]

# Comparison
all.equal(compare_tar,compare_sac)

waldo::compare(compare_tar,compare_sac, tolerance = 1e-7)

rm(compare_sac,compare_tar)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Dist Stats   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.1 SAC --------

Dist_stats_sac <- function(cache, 
                           dsm_table,
                           cache_inventory){
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Calculate Distributional Statistics --------
  
  db_dist_stats <- db_dist_stats_sac(cache = cache,
                                     mean_table = dsm_table)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Add/remove relevant variables --------
  
  dt_dist_stats_sac <- db_create_dist_table_sac(dt = db_dist_stats,
                                                dsm_table = dsm_table,
                                                cache_inventory = cache_inventory)
  
  return(dt_dist_stats_sac)
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.2 Target --------

Dist_stats_tar <- function(cache,
                           dsm_table,
                           dl_aux,
                           cache_ids,
                           py,
                           cache_inventory){
  
  dl_dist_stats_tar <- mp_dl_dist_stats(dt         = cache,
                                        mean_table = dsm_table,
                                        pop_table  = dl_aux$pop,
                                        cache_id   = cache_ids, 
                                        ppp_year   = py)
  
  dt_dist_stats_tar <- db_create_dist_table(dl        = dl_dist_stats_tar,
                                            dsm_table = dsm_table, 
                                            crr_inv   = cache_inventory)
  return(dt_dist_stats_tar)
  
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 4. Replication Dist Stats   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Load output:
dist_out_sac <- Dist_stats_sac(cache = cache_sac, 
                               dsm_table = means_out_sac,
                               cache_inventory = cache_inventory)

dist_out_tar <- Dist_stats_tar(cache = cache,
                               dsm_table = means_out_tar, 
                               dl_aux = dl_aux,
                               cache_ids = cache_ids,
                               py = py,
                               cache_inventory = cache_inventory)



# Filter without new area-level calculations
# compare_sac <- dist_out_sac[dist_out_sac$reporting_level == dist_out_sac$area, -c("area")]

compare_sac <- dist_out_sac

# Eliminate attributes 
compare_sac <- as.data.table(lapply(compare_sac, function(x) { attributes(x) <- NULL; return(x) }))

# Order rows
data.table::setorder(compare_sac, cache_id, reporting_level)
data.table::setorder(dist_out_tar, cache_id, reporting_level)

# Set similar keys
setkey(dist_out_tar, "country_code")
setkey(compare_sac, "country_code")

# Comparison
all.equal(dist_out_tar,compare_sac)

waldo::compare(dist_out_tar,compare_sac, tolerance = 1e-7)

rm(compare_sac)

# Filter without the changes in ppp/cpi
cpi_same <- means_out_tar|>
  joyn::joyn(means_out_sac,
                   by = c("cache_id", 
                          "cpi_data_level", 
                          "ppp_data_level",
                          "cpi",
                          "ppp"
                   ),
                   match_type = "1:1",
                   keep = "left",
                   y_vars_to_keep = FALSE)|>
  fsubset(.joyn != "x" & (!is.na(cpi)|!is.na(ppp)))|>
  fselect(-.joyn)
  
compare_sac <- dist_out_sac|>
  joyn::joyn(cpi_same,
             by = c("cache_id", "reporting_level"
             ),
             match_type = "1:1",
             keep = "left",
             y_vars_to_keep = FALSE) |>
  fsubset(.joyn != "x")|>
  fselect(-c(.joyn))

compare_tar <- dist_out_tar|>
  joyn::joyn(cpi_same,
             by = c("cache_id", "reporting_level"
             ),
             match_type = "1:1",
             keep = "left",
             y_vars_to_keep = FALSE) |>
  fsubset(.joyn != "x")|>
  fselect(-c(.joyn))

# Eliminate attributes 
compare_sac <- as.data.table(lapply(compare_sac, function(x) { attributes(x) <- NULL; return(x) }))

# Order rows
data.table::setorder(compare_sac, cache_id, reporting_level)
data.table::setorder(compare_tar, cache_id, reporting_level)

# Set similar keys
setkey(compare_tar, "country_code")
setkey(compare_sac, "country_code")

# Comparison
all.equal(compare_tar,compare_sac)

waldo::compare(compare_tar,compare_sac, tolerance = 1e-7)

rm(compare_sac,compare_tar)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 5. Prod_svy_estimation   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# IMPORTANT NOTE: Both functions give the same warning on CHN missing ppp

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.1 SAC --------

### Final table with means, dist stats, gdp and pce  ---------

Prod_svy_estimation_sac <- db_create_svy_estimation_table_sac(dsm_table = means_out_sac, 
                                                              dist_table = dist_out_sac,
                                                              gdp_table = dl_aux$gdp,
                                                              pce_table = dl_aux$pce)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.2 Target --------

Prod_svy_estimation_tar <- db_create_svy_estimation_table(dsm_table = means_out_tar, 
                                                          dist_table = dist_out_tar,
                                                          gdp_table = dl_aux$gdp,
                                                          pce_table = dl_aux$pce) 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 6. Replication Prod_svy_estimation   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Filter without new area-level calculations
# to_compare <- Prod_svy_estimation_sac[
#   Prod_svy_estimation_sac$area == "national" | 
#     Prod_svy_estimation_sac$reporting_level == Prod_svy_estimation_sac$area,
#   -c("area")]

# Eliminate attributes 
to_compare <- as.data.table(lapply(Prod_svy_estimation_sac, function(x) { attributes(x) <- NULL; return(x) }))

# Set similar keys
setkey(Prod_svy_estimation_tar, "country_code")
setkey(to_compare, "country_code")

# Comparison
all.equal(Prod_svy_estimation_tar,to_compare)

waldo::compare(Prod_svy_estimation_tar,to_compare, tolerance = 1e-7)

rm(to_compare)

# Filter without the changes in ppp/cpi and survey_median_ppp

compare_tar <- Prod_svy_estimation_tar|>
  joyn::joyn(cpi_same,
             by = c("cache_id", "reporting_level"
             ),
             match_type = "1:1",
             keep = "left",
             y_vars_to_keep = FALSE) |>
  fsubset(.joyn != "x" & !is.na(survey_median_ppp))|>
  fselect(-c(.joyn))

compare_sac <- Prod_svy_estimation_sac|>
  joyn::joyn(cpi_same,
             by = c("cache_id", "reporting_level"
             ),
             match_type = "1:1",
             keep = "left",
             y_vars_to_keep = FALSE) |>
  fsubset(.joyn != "x" )|>
  fselect(-c(.joyn))|>
  joyn::joyn(compare_tar,
             by = c("cache_id", "pop_data_level", "reporting_level"
             ),
             match_type = "1:1",
             keep = "left",
             y_vars_to_keep = FALSE) |>
  fsubset(.joyn != "x")|>
  fselect(-c(.joyn))

# Eliminate attributes 
compare_sac <- as.data.table(lapply(compare_sac, function(x) { attributes(x) <- NULL; return(x) }))

# Set similar keys
setkey(compare_tar, "country_code")
setkey(compare_sac, "country_code")

# Comparison
all.equal(compare_tar,compare_sac)

waldo::compare(compare_tar,compare_sac, tolerance = 1e-7)
