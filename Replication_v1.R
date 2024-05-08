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

# base_dir |>
#   fs::path("_cache_loading_saving.R") |>
#   source(echo = FALSE)

# filter for testing --------
cache_inventory <- pipload::pip_load_cache_inventory(version = '20240326_2017_01_02_PROD')
cache_inventory <- cache_inventory[cache_inventory$cache_id %like% "COL",]
cache <- pipload::pip_load_cache("COL", version = '20240326_2017_01_02_PROD') 

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
# db_compute_survey_mean <- compute_survey_mean
# compute_survey_mean uses: md_compute_survey_mean, gd_compute_survey_mean,
#                           gd_compute_survey_mean, id_compute_survey_mean

svy_mean_lcu_tar <- mp_svy_mean_lcu(cache, gd_means_tar) #Does not work because of size


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
