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

## Functions:

source("C:/WBG/Git repos/Personal/SAC/Functions_SAC.R")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 0. Implementation   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Group data means --------

gd_means_sac <- get_groupdata_means_sac(cache_inventory = cache_inventory, 
                                        gdm = dl_aux$gdm)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Means in LCU --------

svy_mean_lcu_sac <- db_compute_survey_mean_sac(cache = cache_tb, 
                                               gd_mean = gd_means_sac)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Append Population and PFW --------

svy_mean_lcu_table_sac <- db_create_lcu_table_sac(dt = svy_mean_lcu_sac,
                                                  pop_table = dl_aux$pop,
                                                  pfw_table = dl_aux$pfw)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Means in PPP --------

svy_mean_ppp_table_sac <- db_create_dsm_table_sac(lcu_table = svy_mean_lcu_table_sac,
                                                  cpi_table = dl_aux$cpi,
                                                  ppp_table = dl_aux$ppp)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Replication --------

svy_mean_ppp <- svy_mean_lcu_table_sac[svy_mean_lcu_table_sac$area == "national" | 
    svy_mean_lcu_table_sac$reporting_level == svy_mean_lcu_table_sac$area, -c("area","weight")]

setkey(svy_mean_lcu_table_tar, "country_code")
setkey(svy_mean_ppp, "country_code")

all.equal(svy_mean_lcu_table_tar,
          svy_mean_ppp[, colnames(svy_mean_lcu_table_tar), with = FALSE])

waldo::compare(svy_mean_lcu_table_tar,
               svy_mean_ppp[, colnames(svy_mean_lcu_table_tar), with = FALSE], tolerance = 1e7)

