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
# Load SAC Functions   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

source("C:/WBG/Git repos/Personal/SAC/Functions_SAC.R")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. SAC    ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Means_pipeline_sac <- function(cache_inventory, cache, dl_aux){
  
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
  return(svy_mean_ppp_table_sac)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2. Target  --------

Means_pipeline_tar <- function(cache_inventory, cache, dl_aux){
  
  gd_means_tar <- get_groupdata_means(cache_inventory = cache_inventory, gdm = dl_aux$gdm)
  
  svy_mean_lcu_tar <- mp_svy_mean_lcu(cache, gd_means_tar) 
  
  svy_mean_lcu_table_tar <- db_create_lcu_table(dl = svy_mean_lcu_tar,
                                                pop_table = dl_aux$pop,
                                                pfw_table = dl_aux$pfw)
  
  svy_mean_ppp_table_tar <- db_create_dsm_table(lcu_table = svy_mean_lcu_table_tar,
                                                cpi_table = dl_aux$cpi,
                                                ppp_table = dl_aux$ppp)
  
  return(svy_mean_ppp_table_tar)
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Replication   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

out_sac <- Means_pipeline_sac(cache_inventory, cache, dl_aux)
out_tar <- Means_pipeline_tar(cache_inventory, cache, dl_aux)

compare_sac <- out_sac[out_sac$area == "national" | 
    out_sac$reporting_level == out_sac$area, -c("area")]

compare_sac <- as.data.table(lapply(compare_sac, function(x) { attributes(x) <- NULL; return(x) }))

data.table::setorder(compare_sac, survey_id, cache_id, reporting_level)
data.table::setorder(out_tar, survey_id, cache_id, reporting_level)

all.equal(out_tar,compare_sac)

waldo::compare(out_tar,compare_sac, tolerance = 1e-7)

