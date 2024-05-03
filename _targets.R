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
release            <- "20240429"
identity           <- "PROD"
max_year_country   <- 2022
max_year_aggregate <- 2022

force_create_cache_file         <- TRUE
save_pip_update_cache_inventory <- TRUE
save_mp_cache                   <- TRUE

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Load Packages and Data  ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

base_dir <- fs::path("C:/WBG/Git repos/Packages/GPID/PIP/pip_ingestion_pipeline")

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

## Run common R code   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

base_dir |> 
  fs::path("_common.R") |> 
  source(echo = FALSE)

# base_dir |>
#   fs::path("_cache_loading_saving.R") |>
#   source(echo = FALSE)


## Set targets options   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Check that the correct _targets store is used 

# if (!identical(fs::path(tar_config_get('store')),
#                fs::path(gls$PIP_PIPE_DIR, 'pc_data/_targets2017'))) {
#   stop('The store specified in _targets.yaml doesn\'t match with the pipeline directory')
# }

# filter for testing --------


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2: Run pipeline   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

list(
  
  ## Mean estimates ------------
  
  ### LCU survey means -------
  # tar_target(cache, cache_o, iteration = "list"),
  
  ### Fetch GD survey means and convert them to daily values ----
  tar_target(
    gd_means, 
    get_groupdata_means(cache_inventory = cache_inventory, 
                        gdm            = dl_aux$gdm), 
    iteration = "list"
  ),
  
  ### LCU survey mean list ----
  
  tar_target(
    svy_mean_lcu,
    mp_svy_mean_lcu(cache, gd_means)
  ),
  
  
  ### LCU table ------
  tar_target(
    svy_mean_lcu_table,
    db_create_lcu_table(
      dl        = svy_mean_lcu,
      pop_table = dl_aux$pop,
      pfw_table = dl_aux$pfw)
  ),
  
  
  
  ### Deflated survey mean (DSM) table ---- 
  
  tar_target(svy_mean_ppp_table,
             db_create_dsm_table(
               lcu_table = svy_mean_lcu_table,
               cpi_table = dl_aux$cpi,
               ppp_table = dl_aux$ppp)),
  
  ### Reference year Mean table ------
  
  # tar_target(dt_old_ref_mean_pred,
  #            db_create_ref_year_table(
  #              dsm_table = svy_mean_ppp_table,
  #              gdp_table = dl_aux$gdp,
  #              pce_table = dl_aux$pce,
  #              pop_table = dl_aux$pop,
  #              ref_years = gls$PIP_REF_YEARS,
  #              pip_years = gls$PIP_YEARS,
  #              region_code = 'pcn_region_code')),
  
  tar_target(dt_ref_mean_pred, 
             refy_mean_inc_group(dsm    = svy_mean_ppp_table, 
                                 gls    = gls, 
                                 dl_aux = dl_aux, 
                                 pinv   = pipeline_inventory)),
  # tar_target(dt_ref_mean_pred, 
  #            get_ref_mean_pred(old    = dt_old_ref_mean_pred, 
  #                              new    = dt_refy_mean_inc_group)),
  
  ## Distributional stats ---- 
  
  ### Lorenz curves (for microdata) ----
  tar_target(
    lorenz,
    mp_lorenz(cache)
  ),
  
  
  ### Dist statistics list ------
  
  tar_target(dl_dist_stats,
             mp_dl_dist_stats(dt         = cache,
                              mean_table = svy_mean_ppp_table,
                              pop_table  = dl_aux$pop,
                              cache_id   = cache_ids, 
                              ppp_year   = py)
  ),
  
  ### Dist stat table ------
  
  # Covert dist stat list to table
  tar_target(dt_dist_stats,
             db_create_dist_table(
               dl        = dl_dist_stats,
               dsm_table = svy_mean_ppp_table, 
               crr_inv   = cache_inventory)
  ),
  
  ## Output tables --------
  
  ### Reference Year stimations tables ----
  
  tar_target(dt_prod_ref_estimation,
             db_create_ref_estimation_table(
               ref_year_table = dt_ref_mean_pred, 
               dist_table     = dt_dist_stats)
  ),
  
  ### Survey Year stimations tables ----
  
  tar_target(dt_prod_svy_estimation,
             db_create_svy_estimation_table(
               dsm_table = svy_mean_ppp_table, 
               dist_table = dt_dist_stats,
               gdp_table = dl_aux$gdp,
               pce_table = dl_aux$pce)
  ),
  
  ## SPL ----------
  
  ### Get lineup median -------
  # tar_target(dt_lineup_median, 
  #            db_compute_lineup_median(
  #              ref_lkup = dt_prod_ref_estimation, 
  #              cache    = cache)
  #            ),
  # 
  # ### Get SPL  ----------
  # tar_target(dt_spl, 
  #            db_compute_spl(dt = dt_lineup_median, 
  #                           ppp_year = py)
  #            ),
  # 
  # ### Get SPL headcount -------
  # tar_target(dt_spl_headcount, 
  #            db_compute_lineup_headcount(
  #              ref_lkup = dt_prod_ref_estimation, 
  #              cache    = cache, 
  #              spl      = dt_spl)),
  
  ## Coverage and censoring table -------
  
  ### coverage table by region ----
  tar_target(
    dl_coverage,
    db_create_coverage_table(
      ref_year_table        = dt_ref_mean_pred,
      pop_table             = dl_aux$pop,
      cl_table              = dl_aux$country_list,
      incgrp_table          = dl_aux$income_groups, 
      ref_years             = gls$PIP_REF_YEARS,
      urban_rural_countries = c("ARG", "CHN", "IND", "SUR"),
      digits                = 2
    )
  ),
  
  
  ### Censoring table -------
  
  # Create censoring list
  tar_target(
    dl_censored,
    db_create_censoring_table(
      censored           = dl_aux$censoring,
      coverage_list      = dl_coverage,
      coverage_threshold = 50
    )
  ),
  
  ### Regional population table ----
  
  tar_target(
    dt_pop_region,
    db_create_reg_pop_table(
      pop_table   = dl_aux$pop,
      cl_table    = dl_aux$country_list, 
      region_code = 'pcn_region_code',
      pip_years   = gls$PIP_YEARS)
  ),
  
  ### Decomposition table ----
  
  tar_target(
    dt_decomposition,
    db_create_decomposition_table(
      dsm_table = svy_mean_ppp_table)
  ),
  
  ##  Clean AUX data ------
  
  # Clean and transform the AUX tables to the format
  # used on the PIP webpage.
  tar_target(all_aux,
             list(dl_aux$cpi, dl_aux$gdp, dl_aux$pop,
                  dl_aux$ppp, dl_aux$pce),
             iteration = "list"
  ),
  
  tar_target(aux_names,
             c("cpi", "gdp", "pop", "ppp", "pce"),
             iteration = "list"
  ),
  
  tar_target(
    aux_clean,
    db_clean_aux(all_aux, aux_names, pip_years = gls$PIP_YEARS),
    pattern = map(all_aux, aux_names), 
    iteration = "list"
  ),
  
  # Create Framework data
  tar_target(
    dt_framework,
    create_framework(dl_aux$pfw)
  ),
  
  #~~~~~~~~~~~~~~~~~~~~~~
  ## Save data ---- 
  
  ### survey data ------
  
  tar_target(
    survey_files,
    mp_survey_files(
      cache       = cache,
      cache_ids   = cache_ids,
      output_dir  = gls$OUT_SVY_DIR_PC,
      cols        = c('welfare', 'weight', 'area'),
      compress    = gls$FST_COMP_LVL)
  ),
  
  ### Basic AUX data ----
  
  tar_target(aux_out_files,
             aux_out_files_fun(gls$OUT_AUX_DIR_PC, aux_names)
  ),
  tar_target(aux_out,
             fst::write_fst(x        = aux_clean,
                            path     = aux_out_files,
                            compress = gls$FST_COMP_LVL),
             pattern   = map(aux_clean, aux_out_files),
             iteration = "list"),
  
  tar_files(aux_out_dir, aux_out_files),
  
  ### Additional AUX files ----
  
  #### Countries -----------
  tar_target(
    countries_out,
    save_aux_data(
      dl_aux$countries,
      fs::path(gls$OUT_AUX_DIR_PC, "countries.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Countries with missing data ------------
  tar_target(
    missing_data_out,
    save_aux_data(
      dl_aux$missing_data,
      fs::path(gls$OUT_AUX_DIR_PC, "missing_data.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Country List ---------
  tar_target(
    country_list_out,
    save_aux_data(
      dl_aux$country_list,
      fs::path(gls$OUT_AUX_DIR_PC, "country_list.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Regions -----------
  tar_target(
    regions_out,
    save_aux_data(
      dl_aux$regions,
      fs::path(gls$OUT_AUX_DIR_PC, "regions.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Country profiles  ------------
  tar_target(
    country_profiles_out,
    save_aux_data(
      dl_aux$cp,
      fs::path(gls$OUT_AUX_DIR_PC, "country_profiles.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Poverty lines ---------
  tar_target(
    poverty_lines_out,
    save_aux_data(
      dl_aux$pl,
      fs::path(gls$OUT_AUX_DIR_PC, "poverty_lines.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  tar_target(
    national_poverty_lines_out,
    save_aux_data(
      dl_aux$npl,
      fs::path(gls$OUT_AUX_DIR_PC, "national_poverty_lines.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Survey metadata (for Data Sources page) --------
  tar_target(
    survey_metadata_out,
    save_aux_data(
      dl_aux$metadata,
      fs::path(gls$OUT_AUX_DIR_PC, "survey_metadata.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Indicators ----------
  tar_target(
    indicators_out,
    save_aux_data(
      dl_aux$indicators,
      fs::path(gls$OUT_AUX_DIR_PC, "indicators.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  
  ### Coverage files ----
  
  #### Regional population ---------
  tar_target(
    pop_region_out,
    save_aux_data(
      dt_pop_region,
      fs::path(gls$OUT_AUX_DIR_PC, "pop_region.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Regional coverage  ----------
  tar_target(
    region_year_coverage_out,
    save_aux_data(
      dl_coverage$region,
      fs::path(gls$OUT_AUX_DIR_PC, "region_coverage.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Income group coverage ---------
  tar_target(
    incomeGroup_year_coverage_out,
    save_aux_data(
      dl_coverage$incgrp,
      fs::path(gls$OUT_AUX_DIR_PC, "incgrp_coverage.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Country year coverage --------
  tar_target(
    country_year_coverage_out,
    save_aux_data(
      dl_coverage$country_year_coverage,
      fs::path(gls$OUT_AUX_DIR_PC, "country_coverage.fst"),
      compress = TRUE)
  ),
  
  ### Censored  -----
  tar_target(
    censored_out,
    save_aux_data(
      dl_censored,
      fs::path(gls$OUT_AUX_DIR_PC, "censored.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  
  ### Decomposition master --------
  tar_target(
    decomposition_out,
    save_aux_data(
      dt_decomposition,
      fs::path(gls$OUT_AUX_DIR_PC, "decomposition.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  #### Framework data --------------
  tar_target(
    framework_out,
    save_aux_data(
      dt_framework,
      fs::path(gls$OUT_AUX_DIR_PC, "framework.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  ### Dictionary -------------
  tar_target(
    dictionary_out,
    save_aux_data(
      dl_aux$dictionary,
      fs::path(gls$OUT_AUX_DIR_PC, "dictionary.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  ### SPL  --------------
  # tar_target(
  #   spl_out,
  #   save_aux_data(
  #     dt_spl_headcount,
  #     fs::path(gls$OUT_AUX_DIR_PC, "spl.fst"),
  #     compress = TRUE
  #   ),
  #   format = 'file',
  # ),
  
  ### Estimation tables -------
  
  tar_target(
    prod_ref_estimation_file,
    format = 'file', 
    save_estimations(dt       = dt_prod_ref_estimation, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "prod_ref_estimation", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  tar_target(
    prod_svy_estimation_file,
    format = 'file', 
    save_estimations(dt       = dt_prod_svy_estimation, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "prod_svy_estimation", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  # tar_target(
  #   lineup_median_file,
  #   format = 'file', 
  #   save_estimations(dt       = dt_lineup_median, 
  #                    dir      = gls$OUT_EST_DIR_PC, 
  #                    name     = "lineup_median", 
  #                    time     = gls$TIME, 
  #                    compress = gls$FST_COMP_LVL)
  # ),
  
  ###  Lorenz list ----
  
  tar_target(
    lorenz_out,
    save_aux_data(
      lorenz,
      fs::path(gls$OUT_AUX_DIR_PC, "lorenz.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  ### Dist stats table ----
  
  tar_target(
    dist_file,
    format = 'file',
    save_estimations(dt       = dt_dist_stats, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "dist_stats", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  ### Survey means table ----
  
  tar_target(
    survey_mean_file,
    format = 'file', 
    save_estimations(dt       = svy_mean_ppp_table, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "survey_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  tar_target(
    survey_mean_file_aux,
    format = 'file', 
    save_estimations(dt       = svy_mean_ppp_table, 
                     dir      = gls$OUT_AUX_DIR_PC, 
                     name     = "survey_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  tar_target(
    aux_versions_out,
    format = 'file', 
    save_estimations(dt       = aux_versions, 
                     dir      = gls$OUT_AUX_DIR_PC, 
                     name     = "aux_versions", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  ### Interpolated means table ----
  
  tar_target(
    interpolated_means_file,
    format = 'file', 
    save_estimations(dt       = dt_ref_mean_pred, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "interpolated_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  tar_target(
    interpolated_means_file_aux,
    format = 'file', 
    save_estimations(dt       = dt_ref_mean_pred, 
                     dir      = gls$OUT_AUX_DIR_PC, 
                     name     = "interpolated_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  ### Data timestamp file ----
  
  tar_target(
    data_timestamp_file,
    # format = 'file', 
    writeLines(as.character(Sys.time()), 
               fs::path(gls$OUT_DIR_PC, 
                        gls$vintage_dir, 
                        "data_update_timestamp", 
                        ext = "txt"))
  ), 
  
  ## Convert AUX files  to qs ---------
  tar_target(
    aux_qs_out, 
    convert_to_qs(dir = gls$OUT_AUX_DIR_PC)
  )
)