### Filter pipline inventory ---- 

# pipeline_inventory <-
#   pipeline_inventory[grepl("^SOM", cache_id)]
# 

# pipeline_inventory <-
#   pipeline_inventory[grepl("^IND_201[5-9]", cache_id)]

# pipeline_inventory <-
#   pipeline_inventory[grepl("^NIC", cache_id)]

# Uncomment for specific countries
# pipeline_inventory <-
# pipeline_inventory[country_code == 'PHL' & surveyid_year == 2000]


# cts_filter <- c('COL', 'IND', "CHN")
# pipeline_inventory <-
#    pipeline_inventory[country_code %in% cts_filter
#                       ][!(country_code == 'CHN' & surveyid_year >= 2017)]

# pipeline_inventory <-
#    pipeline_inventory[surveyid_year >= 2021]

# cts_filter <- c("CHN")
# pipeline_inventory <-
#    pipeline_inventory[country_code == 'TWN' 
#                       & surveyid_year >= 2022]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Cache files ----

### Create Cache files --------
status_cache_files_creation <- 
  create_cache_file(
    pipeline_inventory = pipeline_inventory,
    pip_data_dir       = gls$PIP_DATA_DIR,
    tool               = "PC",
    cache_svy_dir      = gls$CACHE_SVY_DIR_PC,
    compress           = gls$FST_COMP_LVL,
    force              = force_create_cache_file,
    verbose            = TRUE,
    cpi_table          = dl_aux$cpi,
    ppp_table          = dl_aux$ppp, 
    pfw_table          = dl_aux$pfw, 
    pop_table          = dl_aux$pop)

# Create cache file


### bring cache our of pipeline -----

cache_inventory <- 
  pip_update_cache_inventory(
    pipeline_inventory = pipeline_inventory,
    pip_data_dir       = gls$PIP_DATA_DIR,
    cache_svy_dir      = gls$CACHE_SVY_DIR_PC,
    tool               = "PC", 
    save               = save_pip_update_cache_inventory, 
    load               = TRUE, 
    verbose            = TRUE
  )


# to filter temporarily
# 
# cache_inventory  <-
#   cache_inventory[!(grepl("^(CHN)", survey_id) &
#                       stringr::str_extract(survey_id, "([0-9]{4})") >= 2019)]

# reg <- paste0("^(", paste(cts_filter, collapse = "|"),")")
# 
# cache_inventory <- cache_inventory[grepl(reg, survey_id)]


### Load or create cache list -----------

cache_ppp <- gls$cache_ppp
cache_ids <- get_cache_id(cache_inventory)
cache_dir <- get_cache_files(cache_inventory)
names(cache_dir) <-  cache_ids

cache   <- mp_cache(cache_dir = cache_dir, 
                    load      = TRUE, 
                    save      = save_mp_cache, 
                    gls       = gls, 
                    cache_ppp = cache_ppp)

cache <- purrr::compact(cache)
# selected_files <- which(grepl(reg, names(cache)))
# cache <- cache[selected_files]

# remove CHN 2017 and 2018 manually
# cache[grep("CHN_201[78]", names(cache), value = TRUE)] <- NULL


### remove all the surveyar that are not available in the PFW ----

svy_in_pfw <- dl_aux$pfw[, link] 

pattern <-  "([[:alnum:]]{3}_[[:digit:]]{4}_[[:alnum:]\\-]+)(.*)"
cache_names <- 
  names(cache) |> 
  gsub(pattern = pattern, 
       replacement = "\\1", 
       x = _)

cache_dir_names <- 
  names(cache_dir) |> 
  gsub(pattern = pattern, 
       replacement = "\\1", 
       x = _)

to_drop_cache     <- which(!cache_names %in% svy_in_pfw)
to_drop_cache_dir <- which(!cache_dir_names %in% svy_in_pfw)

cache[to_drop_cache]         <- NULL
if (length(to_drop_cache_dir) > 0)
  cache_dir <- cache_dir[-to_drop_cache_dir]

cache_inventory[,
                cache_names := gsub(pattern = pattern, 
                                    replacement = "\\1", 
                                    x = cache_id)]

cache_inventory <- cache_inventory[cache_names %chin% svy_in_pfw]

cache_ids <- names(cache_dir)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## notify that cache has finished loading (please do NOT delete) ---


if (requireNamespace("pushoverr")) {
  pushoverr::pushover("Finished loading or creating cache list")
}



if (length(cache) != length(cache_dir)) {
  cli::cli_abort("Lengths of cache list ({length(cache)}) and cache directory 
                 ({length(cache_dir)}) are not the same")
}
