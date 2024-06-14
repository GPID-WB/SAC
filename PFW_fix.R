# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Objective:     PFW fix to cache
# Author:        Diana C. Garcia Rojas
# Dependencies:  The World Bank
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Creation Date:    June 2024
# References:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Remove surveys that are not in PFW   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# NOTE: This piece of code was taken from _cache_loading_saving.R file

### remove all the surveyar that are not available in the PFW ----

svy_in_pfw <- dl_aux$pfw[, link]

pattern <-  "([[:alnum:]]{3}_[[:digit:]]{4}_[[:alnum:]\\-]+)(.*)"

cache_names <- 
  names(cache_ls) |> 
  gsub(pattern = pattern, 
       replacement = "\\1", 
       x = _)

#names(cache_dir) <-  cache_ids

cache_dir_names <- 
  cache_ids |> 
  gsub(pattern = pattern, 
       replacement = "\\1", 
       x = _)

to_drop_cache     <- which(!cache_names %in% svy_in_pfw)
to_drop_cache_dir <- which(!cache_dir_names %in% svy_in_pfw)

cache_ls[to_drop_cache]         <- NULL
if (length(to_drop_cache_dir) > 0)
  cache_dir <- cache_dir[-to_drop_cache_dir]

cache_inventory[,
                cache_names := gsub(pattern = pattern, 
                                    replacement = "\\1", 
                                    x = cache_id)]

cache_inventory <- cache_inventory[cache_names %chin% svy_in_pfw]

cache_ids <- names(cache_dir)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## notify that lenghts are different ---

if (length(cache_ls) != length(cache_dir)) {
  cli::cli_abort("Lengths of cache list ({length(cache_ls)}) and cache directory 
                 ({length(cache_dir)}) are not the same")
}

