#function(cache_dir = NULL, 
#         load = TRUE, 
#         save = FALSE, 
#         gls, 
#         cache_ppp) {
  
  
  
  
  
  dir <- fs::path(gls$PIP_PIPE_DIR, "pc_data/cache/global_list/")
  # gio: this was added manually from _cache_loading_saving.R:
  cache_ppp <- gls$cache_ppp
  cache_ids <- get_cache_id(cache_inventory)
  cache_dir <- get_cache_files(cache_inventory)
  names(cache_dir) <-  cache_ids
  
  global_name <- paste0("global_list_", cache_ppp)
  global_file <- fs::path(dir, global_name , ext = "qs")
  
  # gio: we do not need to save files so this is hashed:
  #if (!fs::file_exists(global_file)) {
  #  save <- TRUE
  #  cli::cli_alert("file {.file {global_file}} does not exist. 
  #                  It will be created and saved")
  #}
  
  # gio: we do not need to save files so this is hashed:
  #if (isTRUE(save)) {
    
  #  if (is.null(cache_dir)) {
  #    cli::cli_abort("You must provide a {.code cache_dir} vector")
  #  }
    
  # Rename cache_dir names
    ch_names <- gsub("(.+/)([A-Za-z0-9_\\-]+)(\\.fst$)", "\\2", cache_dir)
    names(ch_names) <- NULL
    names(cache_dir) <- ch_names
    

  #   # gio: removed the saving to avoid issues:
  #   y <- purrr::map(.x = cli::cli_progress_along(ch_names), 
  #                   .f = ~{
  #                     tryCatch(
  #                       expr = {
  #                         # Your code...
  #                         fst::read_fst(path = cache_dir[.x],
  #                                       as.data.table = TRUE)
  #                       }, # end of expr section
  #                       
  #                       error = function(e) {
  #                         NULL
  #                       }, # end of error section
  #                       
  #                       warning = function(w) {
  #                         NULL
  #                       } # end of finally section
  #                       
  #                     ) # End of trycatch
  #                     
  #                   })
  #   
  #   names(y) <- ch_names
  #   
  #   # gio: removed the saving to avoid issues
  #   # qs::qsave(y, global_file)
  #   # readr::write_rds(y, global_file)
  #   
  # }
  
  # if (isTRUE(load)) {
  #   if (isTRUE(save)) { # load from process above
  #     
  #     cli::cli_progress_step("loading from saving process")
  #     x <- y
  #     
  #   } else { # load from file
      
      if (file.exists(global_file)) {
        
        cli::cli_progress_step("Loading list from file")
        
        # x <- readr::read_rds(global_file)
        x <- qs::qread(global_file) # 5:24 -> 
        
      } else {
        cli::cli_abort("file {.file {global_file}} does not exist. 
                       Use option {.code save = TRUE} instead")
      }
      
  #   }
  #   
  # } else {
  #   cli::cli_progress_step("Returning TRUE")
  #   x <- TRUE
  # }
  
  #return(x)
  
#}
