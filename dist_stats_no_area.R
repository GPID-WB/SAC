# cache_sac used instead of cache, make sure you have cache_sac created first
# This file creates "final" which should be comparable to the TAR version.
mean_table <- means_out_sac

# 1. Select variables and subset for Micro data----

dt_m <- cache_sac |>
  # GC Note: here I add additional variables we need to group for, and remove area.
  #fselect(cache_id, distribution_type, reporting_level, imputation_id,
  #        area, weight, welfare_ppp, ) |>
  fselect(cache_id, distribution_type, cpi_data_level, ppp_data_level,
          gdp_data_level, pce_data_level,
          pop_data_level, reporting_level, 
          imputation_id, weight, welfare_ppp) |>
  fsubset(distribution_type %in% c("micro", "imputed")) # takes some time

# 2. Micro and Imputed Data: Level & Area Estimation  ----
# GC Note: Now this is not Level & Area estimation, it is just level estimation.


md_id_area <- dt_m |>
  fselect(-c(distribution_type))|>
  #roworder(cache_id, imputation_id, reporting_level, area, welfare_ppp)|>
  roworder(cache_id, imputation_id, cpi_data_level, ppp_data_level,
           gdp_data_level, pce_data_level,
           pop_data_level, reporting_level, welfare_ppp) |>
  _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp,
                                weight  = weight)),
    # Note: Here we remove area from the grouping too, and we add the other levels:
    by = .(cache_id, imputation_id, cpi_data_level, ppp_data_level,
           gdp_data_level, pce_data_level,
           pop_data_level, reporting_level)]|>
  # Note: and here again, area out, other levels in:
  fgroup_by(cache_id, cpi_data_level, ppp_data_level,
            gdp_data_level, pce_data_level,
            pop_data_level, reporting_level)|>
  collapg(fmean, cols = c("mean","median","gini",
                          "polarization","mld",
                          paste0("decile",1:10)))|>
  fungroup()|>
  frename(survey_median_ppp = median)|>
  fmutate(reporting_level = as.character(reporting_level))
# 199 seconds

# 3. Micro and Imputed Data: National Estimation ----
# This in theory is not needed anymore, because we are just using level estimation.
# md_id_national <- dt_m |>
#   fsubset(reporting_level == 'national' & area != "national") |>
#   fselect(-c(distribution_type))|>
#   roworder(cache_id, imputation_id, welfare_ppp)|>
#   _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp, weight = weight)),
#     by = .(cache_id, imputation_id)]|>
#   fgroup_by(cache_id)|>
#   collapg(fmean, cols = c("mean","median","gini",
#                           "polarization","mld",
#                           paste0("decile",1:10)))|>
#   fungroup()|>
#   frename(survey_median_ppp = median)|>
#   fmutate(reporting_level = as.character("national"),
#           area = as.character("national"))

if(any(cache_sac$distribution_type %in% c("group", "aggregate"))){
  
  
  # Select variables, subset and join mean table
  dt_jn <- cache_sac |>
    # GC NOte: same procedure as before, but we take welfare instead of welfare_ppp:
    #fselect(cache_id, distribution_type, reporting_level, imputation_id,
    #        area, weight, welfare)|>
    fselect(cache_id, distribution_type, imputation_id, cpi_data_level, ppp_data_level,
            gdp_data_level, pce_data_level,
            pop_data_level, reporting_level, weight, welfare) |>
    fsubset(distribution_type %in% c("group", "aggregate"))|>
    collapse::join(mean_table |> 
                     # GC Note: Added variables here and also joined by additional variables:
                     fselect(cache_id, cpi_data_level, ppp_data_level,
                             gdp_data_level, pce_data_level,
                             pop_data_level, reporting_level, 
                             survey_mean_ppp, reporting_pop),
                   on=c("cache_id", "cpi_data_level", "ppp_data_level",
                        "gdp_data_level", "pce_data_level",
                        "pop_data_level", "reporting_level"), 
                   # it is actually over-identified at this stage.
                   validate = "m:1",
                   verbose = 0,
                   column = list(".joyn", c("x", "y", "x & y")))
  
  
  # MISSING WARNING MESSAGE
  
  dt_jn <- dt_jn|>
    fsubset(.joyn != "y")|>
    fselect(-.joyn)
  
  
  # 4. Group and Aggregate Data: Level and Area Estimation -----
  
  
  # this gives an error with wbpip, so we catch them as in TAR.
  gd_ag_area <- dt_jn |>
    fselect(-c(distribution_type, reporting_pop)) |>
    #roworder(cache_id, reporting_level, area, welfare)|>
    roworder(cache_id, cpi_data_level, ppp_data_level,
             gdp_data_level, pce_data_level,
             pop_data_level, reporting_level, welfare) |>
    _[, as.list(safe_wrp_gd_dist_stats(welfare = welfare,
                                       population = weight,
                                       mean = funique(survey_mean_ppp))),
      by = .(cache_id, cpi_data_level, ppp_data_level,
             gdp_data_level, pce_data_level,
             pop_data_level, reporting_level)]|>
    frename(survey_median_ppp = median)|>
    fmutate(reporting_level = as.character(reporting_level))
  
  
  setrename(gd_ag_area, gsub("deciles", "decile", names(gd_ag_area)))
  
  # 5. Aggregate Data: National estimation (synth needed) ----
  
  ag_syn <- dt_jn |>
    fsubset(distribution_type %in% c("aggregate")) |>
    fselect(-c(distribution_type))|>
    # GC Note: We remove the area here:
    roworder(cache_id, cpi_data_level, ppp_data_level,
             gdp_data_level, pce_data_level,
             pop_data_level, reporting_level, welfare) |>
    # GC NOte: And here too:
    fgroup_by(cache_id, cpi_data_level, ppp_data_level,
              gdp_data_level, pce_data_level,
              pop_data_level, reporting_level)|>
    fsummarise(welfare =  wbpip:::sd_create_synth_vector(
      welfare = welfare,
      population = weight,
      mean = funique(survey_mean_ppp),
      pop = funique(reporting_pop))$welfare,
      weight = funique(reporting_pop)/100000) 
  
  # Aggregate to national
  
  ag_national <- ag_syn |> 
    roworder(cache_id, cpi_data_level, ppp_data_level,
             gdp_data_level, pce_data_level,
             pop_data_level, reporting_level, welfare)|>
    _[, as.list(wrp_md_dist_stats(welfare = welfare, weight = weight)),
      by = .(cache_id, cpi_data_level, ppp_data_level,
             gdp_data_level, pce_data_level,
             pop_data_level, reporting_level)]|>
    fgroup_by(cache_id, cpi_data_level, ppp_data_level,
              gdp_data_level, pce_data_level,
              pop_data_level, reporting_level)|>
    collapg(fmean, cols = c("mean","median","gini",
                            "polarization","mld",
                            paste0("decile",1:10)))|>
    fungroup()|>
    frename(survey_median_ppp = median)|>
    fmutate(reporting_level = as.character("national"))
  
  # 6. Row bind and return ----
  
  final <- rowbind(md_id_area, gd_ag_area, ag_national)
}
