# cache_sac used instead of cache, make sure you have cache_sac created first
# This file creates "final" which is comparable to the TAR version.
mean_table <- means_out_sac

# 1. Select variables and subset for Micro data----

dt_m <- cache_sac |>
  fselect(cache_id, distribution_type, cpi_data_level, ppp_data_level,
          gdp_data_level, pce_data_level,
          pop_data_level, reporting_level, 
          imputation_id, area, weight, welfare_ppp) |>
  fsubset(distribution_type %in% c("micro", "imputed")) 

# 2. Micro and Imputed Data: Level & Area Estimation  ----
# GC Note: Now this is not Level & Area estimation, it is just level estimation.

md_level <- dt_m |>
  fsubset(distribution_type %in% c("micro"))
fselect(-c(distribution_type))|>
  roworder(cache_id, imputation_id, pop_data_level, welfare_ppp) |>
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
                 column = list(".joyn", c("x", "y", "x & y")))|>
  _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp,
                                weight  = weight,
                                mean = funique(survey_mean_ppp))),
    # Note: Here we remove area from the grouping too, and we add the other levels:
    by = .(cache_id, imputation_id, cpi_data_level, ppp_data_level,
           gdp_data_level, pce_data_level,
           pop_data_level, reporting_level)]|>
  fungroup()|>
  frename(survey_median_ppp = median)|>
  fmutate(reporting_level = as.character(reporting_level),
          pop_data_level = as.character(pop_data_level))

id_level <- dt_m |>
  fsubset(distribution_type %in% c("imputed"))
fselect(-c(distribution_type))|>
  _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp,
                                weight  = weight,
                                mean = NULL)),
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
  fmutate(reporting_level = as.character(reporting_level),
          pop_data_level = as.character(pop_data_level))

# 3. Micro and Imputed Data: National Estimation ----

md_id_national <- dt_m |>
  # fsubset(reporting_level == 'national' & area != "national") |>
  # this is equivalent to having pop_data_level > 2
  fsubset(reporting_level != 'national' & ppp_data_level != 'national') |> # here is the mistake!
  fselect(-c(distribution_type))|>
  roworder(cache_id, imputation_id, welfare_ppp)|>
  _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp, weight = weight)),
    by = .(cache_id, imputation_id)]|>
  fgroup_by(cache_id)|>
  collapg(fmean, cols = c("mean","median","gini",
                          "polarization","mld",
                          paste0("decile",1:10)))|>
  fungroup()|>
  frename(survey_median_ppp = median)|>
  fmutate(reporting_level = as.character("national"),
          cpi_data_level = as.character("national"),
          ppp_data_level = as.character("national"),
          pce_data_level = as.character("national"),
          pop_data_level = as.character("national"),
          gdp_data_level = as.character("national"))


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
  fmutate(reporting_level = as.character(reporting_level),
          pop_data_level = as.character(pop_data_level))


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
  roworder(cache_id, welfare)|>
  _[, as.list(wrp_md_dist_stats(welfare = welfare, weight = weight)),
    by = .(cache_id)]|>
  fgroup_by(cache_id)|>
  collapg(fmean, cols = c("mean","median","gini",
                          "polarization","mld",
                          paste0("decile",1:10)))|>
  fungroup()|>
  frename(survey_median_ppp = median)|>
  fmutate(reporting_level = as.character("national"),
          cpi_data_level = as.character("national"),
          ppp_data_level = as.character("national"),
          pce_data_level = as.character("national"),
          pop_data_level = as.character("national"),
          gdp_data_level = as.character("national"))




# Comparison ----

final <- rowbind(md_level, id_level, md_id_national, gd_ag_area, ag_national)

# Create quick comparison with dist_out_tar
compare_dist_sac <- final |> fselect(-c(cpi_data_level, ppp_data_level, gdp_data_level, pce_data_level))
setorder(compare_dist_sac, cache_id, reporting_level)
compare_dist_sac <- as.data.table(lapply(compare_dist_sac, function(x) { attributes(x) <- NULL; return(x) }))

compare_dist_tar <- dist_out_tar |> fselect(colnames(compare_dist_sac))
setorder(compare_dist_tar, cache_id, reporting_level)
compare_dist_tar <- as.data.table(lapply(compare_dist_tar, function(x) { attributes(x) <- NULL; return(x) }))

waldo::compare(compare_dist_tar, compare_dist_sac, tolerance = 1e-4) # All match
waldo::compare(compare_dist_tar |> fselect(-c(polarization, mld)), 
               compare_dist_sac |> fselect(-c(polarization, mld)), tolerance = 1e-7) # polarization and mld (only) do not match here










# # NoD2 Comparison ----
#   md_id_national_noD2 <-  md_id_national |>
#     fmutate(source = grepl("D2", cache_id)) |>
#     fsubset(source != TRUE)
#   
#   md_id_area_noD2 <- md_id_area |>
#     fmutate(source = grepl("D2", cache_id)) |>
#     fsubset(source != TRUE)
#   
#   gd_ag_area_noD2 <- gd_ag_area |>
#     fmutate(source = grepl("D2", cache_id)) |>
#     fsubset(source != TRUE)
#   
#   ag_national_noD2 <- ag_national |>
#     fmutate(source = grepl("D2", cache_id)) |>
#     fsubset(source != TRUE)
#   
#   dist_out_tar_noD2 <- dist_out_tar |>
#     fmutate(source = grepl("D2", cache_id)) |>
#     fsubset(source != TRUE)
#     
#   
#   final_noD2 <- rowbind(md_id_area_noD2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization), 
#                    md_id_national_noD2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization), 
#                    gd_ag_area_noD2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization), 
#                    ag_national_noD2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization))
# 
# 
# # Create quick comparison with dist_out_ta
# compare_dist_sac <- final_noD2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization)
# setorder(compare_dist_sac, cache_id, reporting_level)
# compare_dist_sac <- as.data.table(lapply(compare_dist_sac, function(x) { attributes(x) <- NULL; return(x) }))
# 
# compare_dist_tar <- dist_out_tar_noD2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization)
# setorder(compare_dist_tar, cache_id, reporting_level)
# compare_dist_tar <- as.data.table(lapply(compare_dist_tar, function(x) { attributes(x) <- NULL; return(x) }))
# waldo::compare(compare_dist_tar, compare_dist_sac, tolerance = 1e-7)
# # no differences
# 
# # Only D2 comparison ----
# D2_cache_ids <- setdiff(final$cache_id, final_noD2$cache_id)
# 
# final_D2 <- rowbind(md_id_area |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization) |> fsubset(cache_id %in% D2_cache_ids), 
#                     md_id_national |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization) |> fsubset(cache_id %in% D2_cache_ids), 
#                     gd_ag_area |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization) |> fsubset(cache_id %in% D2_cache_ids), 
#                     ag_national |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization) |> fsubset(cache_id %in% D2_cache_ids))
# 
# dist_out_tar_D2 <- dist_out_tar |> fsubset(cache_id %in% D2_cache_ids)
# 
# compare_dist_sac <- final_D2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization)
# setorder(compare_dist_sac, cache_id, reporting_level)
# compare_dist_sac <- as.data.table(lapply(compare_dist_sac, function(x) { attributes(x) <- NULL; return(x) }))
# 
# compare_dist_tar <- dist_out_tar_D2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization)
# setorder(compare_dist_tar, cache_id, reporting_level)
# compare_dist_tar <- as.data.table(lapply(compare_dist_tar, function(x) { attributes(x) <- NULL; return(x) }))
# waldo::compare(compare_dist_tar, compare_dist_sac, tolerance = 1e-7, max_diffs = Inf)
# 
# # Only NOT micro and D2 ----
# micro_D2_cache_ids <- cache_sac |> 
#   fselect(cache_id, distribution_type) |> 
#   fsubset(distribution_type == "micro") |>
#   fmutate(source = grepl("D2", cache_id)) |>
#   fsubset(source)
#   
# final_no_micro_D2 <- rowbind(md_id_area |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization) |> fsubset(!cache_id %in% micro_D2_cache_ids$cache_id), 
#                              md_id_national |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization) |> fsubset(!cache_id %in% micro_D2_cache_ids$cache_id), 
#                              gd_ag_area |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization) |> fsubset(!cache_id %in% micro_D2_cache_ids$cache_id), 
#                              ag_national |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization) |> fsubset(!cache_id %in% micro_D2_cache_ids$cache_id))
# 
# dist_out_tar_no_micro_D2 <- dist_out_tar |> fsubset(!cache_id %in% micro_D2_cache_ids$cache_id)
# 
# 
# compare_dist_sac <- final_no_micro_D2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization)
# setorder(compare_dist_sac, cache_id, reporting_level)
# compare_dist_sac <- as.data.table(lapply(compare_dist_sac, function(x) { attributes(x) <- NULL; return(x) }))
# 
# compare_dist_tar <- dist_out_tar_no_micro_D2 |> fselect(cache_id, reporting_level, decile1, decile5, decile10, mean, polarization)
# setorder(compare_dist_tar, cache_id, reporting_level)
# compare_dist_tar <- as.data.table(lapply(compare_dist_tar, function(x) { attributes(x) <- NULL; return(x) }))
# waldo::compare(compare_dist_tar, compare_dist_sac, tolerance , max_diffs = Inf)
