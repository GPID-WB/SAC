# 1. Arguments ----
cache_tb # This is created from your main code
mean_table <- svy_mean_ppp_table_sac # This as well

# 2. Fill area with national when empty ----
cache_tb <- ftransform(cache_tb, area = ifelse(as.character(area) == "", # if empty
                                               "national", # it gets national
                                               as.character(area))) # else it keeps area

# 3. Micro and Imputed Level & Area Estimation  ----
md_id_area <- cache_tb |>
  fselect(cache_id, distribution_type, reporting_level, imputation_id, 
          area, weight, welfare_ppp) |>
  fsubset(distribution_type %in% c("micro", "imputed")) |>
  # collapse::join(mean_table |> 
  #                  fselect(cache_id, reporting_level, area, survey_mean_ppp),
  #            on=c("cache_id", "reporting_level", "area"), 
  #            validate = "m:1",
  #            how = "left",
  #            verbose = 0) |>
  roworder(cache_id, imputation_id, reporting_level, area, welfare_ppp) |>
  fgroup_by(cache_id, imputation_id, reporting_level, area)|> 
  fsummarise(res = list(wbpip:::md_compute_dist_stats(  
    welfare = welfare_ppp,
    weight = weight
    #mean = funique(survey_mean_ppp)
    )),
    weight = fsum(weight))|>
  _[, c(.SD, .( # using _ because we are using native pipe 
    Statistic = names(unlist(res)), 
    Value = unlist(res))),
    by = .(cache_id, imputation_id, reporting_level, area, weight)] |>
  fselect(-res)|>
  pivot(ids = 1:5, how="w", values = "Value", names = "Statistic") |>
  fgroup_by(cache_id, reporting_level, area)|>
  fsummarise(across(weight:quantiles10, fmean))|> # weight mean as it should be flattened and it removes it from keys
  fungroup()|>
  frename(survey_median_ppp = median)|>
  fmutate(reporting_level = as.character(reporting_level))

setrename(md_id_area, gsub("quantiles", "decile", names(md_id_area)))

# 4. Micro and Imputed National Estimation ----
## Note: Directly from md_id_area, we can take the mean using the weight.
md_id_national_weighted <- md_id_area |>
  # For those cache_id with urban and rural, we can calculate national, so we filter the national ones out:
  fsubset(reporting_level == "national" & area != "national") |> 
  fgroup_by(cache_id) |>
  fsummarise(across(mean:decile10, fmean, w = weight)) |>
  # Then we give it back the value national:
  fungroup()|>
  fmutate(reporting_level = as.character("national"), 
          area = as.character("national"))

md_id_national_complete <- 
  cache_tb |>
  fselect(cache_id, distribution_type, reporting_level, imputation_id, 
          area, weight, welfare_ppp) |>
  fsubset(distribution_type %in% c("micro", "imputed") 
           & reporting_level == 'national' & area != "national") |>
  # collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp) |>
  #                 fsubset(area == 'national'),
  #                 on=c("cache_id", "reporting_level"), 
  #                 validate = "m:1",
  #                 how = "left",
  #                 verbose = 0) |>
  roworder(cache_id, imputation_id, welfare_ppp) |>
  fgroup_by(cache_id, imputation_id)|> 
  fsummarise(res = list(wbpip:::md_compute_dist_stats(  
    welfare = welfare_ppp,
    weight = weight
    #mean = funique(survey_mean_ppp)
    )))|>
  _[, c(.SD, .( # using _ because we are using native pipe 
    Statistic = names(unlist(res)), 
    Value = unlist(res))),
    by = .(cache_id, imputation_id)] |>
  fselect(-res)|>
  pivot(ids = 1:2, how="w", values = "Value", names = "Statistic") |>
  fgroup_by(cache_id)|>
  fsummarise(across(mean:quantiles10, fmean))|> # weight mean as it should be flattened and it removes it from keys
  fungroup()|>
  frename(survey_median_ppp = median) |>
  fmutate(reporting_level = as.character("national"), 
          area = as.character("national"))

setrename(md_id_national_complete, gsub("quantiles", "decile", names(md_id_national_complete)))

# 5. Group and Aggregate Data Level Estimation -----
gd_ag_area <- cache_tb |>
  fselect(cache_id, distribution_type, reporting_level, imputation_id, 
          area, welfare, weight) |>
  fsubset(distribution_type %in% c("group", "aggregate")) |>
  collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp),
                 on=c("cache_id", "reporting_level", "area"), 
                 validate = "m:1",
                 how = "left",
                 verbose = 0) |>
  roworder(cache_id, reporting_level, area, welfare) |>
  fgroup_by(cache_id, reporting_level, area)|>
  fsummarise(res = list(wbpip:::gd_compute_dist_stats(  
    welfare = welfare,
    population = weight,
    mean = funique(survey_mean_ppp))))|>
  _[, c(.SD, .( # using _ because we are using native pipe 
    Statistic = names(unlist(res)), 
    Value = unlist(res))),
    by = .(cache_id, reporting_level, area)] |>
  fselect(-res)|>
  pivot(ids = 1:3, how="w", values = "Value", names = "Statistic")|>
  frename(survey_median_ppp = median)|>
  fmutate(reporting_level = as.character(reporting_level))

setrename(gd_ag_area, gsub("deciles", "decile", names(gd_ag_area)))

# 6. Aggregate data with urban/rural levels (both) (synth needed) ----
ag_national <- cache_tb |>
  fselect(cache_id, distribution_type, reporting_level, area, welfare, welfare_ppp, weight) |>
  fsubset(distribution_type %in% c("aggregate")) |>
  collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp, 
                                       reporting_pop), 
                 # using reporting_pop as it is the same as the one in the pop_table
                 on=c("cache_id", "reporting_level", "area"), 
                 validate = "m:1",
                 how = "left",
                 verbose = 0) |>
  roworder(cache_id, reporting_level, area, welfare) |>
  fgroup_by(cache_id, reporting_level, area)|>
  fsummarise(welfare =  wbpip:::sd_create_synth_vector(
    welfare = welfare,
    population = weight,
    mean = funique(survey_mean_ppp),
    pop = funique(reporting_pop)
  )$welfare,
  weight = funique(reporting_pop)/100000) |> 
  roworder(cache_id, welfare) |>
  fgroup_by(cache_id) |>
  fsummarise(res = list(wbpip:::md_compute_dist_stats(  
    welfare = welfare,
    weight = weight)))|>
  _[, c(.SD, .( 
    Statistic = names(unlist(res)), 
    Value = unlist(res))),
    by = .(cache_id)] |>
  fselect(-res)|>
  pivot(ids = 1, how="w", values = "Value", names = "Statistic")|>
  mutate(reporting_level = as.character("national"), 
         area = as.character("national")) |>
  frename(survey_median_ppp = median)
  
setrename(ag_national, gsub("quantiles", "decile", names(ag_national)))


# Create final with complete and weighted version of md_id_national:
final <- rbindlist(list(md_id_area |> fselect(-weight), md_id_national_complete, 
                                 gd_ag_area, ag_national), use.names = TRUE)

final_weighted <- rbindlist(list(md_id_area |> fselect(-weight), md_id_national_weighted, 
                        gd_ag_area, ag_national), use.names = TRUE)


# 7. (NO NEED TO RUN) Quick check of values ----
# Note: !!!! NO NEED TO RUN ALL OF THESE, MOVE TO DIST_TABLE_STEPS, which is cleaner.
# Version with the "complete' procedure for the md_id_national:
dist_to_compare_sac<- final |>
  fsubset(reporting_level == area) |>
  fselect(cache_id, reporting_level, survey_median_ppp, decile1:decile10, 
          mean, gini, mld, polarization) |>
  roworder(cache_id, reporting_level)

# Version with the "weighted' procedure where we weight the rural/urban to get the national:
dist_to_compare_sac_weighted<- final_weighted |>
  fsubset(reporting_level == area) |>
  fselect(cache_id, reporting_level, survey_median_ppp, decile1:decile10, 
          mean, gini, mld, polarization) |>
  roworder(cache_id, reporting_level)

# TAR version
dist_to_compare_tar <- dt_dist_stats_tar |>
  fselect(cache_id, reporting_level, survey_median_ppp, decile1:decile10, 
          mean, gini, mld, polarization) |>
  roworder(cache_id, reporting_level)

# Differences between SAC weighted and SAC complete:
waldo::compare(dist_to_compare_sac_weighted, 
               dist_to_compare_sac, max_diffs = Inf, tolerance = 1e-7)

# Differences between SAC complete and TAR:
## at 1e-7 many differences:
waldo::compare(dist_to_compare_tar, 
               dist_to_compare_sac, max_diffs = Inf, tolerance = 1e-7)

## at 1e-3 still some differences (lines 34 and 37):
waldo::compare(dist_to_compare_tar, 
               dist_to_compare_sac, max_diffs = Inf, tolerance = 1e-3)


# 8. (NO NEED TO RUN) Comparison one by one -----
# Note: !!!! NO NEED TO RUN ALL OF THESE, MOVE TO DIST_TABLE_STEPS, which is cleaner.
# md_id_national example:
waldo::compare(dist_to_compare_sac |>
  fsubset(cache_id == "NGA_1996_NCS_D1_CON_HIST"),
dist_to_compare_tar |>
  fsubset(cache_id == "NGA_1996_NCS_D1_CON_HIST"), max_diffs = Inf, tolerance = 1e-7) # no diff

waldo::compare(dist_to_compare_sac |>
                 fsubset(cache_id == "BOL_1997_ENE_D1_INC_GPWG"),
               dist_to_compare_tar |>
                 fsubset(cache_id == "BOL_1997_ENE_D1_INC_GPWG"), max_diffs = Inf, tolerance = 1e-7) # no diff

# gd_ag_area:
# group
waldo::compare(dist_to_compare_sac |>
                 fsubset(cache_id == "NGA_1985_NCS_D1_CON_GROUP"),
               dist_to_compare_tar |>
                 fsubset(cache_id == "NGA_1985_NCS_D1_CON_GROUP"), max_diffs = Inf, tolerance = 1e-7) # no diff

# aggregate
waldo::compare(dist_to_compare_sac_weighted |>
                 fsubset(cache_id == "CHN_1981_CRHS-CUHS_D2_INC_GROUP") |> fsubset(reporting_level == "national"),
               dist_to_compare_tar |>
                 fsubset(cache_id == "CHN_1981_CRHS-CUHS_D2_INC_GROUP") |> fsubset(reporting_level == "national"), 
               max_diffs = Inf, tolerance = 1e-7)

waldo::compare(dist_to_compare_sac |>
                 fsubset(cache_id == "CHN_2020_CNIHS_D2_CON_GROUP") |> fsubset(reporting_level == "national"),
               dist_to_compare_tar |>
                 fsubset(cache_id == "CHN_2020_CNIHS_D2_CON_GROUP") |> fsubset(reporting_level == "national"), 
               max_diffs = Inf, tolerance = 1e-7)

# Problematic cases (34 and 37):
# 34: CHN_1990_CRHS-CUHS_D2_CON_GROUP
waldo::compare(dist_to_compare_tar |>
                 fsubset(cache_id == "CHN_1990_CRHS-CUHS_D2_CON_GROUP") |> fsubset(reporting_level == "national"),
               dist_to_compare_sac |>
                 fsubset(cache_id == "CHN_1990_CRHS-CUHS_D2_CON_GROUP") |> fsubset(reporting_level == "national"), 
               max_diffs = Inf, tolerance = 1e-7)

# 37: CHN_1993_CRHS-CUHS_D2_CON_GROUP
waldo::compare(dist_to_compare_tar |>
                 fsubset(cache_id == "CHN_1993_CRHS-CUHS_D2_CON_GROUP") |> fsubset(reporting_level == "national"),
               dist_to_compare_sac |>
                 fsubset(cache_id == "CHN_1993_CRHS-CUHS_D2_CON_GROUP") |> fsubset(reporting_level == "national"), 
               max_diffs = Inf, tolerance = 1e-7)

# Maybe it comes from a different mean?
dist_to_compare_sac |> head()
dist_to_compare_sac_weighted |> head()

compare(dist_to_compare_sac[3], 
        dist_to_compare_sac_weighted[3], max_diffs = Inf)



