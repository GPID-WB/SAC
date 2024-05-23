# 1. Arguments ----
cache_tb
mean_table <- svy_mean_ppp_table_sac
pop_table <- dl_aux$pop
ppp_year <- py


# 2. Fill area with national when empty ----
cache_tb <- ftransform(cache_tb, area = ifelse(as.character(area) == "", # if empty
                                               "national", # it gets national
                                               as.character(area))) # else it keeps area

# 3. Micro and Imputed Level & Area Estimation  ----
md_id_area <- cache_tb |>
  fselect(cache_id, country_code, surveyid_year, distribution_type, reporting_level, imputation_id, 
          area, weight, welfare_ppp) |>
  fsubset(distribution_type %in% c("micro", "imputed")) |>
  collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp),
             on=c("cache_id", "reporting_level", "area"), # area needs to be added back here once we have it in the mean table
             validate = "m:1",
             how = "left",
             verbose = 0) |>
  roworder(cache_id, country_code, surveyid_year, distribution_type, 
           reporting_level, imputation_id, area, welfare_ppp) |>
  fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
            reporting_level, imputation_id, area)|> 
  fsummarise(res = list(wbpip:::md_compute_dist_stats(  
    welfare = welfare_ppp,
    weight = weight,
    mean = funique(survey_mean_ppp))),
    weight = fsum(weight))|>
  _[, c(.SD, .( # using _ because we are using native pipe 
    Statistic = names(unlist(res)), 
    Value = unlist(res))),
    by = .(cache_id, country_code, surveyid_year, distribution_type,
           reporting_level, imputation_id, area)] |>
  dcast(cache_id + country_code + surveyid_year + distribution_type + 
          reporting_level + imputation_id + area + weight ~ Statistic, 
        value.var = "Value") |>
  fgroup_by(cache_id, country_code, surveyid_year, reporting_level, area, weight)|>
  fsummarise(across(gini:quantiles9, fmean))

# 4. Micro and Imputed National Estimation ----
md_id_national <- md_id_area |>
  fsubset(reporting_level == "national" & area != "national") |>
  fgroup_by(cache_id, country_code, surveyid_year) |>
  fsummarise(across(gini:quantiles9, fmean, w = weight)) |>
  fmutate(area = factor("national"))
  
  # fselect(cache_id, country_code, surveyid_year, distribution_type, reporting_level, imputation_id, 
  #         area, weight, welfare_ppp) |>
  # fsubset(distribution_type %in% c("micro", "imputed") 
  #         & reporting_level == 'national' & area != "nati") |>
  # collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp) |>
  #                  fsubset(area == 'national'),
  #                on=c("cache_id", "reporting_level"), 
  #                validate = "m:1",
  #                how = "left",
  #                verbose = 0) |>
  # roworder(cache_id, country_code, surveyid_year, distribution_type, 
  #          reporting_level, imputation_id, welfare_ppp) |>
  # fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
  #           reporting_level, imputation_id)|> 
  # fsummarise(res = list(wbpip:::md_compute_dist_stats(  
  #   welfare = welfare_ppp,
  #   weight = weight,
  #   mean = funique(survey_mean_ppp))))|>
  # _[, c(.SD, .( # using _ because we are using native pipe 
  #   Statistic = names(unlist(res)), 
  #   Value = unlist(res))),
  #   by = .(cache_id, country_code, surveyid_year, distribution_type,
  #          reporting_level, imputation_id)] |>
  # pivot(ids = 1:6, how="w", values = "Value", names = "Statistic") |>
  # fgroup_by(cache_id, country_code, surveyid_year, reporting_level)|>
  # fsummarise(across(mean:quantiles9, fmean))|>
  # fmutate(area = "national")

# 5. Group and Aggregate Data Level Estimation -----
gd_ag_area <- cache_tb |>
  fselect(cache_id, country_code, surveyid_year, distribution_type, reporting_level, imputation_id, 
          area, welfare, weight, welfare) |>
  fsubset(distribution_type %in% c("group", "aggregate")) |>
  collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp),
                 on=c("cache_id", "reporting_level", "area"), # area needs to be added back here once we have it in the mean table
                 validate = "m:1",
                 how = "left",
                 verbose = 0) |>
  roworder(cache_id, country_code, surveyid_year, distribution_type, 
           reporting_level, area, welfare) |>
  fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
            reporting_level, area)|> # no area so dist stat are the sum of both
  fsummarise(res = list(wbpip:::gd_compute_dist_stats(  
    welfare = welfare,
    population = weight,
    mean = funique(survey_mean_ppp))))|>
  _[, c(.SD, .( # using _ because we are using native pipe 
    Statistic = names(unlist(res)), 
    Value = unlist(res))),
    by = .(cache_id, country_code, surveyid_year, distribution_type,
           reporting_level, area)] |>
  dcast(cache_id + country_code + surveyid_year + distribution_type + 
          reporting_level + area ~ Statistic, 
        value.var = "Value")



# 6. Aggregate data with urban/rural levels (both) (synth needed) ----
ag_national <- cache_tb |>
  fselect(cache_id, country_code, surveyid_year, distribution_type, reporting_level, 
          area, welfare, weight) |>
  fsubset(distribution_type %in% c("aggregate")) |>
  collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp, 
                                       reporting_pop),
                 on=c("cache_id", "reporting_level", "area"), 
                 validate = "m:1",
                 how = "left",
                 verbose = 0) |>
  # collapse::join(pop_table |> fselect(-pop_domain),
  #                on=c("country_code",
  #                     "surveyid_year" = "year",
  #                     "reporting_level" = "pop_data_level"),
  #                validate = "m:1",
  #                how = "left",
  #                verbose = 0) |>
  fgroup_by(cache_id, reporting_level, area)|>
  fsummarise(welfare =  wbpip:::sd_create_synth_vector(
    welfare = welfare,
    population = weight,
    mean = funique(survey_mean_ppp),
    pop = funique(reporting_pop)
  )$welfare) |>
  fmutate(weight = 1) |>
  collapse::join(mean_table |>
                   fsubset(area == "national") |>
                   fselect(cache_id, survey_mean_ppp),
                 on=c("cache_id"), 
                 validate = "m:1",
                 how = "left",
                 verbose = 0) |>
  roworder(cache_id, welfare) |>
  fgroup_by(cache_id) |>
  fsummarise(res = list(wbpip:::md_compute_dist_stats(  
    welfare = welfare,
    weight = weight,
    mean = funique(survey_mean_ppp))))|>
  _[, c(.SD, .( # using _ because we are using native pipe 
    Statistic = names(unlist(res)), 
    Value = unlist(res))),
    by = .(cache_id)] |>
  pivot(ids = 1, how="w", values = "Value", names = "Statistic")|>
  mutate(reporting_level = factor("national"), 
         area = factor("national"))


# 7. Quick check ----
md_id_area_clean <- md_id_area |>
  fungroup() |>
  fselect(cache_id, reporting_level, area, gini:quantiles9)
md_id_area_clean <- frename(md_id_area_clean, gsub("quantiles", "decile", 
                                                   names(md_id_area_clean)))

md_id_national_clean <- md_id_national |>
  fmutate(reporting_level = factor("national"))|>
  fselect(cache_id, reporting_level, area, gini:quantiles9)
md_id_national_clean <- frename(md_id_national_clean, gsub("quantiles", "decile", 
                                                           names(md_id_national_clean)))

gd_ag_area_clean <- gd_ag_area |>
  fselect(cache_id, reporting_level, area, deciles1:polarization)
gd_ag_area_clean <- frename(gd_ag_area_clean, gsub("deciles", "decile", 
                                                           names(gd_ag_area_clean)))

ag_national_clean <- ag_national |>
  fmutate(area = factor("national"), reporting_level = factor("national"))|>
  fselect(cache_id, reporting_level, area, mean:quantiles10)
ag_national_clean <- frename(ag_national_clean, gsub("quantiles", "decile", 
                                                   names(ag_national_clean)))



dist_to_compare_sac<- md_id_national_clean |>
  rbind(md_id_area_clean) |>
  rbind(gd_ag_area_clean) |>
  rbind(ag_national_clean)


dist_to_compare_tar <- dt_dist_stats_tar |>
  fselect(cache_id, reporting_level, decile1:polarization) |>
  roworder(cache_id, reporting_level)

dist_to_compare_sac_national <- dist_to_compare_sac |>
  fsubset(area == "national") |>
  fmutate(reporting_level = as.character(reporting_level))|>
  fselect(-area)|>
  roworder(cache_id, reporting_level)
  
  

waldo::compare(dist_to_compare_tar, 
               dist_to_compare_sac_national)













if ("micro" %in% distributions){
  # Micro data have national and level data estimations:
  if ("rural" %in% areas || "urban" %in% areas) {
    # 3. Micro and Imputed Data ----
    ## 3.1 Level Estimates ----
    md_id_dt_area<- 
      cache_tb |>
      fselect(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level, imputation_id, area, welfare, weight, welfare_ppp) |>
      fsubset(distribution_type %in% c("imputed", "micro")) |>
      joyn::joyn(mean_table |> fsubset(distribution_type %in% c("imputed", "micro")),
                 by=c("cache_id", "surveyid_year", "reporting_level", "area"), # by "area" needs to be added back here 
                 y_vars_to_keep = c("survey_mean_ppp"),
                 match_type = "m:1",
                 reportvar = FALSE,
                 keep = "left") |>
      # welfare needs to be ordered (but not by area)
      # imputation_id is only for imputed, but can be left in for micro, as there is only one value (empty)
      roworder(cache_id, country_code, surveyid_year, distribution_type, 
               reporting_level, imputation_id, area, welfare_ppp) |>
      fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
                reporting_level, imputation_id, area)|> 
      fsummarise(res = list(wbpip:::md_compute_dist_stats(  
        welfare = welfare_ppp,
        weight = weight,
        mean = funique(survey_mean_ppp))))|>
      _[, c(.SD, .( # using _ because we are using native pipe 
        Statistic = names(unlist(res)), 
        Value = unlist(res))),
        by = .(cache_id, country_code, surveyid_year, distribution_type,
               reporting_level, imputation_id, area)] |>
      dcast(cache_id + country_code + surveyid_year + distribution_type + 
              reporting_level + imputation_id + area ~ Statistic, 
            value.var = "Value") |>
      fgroup_by(cache_id, country_code, surveyid_year, area)|>
      fsummarise(across(gini:quantiles9, fmean))
  }
  
  ## 3.2 National Estimates ----
  md_id_dt_national<- 
    cache_tb |>
    fselect(cache_id, country_code, surveyid_year, distribution_type,
            reporting_level, imputation_id, area, welfare, weight, welfare_ppp) |>
    fsubset(distribution_type %in% c("imputed", "micro")) |>
    joyn::joyn(mean_table |> fsubset(distribution_type %in% c("imputed", "micro") 
                                     & area == "national"),
               by=c("cache_id", "surveyid_year", "reporting_level"),
               y_vars_to_keep = c("survey_mean_ppp"),
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left") |>
    # welfare needs to be ordered (but not by area)
    roworder(cache_id, country_code, surveyid_year, distribution_type, 
             reporting_level, imputation_id, welfare_ppp) |>
    fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level, imputation_id)|> # no area so dist stat are the sum of both
    fsummarise(res = list(wbpip:::md_compute_dist_stats(  
      welfare = welfare_ppp,
      weight = weight,
      mean = funique(survey_mean_ppp))))|>
    _[, c(.SD, .( # using _ because we are using native pipe 
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, country_code, surveyid_year, distribution_type,
             reporting_level, imputation_id)] |>
    dcast(cache_id + country_code + surveyid_year + distribution_type + 
            reporting_level + imputation_id ~ Statistic, 
          value.var = "Value") |>
    fgroup_by(cache_id, country_code, surveyid_year)|>
    fsummarise(across(gini:quantiles9, fmean))
  
}


if ("group" %in% distributions){
  
  gd_dt_national<- 
    cache_tb |>
    fselect(cache_id, country_code, surveyid_year, distribution_type,
            reporting_level, area, welfare, weight, welfare_ppp) |>
    fsubset(distribution_type == 'group') |>
    joyn::joyn(mean_table |> fsubset(distribution_type == 'group' 
                                     & area == "national"),
               by=c("cache_id", "surveyid_year", "reporting_level"), # by "area" needs to be added back here 
               y_vars_to_keep = c("survey_mean_ppp"),
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left") |>
    # welfare needs to be ordered (but not by area)
    roworder(cache_id, country_code, surveyid_year, distribution_type, 
             reporting_level, area, welfare_ppp) |>
    fgroup_by(cache_id, country_code, surveyid_year, distribution_type,
              reporting_level, area)|> # no area so dist stat are the sum of both
    fsummarise(res = list(wbpip:::gd_compute_dist_stats(  
      welfare = welfare,
      population = weight,
      mean = funique(survey_mean_ppp))))|>
    _[, c(.SD, .( # using _ because we are using native pipe 
      Statistic = names(unlist(res)), 
      Value = unlist(res))),
      by = .(cache_id, country_code, surveyid_year, distribution_type,
             reporting_level, area)] |>
    dcast(cache_id + country_code + surveyid_year + distribution_type + 
            reporting_level + area ~ Statistic, 
          value.var = "Value")

  if ("aggregate" %in% distributions){
    # Aggregate data have national, and level data estimations. 
    # National levels are calculated with the synthetic distribution
    # Level data are calculated with th
    
    
    
  }  
}