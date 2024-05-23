# Set-up
# filter for testing --------
cache_inventory <- pipload::pip_load_cache_inventory(version = '20240326_2017_01_02_PROD')
cache_inventory <- cache_inventory[grep("CHN|NGA|BOL", cache_id), ]
cache <- pipload::pip_load_cache(c("NGA", "BOL", "CHN"), type="list", version = '20240326_2017_01_02_PROD') 
cache_tb <- pipload::pip_load_cache(c("NGA", "BOL", "CHN"), version = '20240326_2017_01_02_PROD') 
cache_ids <- get_cache_id(cache_inventory) 


# 1. Arguments ----
cache_tb
mean_table <- svy_mean_ppp_table_sac
pop_table <- dl_aux$pop
ppp_year <- py

# 2. Check which distribution types/areas (levels)/grep in the set (add later).
distributions <- funique(cache_tb$distribution_type)
areas <- funique(cache_tb$area)
d2 <- grepl("D2", funique(cache_tb$cache_id))

# 3. Fill area -----
# Fill area with national when empty and reporting_level for aggregate distribution
cache_tb <- ftransform(cache_tb, area = ifelse(as.character(area) == "", # if empty
                                               "national", # it gets national
                                               as.character(area))) # else it keeps area




# 4. Level and Area estimation ----
# Level and area estimation happens at:
## The urban and rural level for micro data, aggregate data, and for some exceptions of group data.
## The national level for imputed data and group data.
## The join needs to take pop_data_level at the area level, and keep it disaggregated.
## These are the cases where reporting_level == area.

# Note: remember that there are two levels for gender.
area_estimation <- cache_tb |>
  fsubset(reporting_level == area) |>
  fselect(cache_id, country_code, surveyid_year, distribution_type, reporting_level, imputation_id, 
          area, welfare, weight, welfare_ppp) |>
  collapse::join(mean_table |> fselect(cache_id, reporting_level, area, survey_mean_ppp),
             on=c("cache_id", "reporting_level"), # area needs to be added back here once we have it in the mean table
             validate = "m:1",
             how = "left") |>
  collapse::join(pop_table,
             on=c("country_code", 
                  "surveyid_year" = "year",
                  "reporting_level" = "pop_data_level"), # area needs to be added back here once we have it in the mean table
             validate = "m:1",
             how = "left")|>
  

  
  




# 5. National estimation ----
# National estimation happens:
## Aggregate data: synth + md_estimation.
## Micro data: md_estimation.
## In both cases, the join still needs to take pop_data_level at the area level,
## but then it needs to be combined.
## These are the cases where reporting_level != area level OR distribution_type == "aggregate".















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