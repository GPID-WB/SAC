# 1. Arguments
dt <- final
dsm <- svy_mean_ppp_table_sac



# 2. Addition to the table
dt |>
  collapse::join(dsm|>
                   fselect("survey_id", "cache_id", "wb_region_code", "pcn_region_code",
                           "country_code", "surveyid_year", "survey_year",
                           "reporting_year", "survey_acronym", "welfare_type",
                           "cpi", "ppp", "pop_data_level", "reporting_level", "area"),
                 on=c("cache_id", "reporting_level", "area"), 
                 validate = "1:1",
                 how = "left",
                 verbose = 0)|>
  fmutate(survey_median_lcu = survey_median_ppp*ppp*cpi)|>
  fselect(-ppp, -cpi)|>
  colorder(survey_id, cache_id, wb_region_code, pcn_region_code, country_code,
           survey_acronym, surveyid_year, survey_year, reporting_year, welfare_type,
           reporting_level, area, survey_median_lcu, survey_median_ppp, decile1:decile10,
           mean, gini, polarization, mld, pop_data_level)
