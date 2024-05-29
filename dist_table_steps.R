# 1. Arguments ----
dt <- final # load the "final" from dist_stats_steps_v2.R
dsm <- svy_mean_ppp_table_sac # this is created from your code


# 2. Addition to the table ----
dt_dist_stats_sac <- dt |>
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
           mean, gini, mld, polarization, pop_data_level)

# 3. Checks ----
to_compare <- dt_dist_stats_sac |>
  fsubset(area == reporting_level)|>
  fselect(-area) |>
  roworder(cache_id, reporting_level)

to_compare <- as.data.table(lapply(to_compare, function(x) { attributes(x) <- NULL; return(x) }))

waldo::compare(dt_dist_stats_tar, 
               to_compare,
               tolerance = 1e-7,
               max_diffs = Inf) # many diff at this level

waldo::compare(dt_dist_stats_tar, 
               to_compare,
               tolerance = 1e-3,
               max_diffs = Inf) # still some weird diff (characters are actually the same)

# In fact, if you check the single lines for the 'character' differences, they are okay:
compare(to_compare[76],
dt_dist_stats_tar[76], tolerance = 1e-7)

# The problem remains with the two China cases:
compare(to_compare[34],
        dt_dist_stats_tar[34], tolerance = 1e-7)

compare(to_compare[37],
        dt_dist_stats_tar[37], tolerance = 1e-7)
