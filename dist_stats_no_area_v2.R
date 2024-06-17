# cache_sac used instead of cache, make sure you have cache_sac created first
# This file creates "final" which is comparable to the TAR version.
mean_table <- means_out_sac

# 1. Select variables and subset for Micro data----

dt_m <- cache_sac |>
  fselect(cache_id, distribution_type, cpi_data_level, ppp_data_level,
          gdp_data_level, pce_data_level,
          pop_data_level, reporting_level, 
          imputation_id, weight, welfare_ppp) |>
  fsubset(distribution_type %in% c("micro", "imputed")) # 2 minutes

# 2. Micro and Imputed Data: Level & Area Estimation  ----
# GC Note: Now this is not Level & Area estimation, it is just level estimation.

md_level <- dt_m |>
  fsubset(distribution_type %in% c("micro")) |>
  fselect(-c(distribution_type))|>
  roworder(cache_id, pop_data_level, welfare_ppp) |>
  collapse::join(mean_table |> 
                   fselect(cache_id, cpi_data_level, ppp_data_level,
                           gdp_data_level, pce_data_level,
                           pop_data_level, reporting_level, 
                           survey_mean_ppp, reporting_pop),
                 on=c("cache_id", "cpi_data_level", "ppp_data_level",
                      "gdp_data_level", "pce_data_level",
                      "pop_data_level", "reporting_level"), 
                 # GC Note: it is actually over-identified at this stage. 
                 # Maybe we can exlpore whether this is really needed?
                 validate = "m:1",
                 verbose = 0,
                 column = list(".joyn", c("x", "y", "x & y")))|>
  _[, as.list(wrp_md_dist_stats(welfare = welfare_ppp,
                                weight  = weight,
                                mean = funique(survey_mean_ppp))),
    by = .(cache_id, cpi_data_level, ppp_data_level,
           gdp_data_level, pce_data_level,
           pop_data_level, reporting_level)]|>
  fungroup()|>
  frename(survey_median_ppp = median)|>
  fmutate(reporting_level = as.character(reporting_level),
          pop_data_level = as.character(pop_data_level)) |>
  fselect(-c(cpi_data_level, ppp_data_level,
             gdp_data_level, pce_data_level)) # 3 minutes

id_level <- dt_m |>
  fsubset(distribution_type %in% c("imputed")) |>
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
          pop_data_level = as.character(pop_data_level))|>
  fselect(-c(cpi_data_level, ppp_data_level,
             gdp_data_level, pce_data_level)) # < 1 minute


# 3. Micro and Imputed Data: National Estimation ----

md_id_national <- dt_m |>
  # Gc Note: this is equivalent to having pop_data_level > 1 and D2 in cache_id:
  fsubset(reporting_level != 'national' & ppp_data_level != 'national') |>
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
          pop_data_level = as.character("national")) # < 1 minute


# Select variables, subset and join mean table
dt_jn <- cache_sac |>
  fselect(cache_id, distribution_type, imputation_id, cpi_data_level, ppp_data_level,
          gdp_data_level, pce_data_level,
          pop_data_level, reporting_level, weight, welfare) |>
  fsubset(distribution_type %in% c("group", "aggregate"))|>
  collapse::join(mean_table |> 
                   fselect(cache_id, cpi_data_level, ppp_data_level,
                           gdp_data_level, pce_data_level,
                           pop_data_level, reporting_level, 
                           survey_mean_ppp, reporting_pop),
                 on=c("cache_id", "cpi_data_level", "ppp_data_level",
                      "gdp_data_level", "pce_data_level",
                      "pop_data_level", "reporting_level"), 
                 # GC Note: it is actually over-identified at this stage as well.
                 validate = "m:1",
                 verbose = 0,
                 column = list(".joyn", c("x", "y", "x & y"))) # immediate


# MISSING WARNING MESSAGE

dt_jn <- dt_jn|>
  fsubset(.joyn != "y")|>
  fselect(-.joyn)


# 4. Group and Aggregate Data: Level and Area Estimation -----


# this gives an error with wbpip, so we catch them as in TAR.
gd_ag_level <- dt_jn |>
  fselect(-c(distribution_type, reporting_pop)) |>
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
          pop_data_level = as.character(pop_data_level))|>
  fselect(-c(cpi_data_level, ppp_data_level,
             gdp_data_level, pce_data_level)) # immediate


setrename(gd_ag_area, gsub("deciles", "decile", names(gd_ag_area)))

# 5. Aggregate Data: National estimation (synth needed) ----

ag_syn <- dt_jn |>
  fsubset(distribution_type %in% c("aggregate")) |>
  fselect(-c(distribution_type))|>
  roworder(cache_id, cpi_data_level, ppp_data_level,
           gdp_data_level, pce_data_level,
           pop_data_level, reporting_level, welfare) |>
  fgroup_by(cache_id, cpi_data_level, ppp_data_level,
            gdp_data_level, pce_data_level,
            pop_data_level, reporting_level)|>
  fsummarise(welfare =  wbpip:::sd_create_synth_vector(
    welfare = welfare,
    population = weight,
    mean = funique(survey_mean_ppp),
    pop = funique(reporting_pop))$welfare,
    weight = funique(reporting_pop)/100000) # immediate

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
          pop_data_level = as.character("national")) # immediate

# Merge ----

final <- rowbind(md_level, id_level, md_id_national, gd_ag_level, ag_national)


# Clean-up ----

dt_clean <- final |>
  collapse::join(mean_table|>
                   fselect("survey_id", "cache_id", "wb_region_code", "pcn_region_code",
                           "country_code", "surveyid_year", "survey_year",
                           "reporting_year", "survey_acronym", "welfare_type",
                           "cpi", "ppp", "pop_data_level", "reporting_level"),
                 on=c("cache_id", "reporting_level", "pop_data_level"), 
                 validate = "1:1",
                 how = "left",
                 verbose = 0,
                 overid = 2)|>
  fmutate(survey_median_lcu = survey_median_ppp*ppp*cpi,
          survey_id = toupper(survey_id))|>
  fselect(-ppp, -cpi)|>
  colorder(survey_id, cache_id, wb_region_code, pcn_region_code, country_code,
           survey_acronym, surveyid_year, survey_year, reporting_year, welfare_type,
           reporting_level, survey_median_lcu, survey_median_ppp, decile1:decile10,
           mean, gini, polarization, mld, pop_data_level)


# Comparison ----

# Create quick comparison with dist_out_tar
compare_dist_sac <- dt_clean
setorder(compare_dist_sac, cache_id, reporting_level)
compare_dist_sac <- as.data.table(lapply(compare_dist_sac, function(x) { attributes(x) <- NULL; return(x) }))

compare_dist_tar <- dist_out_tar
setorder(compare_dist_tar, cache_id, reporting_level)
compare_dist_tar <- as.data.table(lapply(compare_dist_tar, function(x) { attributes(x) <- NULL; return(x) }))

waldo::compare(compare_dist_tar, compare_dist_sac, tolerance = 1e-7) # All match






