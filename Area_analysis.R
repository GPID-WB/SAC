#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Analysis on area variable in cache   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Nobs_rl_a <- cache_sac[["micro_imputed"]]|>
  fcount(reporting_level,area)

Syv_rl_a <- cache_sac[["micro_imputed"]]|>
  fcount(cache_id, reporting_level,area)

A_table <- Syv_rl_a|>
  rename(Nobs_rl_a = N)|>
  fgroup_by(cache_id, reporting_level)|>
  fmutate(Nobs_rl=fsum(Nobs_rl_a))|>
  fungroup()|>
  fgroup_by(cache_id)|>
  fmutate(Nobs = fsum(Nobs_rl_a),
          Perc_a = round(Nobs_rl_a/Nobs,2),
          Perc_rl = round(Nobs_rl/Nobs,2))

