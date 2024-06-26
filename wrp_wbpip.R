# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project:       Wrappers for wbpip functions
# Author:        Diana C. Garcia Rojas
# Dependencies:  The World Bank
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Creation Date:    June 2024
# References:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1. wrp_md_dist_stats -----
##
## Objective: Wrapper for wbpip function `wbpip::md_compute_dist_stats`  
## (Diana comment: If this works it could be implemented in wbpip)

wrp_md_dist_stats <- function(welfare,
                              weight,
                              mean       = NULL,
                              nbins      = 10,
                              lorenz     = NULL,
                              n_quantile = 10) {
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # computations   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  if (is.null(mean)) {
    mean <- fmean(x = welfare,
                  w = weight)
  }
  
  if (is.null(lorenz)) {
    lorenz <- wbpip:::md_compute_lorenz(welfare = welfare,
                                        weight  = weight,
                                        nbins   = nbins)
  }
  
  share_quant <- wbpip:::md_compute_quantiles_share(welfare    = welfare,
                                                    weight     = weight,
                                                    n_quantile = n_quantile,
                                                    lorenz     = lorenz)
  
  names(share_quant) <- paste0("decile",1:n_quantile)
  
  median <- wbpip:::md_compute_median(welfare = welfare,
                                      weight  = weight,
                                      lorenz  = lorenz)
  
  gini <- wbpip:::md_compute_gini(welfare = welfare,
                                  weight  = weight)
  
  mld <- wbpip:::md_compute_mld(welfare = welfare,
                                weight  = weight,
                                mean    = mean)
  
  polarization <- wbpip:::md_compute_polarization(welfare = welfare,
                                                  weight  = weight,
                                                  gini    = gini,
                                                  mean    = mean,
                                                  median  = median)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Return   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  return(c(list(
    mean         = mean,
    median       = median,
    gini         = gini,
    polarization = polarization,
    mld          = mld),
    share_quant))
  
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2. wrp_gd_dist_stats -----
##
## Objective: Wrapper for wbpip function `wbpip::gd_compute_dist_stats`  
## (Diana comment: If this works it could be implemented in wbpip)

wrp_gd_dist_stats <- function(welfare,
                              population,
                              mean,
                              p0 = 0.5) {
  
  
  # Apply Lorenz quadratic fit ----------------------------------------------
  
  # STEP 1: Prep data to fit functional form
  prepped_data <- wbpip::create_functional_form_lq(
    welfare = welfare, population = population
  )
  
  # STEP 2: Estimate regression coefficients using LQ parameterization
  reg_results_lq <- wbpip::regres(prepped_data, is_lq = TRUE)
  A <- reg_results_lq$coef[1]
  B <- reg_results_lq$coef[2]
  C <- reg_results_lq$coef[3]
  kv <- wbpip:::gd_lq_key_values(A, B, C)
  
  # STEP 3: Compute Sum of Squared Error
  reg_results_lq[["sse"]] <- wbpip:::gd_compute_dist_fit_lq(welfare = welfare,
                                                    population = population,
                                                    A = A,
                                                    B = B,
                                                    C = C,
                                                    key_values = kv)
  
  # STEP 3: Calculate distributional stats
  # Compute key numbers from Lorenz quadratic form
  
  
  results_lq <- wbpip:::gd_estimate_dist_stats_lq(mean = mean,
                                          p0 = p0,
                                          A = A,
                                          B = B,
                                          C = C,
                                          key_values = kv)
  
  results_lq <- append(results_lq, reg_results_lq)
  
  # Apply Lorenz beta fit ---------------------------------------------------
  
  # STEP 1: Prep data to fit functional form
  prepped_data <- wbpip::create_functional_form_lb(
    welfare = welfare, population = population
  )
  
  # STEP 2: Estimate regression coefficients using LB parameterization
  reg_results_lb <- wbpip::regres(prepped_data, is_lq = FALSE)
  A <- reg_results_lb$coef[1]
  B <- reg_results_lb$coef[2]
  C <- reg_results_lb$coef[3]
  
  # STEP 3: Compute Sum of Squared Error
  reg_results_lb[["sse"]] <- wbpip:::gd_compute_dist_fit_lb(welfare = welfare,
                                                    population = population,
                                                    A = A,
                                                    B = B,
                                                    C = C)
  
  # STEP 3: Calculate distributional stats
  results_lb <- wbpip:::gd_estimate_dist_stats_lb(mean = mean,
                                          p0 = p0,
                                          A = A,
                                          B = B,
                                          C = C)
  
  results_lb <- append(results_lb, reg_results_lb)
  
  # Apply selection rules -----------------------------------------------
  
  # STEP 4: Select best fit
  out <- wbpip:::gd_select_lorenz_dist(
    lq = results_lq, lb = results_lb
  )
  
  # Deciles change -------
  
  # deciles <- unlist2d(out["deciles"], idcols = FALSE)
  # names(deciles) <- paste0("decile",1:10)
  # 
  # deciles <- unlist(out["deciles"])
  
  # Return only subset of variables
  out <- c(out[c(
    "mean",
    "median",
    "gini",
    "mld",
    "polarization"
  )], unlist(out["deciles"]))
  
  return(out)
}


## 2.1 safe wrp_gd_dist_stats ----
# Safe GD estimation
safe_wrp_gd_dist_stats <- function(welfare, population, mean, cache_id) {
  tryCatch(
    
    # Run function:
    expr = {
      
      res <- wrp_gd_dist_stats(welfare = welfare, 
                               population = population, 
                               mean = mean)
      
      # Return if it works
      return(res)
    },
    
    # If error:
    error = function(e) {
      rlang::warn("Distributional statistics caluclation failed. Returning NULL.")
      
      return(NULL)
    }
  )
}



