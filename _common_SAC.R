
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Load globals   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

gls <- pipfun::pip_create_globals(
  root_dir   = Sys.getenv("PIP_ROOT_DIR"), 
  out_dir    = config$out_dir,
  vintage    = list(release = release, 
                    ppp_year = py, 
                    identity = identity), 
  create_dir = TRUE, 
  max_year_country   = max_year_country, 
  max_year_aggregate = max_year_aggregate
)


# to delete and modify in pipfun code
gls$FST_COMP_LVL <- 20

cli::cli_text("Vintage directory {.file {gls$vintage_dir}}")

# pipload::add_gls_to_env(vintage = "20220408")

# pipload::add_gls_to_env(vintage = "new",
#                         out_dir = fs::path("y:/pip_ingestion_pipeline/temp/"))
# 

# Set targets options 
  # controller <- crew::crew_controller_local(
  #   name = "pipeline_controller",
  #   workers = 10,
  #   seconds_idle = 3
  # )


tar_option_set(
  garbage_collection = TRUE,
  memory = 'transient',
  format = 'qs', #'fst_dt',
  workspace_on_error = TRUE, 
  error = "stop"  # or "null"
  # controller = controller
  # , controller = crew::crew_controller_local(workers = 2)
)


# make sure joyn does not display messages
options(joyn.verbose    = FALSE, 
        pipload.verbose = FALSE, 
        joyn.reportvar  = ".joyn"
        ) 



# Set future plan (for targets::tar_make_future)
# plan(multisession)

# ---- Step 1: Prepare data ----

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##  AUX data -----
aux_tb <- prep_aux_data(maindir = gls$PIP_DATA_DIR)
# filter 
aux_tb <- aux_tb[!(auxname %chin% c("maddison"))]

aux_ver <- rep("00", length(aux_tb$auxname))

# aux_ver[which(aux_tb$auxname == "cpi")] <- -1 # remove for march update

dl_aux <- purrr::map2(.x = aux_tb$auxname,
                      .y =  aux_ver,
                      .f = ~ {
                        pipload::pip_load_aux(
                          measure     = .x, 
                          apply_label = FALSE,
                          maindir     = gls$PIP_DATA_DIR, 
                          verbose     = FALSE, 
                          version     = .y, 
                          branch      = branch)
                      }
)

names(dl_aux) <- aux_tb$auxname    

aux_versions <- purrr::map_df(aux_tb$auxname, ~{
  y <- attr(dl_aux[[.x]], "version")
  w <- data.table(aux = .x, 
                  version = y)
  w
})


# temporal change. 
dl_aux$pop$year <- as.numeric(dl_aux$pop$year)


### Select PPP year --------

vars     <- c("ppp_year", "release_version", "adaptation_version")
ppp_v    <- unique(dl_aux$ppp[, ..vars], by = vars)
data.table::setnames(x = ppp_v,
                     old = c("release_version", "adaptation_version"),
                     new = c("ppp_rv", "ppp_av"))

# max release version
m_rv <- ppp_v[ppp_year == py, max(ppp_rv)]

# max adaptation year
m_av <- ppp_v[ppp_year == py & ppp_rv == m_rv, 
              max(ppp_av)]


dl_aux$ppp <- dl_aux$ppp[ppp_year == py 
                         & release_version    == m_rv
                         & adaptation_version == m_av
][, 
  ppp_default := TRUE]


### Select the right CPI --------

cpivar <- paste0("cpi", py)

dl_aux$cpi[, cpi := get(cpivar)]

### Select right Poverty lines table ------

dl_aux$pl <- dl_aux$pl[ppp_year == py
][, 
  ppp_year := NULL]

### Select right Country Profile ------


dl_aux$cp <-
  lapply(dl_aux$cp,
         \(.) { # for each list *key indicators and charts
           lapply(.,
                  \(x) { # for each table inside each list
                    if ("ppp_year" %in% names(x)) {
                      x <-
                        x[ppp_year == py
                          ][,
                            ppp_year := NULL]
                    }
                    x
                  })
         })

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##  PIP inventory ----
pip_inventory <- 
  pipload::pip_find_data(
    inv_file = fs::path(gls$PIP_DATA_DIR, '_inventory/inventory.fst'),
    filter_to_pc = TRUE,
    maindir = gls$PIP_DATA_DIR)




if (".joyn" %in% names(pip_inventory)) {
  pip_inventory[, .joyn := NULL]
}



### pipeline inventory ----

# filter by  year 




pipeline_inventory <- 
  db_filter_inventory(dt        = pip_inventory,
                      pfw_table = dl_aux$pfw)


  