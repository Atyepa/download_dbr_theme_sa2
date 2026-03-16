---- 0) Libraries ----
library(tidyverse)
library(readr)
library(janitor)
library(scales)
library(mgcv)
library(broom)
library(readxl)              
library(DT)     
library(jsonlite)
library(readr)
library(dplyr)             
library(httr)             

# ==========================================================
# ABS DBR (SA2) downloader using download.file(method="wininet")
# - Avoids httr/curl timeouts behind corporate proxies
# - Writes CSV(s) to getwd()
# ==========================================================

# Packages
suppressPackageStartupMessages({
  library(jsonlite)
  library(readr)
  library(dplyr)
  library(httr)
})

# Increase timeout (seconds)
options(timeout = max(120, getOption("timeout")))
httr::set_config(httr::timeout(120))

# Auto-detect Windows corporate proxy with NTLM authentication
if (.Platform$OS.type == "windows") {
  proxy_url <- tryCatch(curl::ie_get_proxy_for_url("https://www.arcgis.com"), error = function(e) "")
  if (nzchar(proxy_url)) {
    message("Using proxy: ", proxy_url)
    httr::set_config(httr::use_proxy(proxy_url, auth = "ntlm"))
  }
}

# Helper: POST form data to an ArcGIS REST endpoint and return parsed JSON
arcgis_post_json <- function(endpoint, fields) {
  resp <- httr::POST(endpoint, body = fields, encode = "form")
  httr::stop_for_status(resp)
  jsonlite::fromJSON(httr::content(resp, as = "text", encoding = "UTF-8"),
                     simplifyDataFrame = TRUE)
}

# Helper: resolve ArcGIS itemId -> Feature Service URL
get_feature_service_url <- function(item_id) {
  js <- arcgis_post_json(
    sprintf("https://www.arcgis.com/sharing/rest/content/items/%s", item_id),
    list(f = "json")
  )
  if (is.null(js$url) || !nzchar(js$url))
    stop("Could not resolve Feature Service URL for item_id = ", item_id)
  js$url
}

# Helper: fetch ALL rows from a FeatureServer layer as JSON pages and bind
arcgis_fetch_csv_pages <- function(service_url, layer = 0, file_stub = "dbr_theme",
                                   where = "1=1", out_fields = "*",
                                   page_size = 2000, max_pages = 500, verbose = TRUE) {
  base_query <- sprintf("%s/%d/query", service_url, layer)
  all_rows <- list()
  offset <- 0L
  page <- 1L

  repeat {
    if (verbose) message(sprintf("Fetching page %d (offset=%d)...", page, offset))
    js <- tryCatch(
      arcgis_post_json(base_query, list(
        where             = where,
        outFields         = out_fields,
        returnGeometry    = "false",
        f                 = "json",
        resultRecordCount = as.character(page_size),
        resultOffset      = as.character(offset)
      )),
      error = function(e) e
    )
    if (inherits(js, "error")) stop("Fetch failed: ", conditionMessage(js))
    if (!is.null(js$error)) stop("ArcGIS error ", js$error$code, ": ", js$error$message)

    features <- js$features
    if (is.null(features) || nrow(features) == 0) break

    df <- as.data.frame(features$attributes)
    n <- nrow(df)
    if (verbose) message("  -> rows: ", n)
    
    if (n == 0) break
    all_rows[[page]] <- df
    
    # Heuristic: last page often returns < page_size rows
    if (n < page_size) break
    if (page >= max_pages) {
      warning("Reached max_pages; stopping early.")
      break
    }
    
    offset <- offset + n
    page <- page + 1L
  }
  
  if (length(all_rows) == 0) {
    warning("No data returned.")
    return(tibble::tibble())
  }
  
  bind_rows(all_rows)
}

# Main function: resolve item -> fetch all pages -> write CSV -> return data frame
download_dbr_theme_sa2 <- function(item_id, layer = 0, file_stub = "dbr_theme",
                                   where = "1=1", out_fields = "*",
                                   page_size = 2000, max_pages = 500, verbose = TRUE) {
  if (verbose) message("Resolving Feature Service URL for item: ", item_id)
  service_url <- get_feature_service_url(item_id)
  if (verbose) message("Service URL: ", service_url)

  # Inspect service metadata and auto-detect layer ID
  svc_meta <- arcgis_post_json(service_url, list(f = "json"))
  if (!is.null(svc_meta$layers) && is.data.frame(svc_meta$layers)) {
    if (verbose) message("Available layers: ", paste(
      paste0(svc_meta$layers$id, " = ", svc_meta$layers$name), collapse = ", "))
    if (!layer %in% svc_meta$layers$id) {
      layer <- svc_meta$layers$id[1]
      if (verbose) message("Specified layer not found; using layer ", layer)
    }
  }

  df <- arcgis_fetch_csv_pages(
    service_url = service_url,
    layer       = layer,
    file_stub   = file_stub,
    where       = where,
    out_fields  = out_fields,
    page_size   = page_size,
    max_pages   = max_pages,
    verbose     = verbose
  )

  out_file <- file.path(getwd(), paste0(file_stub, ".csv"))
  readr::write_csv(df, out_file)
  if (verbose) message("Written to: ", out_file, " (", nrow(df), " rows)")

  df
}

# ----------------------------------------------------------
# Download TWO themes (Population & People; Family & Community)
# ----------------------------------------------------------

# 1) Population & People (SA2, 2021) — DBR (Digital Atlas item)
# Ref: https://www.arcgis.com/home/item.html?id=eeae237cd5dd4c8d884f6efdbc8c0fa0
pop_people_csv <- download_dbr_theme_sa2(
  item_id  = "eeae237cd5dd4c8d884f6efdbc8c0fa0",
  layer    = 1,
  file_stub = "dbr_population_people_2023",
  page_size = 1000,
  verbose   = TRUE
)

# 2) Family & Community (SA2, 2021) — DBR (Digital Atlas item)
# Ref: https://www.arcgis.com/home/item.html?id=90a4865dffb145ae865d7db6b49c823b
fam_comm_csv <- download_dbr_theme_sa2(
  item_id  = "90a4865dffb145ae865d7db6b49c823b",
  layer    = 1,
  file_stub = "dbr_family_community_2024",
  page_size = 1000,
  verbose   = TRUE
)

# 3) Persons born overseas (SA2, Nov 2025) — DBR (Digital Atlas item)
# Ref: https://www.arcgis.com/home/item.html?id=0fa3c3b2c9924d3dae6acedaa35c76c4
born_overseas_csv <- download_dbr_theme_sa2(
  item_id   = "0fa3c3b2c9924d3dae6acedaa35c76c4",
  layer     = 1,
  file_stub = "dbr_persons_born_overseas_2025",
  page_size = 1000,
  verbose   = TRUE
)

# 4) Education & Employment (SA2, Nov 2024) — DBR (Digital Atlas item)
# Ref: https://www.arcgis.com/home/item.html?id=1e7b885722924025a3f15c519dc9233c
edu_empl_csv <- download_dbr_theme_sa2(
  item_id   = "1e7b885722924025a3f15c519dc9233c",
  layer     = 1,
  file_stub = "dbr_education_employment_2024",
  page_size = 1000,
  verbose   = TRUE
)

pop_people_csv
fam_comm_csv
born_overseas_csv
edu_empl_csv
