suppressPackageStartupMessages({
  library(readxl)
  library(dplyr)
  library(yaml)
  library(janitor)
})

# -------- helpers --------
stopf <- function(...) stop(sprintf(...), call. = FALSE)

pick_newest_xlsx <- function(dir, pattern = "\\.xlsx$") {
  files <- list.files(dir, pattern = pattern, full.names = TRUE)
  if (length(files) == 0) stopf("No .xlsx files found in: %s", dir)
  
  newest_name <- max(basename(files))      # max() works on characters lexicographically
  newest <- files[basename(files) == newest_name][1]
  newest
}




clean_excel_table <- function(df) {
  # Drop first column (trash)
  if (ncol(df) < 2) stopf("Expected >=2 columns so we can drop the first trash column.")
  df <- df[, -1, drop = FALSE]
  
  # First row becomes colnames
  if (nrow(df) < 2) stopf("Expected >=2 rows so row 1 can be headers and row 2+ data.")
  new_names <- as.character(unlist(df[1, , drop = TRUE]))
  new_names[is.na(new_names) | new_names == ""] <- paste0("v", which(is.na(new_names) | new_names == ""))
  
  # Remove header row from data
  df <- df[-1, , drop = FALSE]
  
  # Set names, but keep Unicode; just make them syntactically safe + unique
  names(df) <- make.names(new_names, unique = TRUE)
  
  df
}


read_clean_single_sheet <- function(path) {
  df <- readxl::read_excel(
    path,
    col_names = FALSE,
    .name_repair = "minimal"
  )
  
  # remove trash first row
  df <- df[-1, ]
  
  # first remaining row becomes column names
  colnames(df) <- as.character(unlist(df[1, ]))
  
  # remove the header row
  df <- df[-1, ]
  
  df
}


read_clean_all_sheets <- function(path) {
  sheets <- readxl::excel_sheets(path)
  if (length(sheets) == 0) stop("No sheets found")
  
  out <- vector("list", length(sheets))
  names(out) <- sheets
  
  for (sh in sheets) {
    df <- readxl::read_excel(
      path,
      sheet = sh,
      col_names = FALSE,
      .name_repair = "minimal"
    )
    
    # remove trash first row
    df <- df[-1, ]
    
    # first remaining row becomes column names
    colnames(df) <- as.character(unlist(df[1, ]))
    
    # remove the header row
    df <- df[-1, ]
    
    out[[sh]] <- df
  }
  
  out
}

# -------- load config --------
cfg <- yaml::read_yaml("config.yml")



# -------- pick newest files --------
file_a <- pick_newest_xlsx(cfg$paths$input_dir_a, cfg$input$pattern)
file_b <- pick_newest_xlsx(cfg$paths$input_dir_b, cfg$input$pattern)



# -------- read + clean --------
data_a <- read_clean_single_sheet(file_a)     # data.frame/tibble
data_b <- read_clean_all_sheets(file_b)       # named list of tibbles (one per sheet)













# -------- create output directory (final structure) --------
run_id <- format(Sys.time(), "%Y-%m-%d")
out_dir <- file.path("Outputs", paste0("run_", run_id))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# -------- run pipeline (function-based contract) --------
source("pipeline.R")



run_pipeline(
  input_a = data_a,
  input_b = data_b,
  out_dir  = out_dir
)







## send email

source("send_email.R")



send_email(out_dir) 





