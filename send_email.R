suppressPackageStartupMessages({
  library(yaml)
})

send_email <- function(out_dir) {
  if (!dir.exists(out_dir)) stop("Output directory does not exist: ", out_dir)
  
  files <- c(
    list.files(out_dir, full.names = TRUE),
    file.path("docs", "Kotsovolos_Lists_Description.txt")
  )
  
  if (length(files) == 0) stop("No files found in output directory; email not sent.")
  
  cfg <- yaml::read_yaml("config.yml")
  email_cfg <- cfg$email
  
  if (!isTRUE(email_cfg$enabled)) {
    message("Email disabled in config.yml; skipping send.")
    return(invisible(NULL))
  }
  
  to_str  <- if (length(email_cfg$to)  > 0) paste(email_cfg$to,  collapse = ";") else ""
  cc_str  <- if (length(email_cfg$cc)  > 0) paste(email_cfg$cc,  collapse = ";") else ""
  bcc_str <- if (length(email_cfg$bcc) > 0) paste(email_cfg$bcc, collapse = ";") else ""
  
  run_date <- format(Sys.Date(), "%d/%m")
  subject <- gsub("\\{run_date\\}", run_date, email_cfg$subject)
  body    <- gsub("\\{run_date\\}", run_date, email_cfg$body)
  
  files_norm <- normalizePath(files, winslash = "\\", mustWork = TRUE)
  
  files_ps <- paste(
    sprintf("'%s'", gsub("'", "''", files_norm)),
    collapse = ","
  )
  
  attach_arr <- paste0("@(", files_ps, ")")
  
  
  esc_ps <- function(x) gsub("'", "''", x, fixed = TRUE)
  
  ps_cmd <- paste0(
    "$ErrorActionPreference = 'Stop'; ",
    "$outlook = New-Object -ComObject Outlook.Application; ",
    "$mail = $outlook.CreateItem(0); ",
    "$mail.To = '",  esc_ps(to_str),  "'; ",
    "$mail.CC = '",  esc_ps(cc_str),  "'; ",
    "$mail.BCC = '", esc_ps(bcc_str), "'; ",
    "$mail.Subject = '", esc_ps(subject), "'; ",
    "$mail.Body = '", esc_ps(body), "'; ",
    "$files = ", attach_arr, "; ",
    "foreach ($f in $files) { if (Test-Path $f) { $null = $mail.Attachments.Add($f) } }; ",
    "$mail.Send(); "
  )
  
  system2("powershell.exe", args = c("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd))
  
  invisible(TRUE)
}
