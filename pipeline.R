


run_pipeline <- function(input_a, input_b, out_dir) {
  
  # bind inputs to the names your pipeline already uses
  kotsovolos       <- input_a
  kots_extra_data <- input_b
  
  library(dplyr)
  library(readxl)
  library(lubridate)
  library(writexl)
  library(stringr)
  library(openxlsx)
  library(tidyr)
  library(ggplot2)
  library(scales)
  library(janitor)
  library(readr)





## split the dtabase to cy and gr(main + franchise)



kotsovolos_cy <- kotsovolos %>%
  filter(
    Project == "KOTSOVOLOS CY"
  )

kotsovolos <- kotsovolos %>%
  filter(
    !Project == "KOTSOVOLOS CY"
  )


## remove specified last action categories


actions2remove <- c("ΕΞΟΦΛΗΣΗ SOLD OUT", "Παραλαβή δικαιολογητικών", "Άρνηση επικοινωνίας", 
                    "Άρνηση επικοινωνίας με εξωτερικούς συνεργάτες")


# from kotsovolos + franchise
kotsovolos<- kotsovolos %>%
  filter(
    !`Last Action` %in% actions2remove 
  )

# from cyprus
kotsovolos_cy<- kotsovolos_cy %>%
  filter(
    !`Last Action` %in% actions2remove 
  )



kotsovolos_bckp <- kotsovolos
kotsovolos_bckp_cy <- kotsovolos_cy






# filter out delays days > 10, etc

kotsovolos <- kotsovolos %>%
  filter(
    `Delay Days` >= 10,
    `Συνολικό υπόλοιπο δανείου` >= 20,
    `Ληξιπρόσθεσμο ποσό` >= 20,
    `Ενεργό Settlement` == 0
  )



# same for cyprus

kotsovolos_cy <- kotsovolos_cy %>%
  filter(
    `Delay Days` >= 10,
    `Συνολικό υπόλοιπο δανείου` >= 20,
    `Ληξιπρόσθεσμο ποσό` >= 20,
    `Ενεργό Settlement` == 0
  )





kotsovolos_flagged <- kotsovolos %>%
  mutate(
    # Core helpers used everywhere
    last_payment_date = dmy(`Ημερομηνία last payment`),
    days_since_last_payment = as.numeric(Sys.Date() - last_payment_date),
    
    any_valid_phone = (`Έγκυρα τηλέφωνα κινητό` > 0 |
                         `Πλήθος με έγκυρα τηλέφωνα πλην κινητού` > 0),
    
    # Creme de la creme flag
    manual_flag = 
      `Συνολικό υπόλοιπο δανείου` >= 500 &
      any_valid_phone &
      !is.na(last_payment_date) &
      days_since_last_payment <= 90 &
      `Πλήθος μηνιαίων πληρωμών τους τελευταίους 12 μήνες` >= 3 &
      `Current account recovery rate` >= 30
  )





# Top cases: manual
kotsovolos_manual <- kotsovolos_flagged %>%
  filter(manual_flag)

# Remaining accounts where you apply the existing priority logic
kotsovolos_base <- kotsovolos_flagged %>%
  filter(!manual_flag)
















###



###



###



###



### newer strategy

# unpack components (rename for convenience)

df_callback   <- kots_extra_data$`Υποθέσεις με επανάκληση`
df_extra_tel  <- kots_extra_data$`Ενεργά τηλέφωνα λοιπών υποθέσεω`
df_timezones  <- kots_extra_data$`Υποθέσεις - Ζώνες ώρας δίχως κλ`
df_high_calls <- kots_extra_data$`Υποθέσεις με > 20 κλήσεις ανά μ`

promise_actions <- c("Υπόσχεση κατάθεσης",
                     "Υπόσχεση κατάθεσης (Μετά από settlement)",
                     "Υπόσχεση Κατάθεσης (down payment Διακ/σμού)",
                     "Υπόσχεση κατάθεσης (Μετά από διακανονισμό)",
                     "Υπόσχεση κατάθεσης (Από εισερχόμενη κλήση)",
                     "Υπόσχεση Υλοποίησης Ρύθμισης",
                     "Υπόσχεση έναντι κατάθεσης (από εισερχόμενη κλήση)" )

# flags per source



# 1) has_promise at case level
kotsovolos_base <- kotsovolos_base %>%
  mutate(
    promise_flag =
      !is.na(`Strongest last action μήνα`) &
      `Strongest last action μήνα` %in% promise_actions &
      !is.na(last_payment_date) &
      as.Date(last_payment_date) <= Sys.Date() - 31 &
      `Counter ημερολογιακών ημερών από το τελευταίο RPC action` <= 40
  )





# 2) has_callback at case level
callback_flag <- df_callback %>%
  distinct(`Κωδικός Υπόθεσης`) %>%
  mutate(has_callback = TRUE)

# 3) extra_valid_phone at customer level
extra_phone_flag <- df_extra_tel %>%
  distinct(`Κωδικός Πελάτη`) %>%
  mutate(extra_valid_phone = TRUE)

# 4) no_calls_ever + time-band info at case level
nocall_flag <- df_timezones %>%
  select(`Κωδικός Υπόθεσης`, `No Calls Ever`) %>%
  distinct() %>%
  rename(no_calls_ever = `No Calls Ever`)

# 5) high_calls_pm at case level (belongs to "cases with > 20 calls per month")
high_calls_flag <- df_high_calls %>%
  distinct(`Κωδικός Υπόθεσης`) %>%
  mutate(high_calls_pm = TRUE)




kots <- kotsovolos_base %>%
  # join extra behaviour / contact flags
  left_join(callback_flag, by = c("Account Number" = "Κωδικός Υπόθεσης")) %>%
  left_join(extra_phone_flag, by = c("CIF" = "Κωδικός Πελάτη")) %>%
  left_join(nocall_flag,    by = c("Account Number" = "Κωδικός Υπόθεσης")) %>%
  left_join(high_calls_flag,by = c("Account Number" = "Κωδικός Υπόθεσης")) %>%
  mutate(
    has_callback      = coalesce(has_callback, FALSE),
    extra_valid_phone = coalesce(extra_valid_phone, FALSE),
    no_calls_ever = no_calls_ever %in% c("YES", "Yes", "Y", "1"),
    high_calls_pm     = coalesce(high_calls_pm, FALSE)
  ) %>%
  # base numeric / logical fields
  mutate(
    delay_days              = as.integer(`Delay Days`),
    balance                 = as.numeric(`Συνολικό υπόλοιπο δανείου`),
    total_collections       = as.numeric(`Συνολικές εισπράξεις στο account από την έναρξη της διαχείρισης`),
    recovery_rate           = as.numeric(`Current account recovery rate`) / 100,
    last_payment_date       = dmy(`Ημερομηνία last payment`),
    days_since_last_payment = as.integer(Sys.Date() - last_payment_date),
    n_payments_12m          = as.integer(`Πλήθος μηνιαίων πληρωμών τους τελευταίους 12 μήνες`),
    valid_mobile            = as.integer(`Έγκυρα τηλέφωνα κινητό`),
    valid_other             = as.integer(`Πλήθος με έγκυρα τηλέφωνα πλην κινητού`),
    no_valid_phone          = as.integer(`Χωρίς εγκυρα τηλέφωνα`),
    valid_phone_raw         = no_valid_phone == 0 & (valid_mobile == 1 | valid_other > 0),
    # corrected phone validity using external active phones
    valid_phone             = valid_phone_raw | extra_valid_phone,
    calls_60                = as.integer(`Πλήθος κλήσεων τις τελευταίες 60 ημέρες`),
    calls_120               = as.integer(`Πλήθος κλήσεων τις τελευταίες 120 ημέρες`),
    rpc_60                  = as.integer(`RPC τις τελευταίες 60 ημέρες`),
    rpc_days_since          = as.integer(`Counter ημερολογιακών ημερών από το τελευταίο RPC action`),
    broken_promise          = as.integer(`Current Broken promise`)
  )



## temporary 
# broken of the month


broken_month <- kots %>%
  filter( promise_flag == T,
          is.na(days_since_last_payment) | days_since_last_payment >= 30,
          rpc_days_since <= 30,
          
  )




# remove from dataset
kots <- kots %>%
  anti_join(
    bind_rows(broken_month) 
    %>% select(`Account Number`),
    by = "Account Number"
  )

# broken of the quarter
broken_quarter <- kots %>%
  filter( promise_flag == T,
          is.na(days_since_last_payment) | days_since_last_payment >= 90,
          rpc_days_since >= 30 & rpc_days_since <= 90
  )



#remove from dataset
kots <- kots %>%
  anti_join(
    bind_rows(broken_quarter) 
    %>% select(`Account Number`),
    by = "Account Number"
  )



## end of temporary 
###











kots_base <- kots %>%
  mutate(
    priority_base = case_when(
      # Priority 1 – core focus
      valid_phone &
        balance >= 20 &
        (total_collections > 0 |
           n_payments_12m >= 1 
        ) &
        (days_since_last_payment <= 180 |
           rpc_60 > 0 |
           rpc_days_since <= 120) &
        delay_days <= 1800 &
        calls_120 <= 30 &
        broken_promise <= 1                                       ~ 1L,
      
      # Priority 2 – broad active / reactivation
      valid_phone &
        balance >= 20 &
        delay_days <= 2000 &
        recovery_rate >= 0.05 & recovery_rate <= 0.9 &
        (days_since_last_payment <= 365 |
           total_collections > 0 |
           n_payments_12m >= 1)                                   ~ 2L,
      
      # Priority 3 – promising with positive history (with or without valid phone)
      recovery_rate >= 0.05 & recovery_rate <= 0.9 &
        (total_collections > 0 |
           n_payments_12m > 0 |
           recovery_rate >= 0.05) &
        (days_since_last_payment <= 730 |
           delay_days <= 2200)                                    ~ 3L,
      
      # Priority 5 – truly hard cases
      (
        (total_collections == 0 & recovery_rate == 0 &
           n_payments_12m == 0 &
           (is.na(last_payment_date) | days_since_last_payment > 730) &
           delay_days > 2400) |
          (calls_120 > 60 &
             total_collections == 0 &
             n_payments_12m == 0 &
             recovery_rate == 0)
      )                                                           ~ 5L,
      
      # Priority 4 – mid-tier (some signal or some contact)
      (valid_phone |
         total_collections > 0 |
         n_payments_12m > 0 |
         recovery_rate > 0)                                       ~ 4L,
      
      # Priority 6 – everything else
      TRUE                                                        ~ 6L
    )
  )









kots_base <- kots_base %>%
  mutate(
    priority_final = priority_base,
    
    # 1) Promise: always treat as top priority
    priority_final = if_else(promise_flag & delay_days > 5, 1L, priority_final),
    
    # 2) Callback: bump up one level if > 2
    priority_final = if_else(
      has_callback & priority_final > 2,
      priority_final - 1L,
      priority_final
    ),
    
    # 3) High calls per month + zero behaviour -> force hard bucket (>=5)
    zero_behaviour = (total_collections == 0 &
                        n_payments_12m == 0 &
                        recovery_rate == 0),
    
    priority_final = if_else(
      high_calls_pm & zero_behaviour & priority_final < 5L,
      5L,
      priority_final
    ),
    
    # 4) Never called at all – don't leave as 5 (give a chance in mid-tier)
    priority_final = if_else(
      no_calls_ever & priority_final == 5L,
      4L,
      priority_final
    )
  )




























## summary for plot


summary_table <- kots_base %>%
  group_by(priority_final) %>%
  summarise(
    total_amount = sum(balance, na.rm = TRUE),
    n_accounts   = n(),
    .groups = "drop"
  ) %>%
  mutate(
    label = paste0(
      scales::comma(total_amount, accuracy = 1), "€ (n = ", n_accounts, ")"
    )
  )





## plot 




ggplot(summary_table, aes(x = factor(priority_final), y = total_amount)) +
  geom_col() +
  geom_text(aes(label = label), vjust = -0.5, size = 4) +
  labs(
    title = "Total Loan Amount and Case Count per Priority",
    x = "Priority",
    y = "Total Outstanding Amount (€)"
  )





kotsovolos_path <- out_dir

priority_list <- lapply(1:6, function(p) {
  kots_base %>%
    filter(priority_final == p) %>%
    pull(`Account Number`)
})

# export 6 txt files at the predefined path
for (p in 1:6) {
  file_name <- file.path(kotsovolos_path, paste0(p + 2, ".priority_", p, ".txt"))
  writeLines(priority_list[[p]], file_name, useBytes = TRUE)
}


## export manual
output_file <- file.path(kotsovolos_path, "manual.txt")

writeLines(
  kotsovolos_manual$`Account Number`,
  con = output_file,
  useBytes = TRUE
)













output_file <- file.path(kotsovolos_path, "1.broken_month.txt")

writeLines(
  broken_month$`Account Number`,
  con = output_file,
  useBytes = TRUE
)

output_file <- file.path(kotsovolos_path, "2.broken_quarter.txt")

writeLines(
  broken_quarter$`Account Number`,
  con = output_file,
  useBytes = TRUE
)




##  cyprus strategy


kots_cy <- kotsovolos_cy %>%
  mutate(
    delay_days              = as.integer(`Delay Days`),
    balance                 = as.numeric(`Συνολικό υπόλοιπο δανείου`),
    total_collections       = as.numeric(`Συνολικές εισπράξεις στο account από την έναρξη της διαχείρισης`),
    recovery_rate           = as.numeric(`Current account recovery rate`) / 100,
    last_payment_date       = dmy(`Ημερομηνία last payment`),
    days_since_last_payment = as.integer(Sys.Date() - last_payment_date),
    n_payments_12m          = as.integer(`Πλήθος μηνιαίων πληρωμών τους τελευταίους 12 μήνες`),
    valid_mobile            = as.integer(`Έγκυρα τηλέφωνα κινητό`),
    valid_other             = as.integer(`Πλήθος με έγκυρα τηλέφωνα πλην κινητού`),
    no_valid_phone          = as.integer(`Χωρίς εγκυρα τηλέφωνα`),
    valid_phone             = no_valid_phone == 0 & 
      (valid_mobile == 1 | valid_other > 0),
    calls_60                = as.integer(`Πλήθος κλήσεων τις τελευταίες 60 ημέρες`),
    calls_120               = as.integer(`Πλήθος κλήσεων τις τελευταίες 120 ημέρες`),
    rpc_60                  = as.integer(`RPC τις τελευταίες 60 ημέρες`),
    rpc_days_since          = as.integer(`Counter ημερολογιακών ημερών από το τελευταίο RPC action`),
    broken_promise          = as.integer(`Current Broken promise`)
  )


kots_cy <- kots_cy %>%
  mutate(
    priority = case_when(
      # Priority 1 – core focus 
      valid_phone &
        balance >= 20 &
        (total_collections > 0 |
           n_payments_12m >= 1 |
           recovery_rate >= 0.02) &
        (days_since_last_payment <= 180 |
           rpc_60 > 0 |
           rpc_days_since <= 120) &
        delay_days <= 1800 &
        calls_120 <= 30 &
        broken_promise <= 1                                       ~ 1L,
      
      # Priority 2 – broad active / reactivation 
      valid_phone &
        balance >= 20 &
        delay_days <= 2000 &
        (days_since_last_payment <= 365 |
           recovery_rate > 0 |
           total_collections > 0 |
           n_payments_12m >= 1)                                   ~ 2L,
      # Everything else
      TRUE ~ 3L
      
    )
  )



priority_list_cy <- lapply(1:3, function(p) {
  kots_cy %>%
    filter(priority == p) %>%
    pull(`Account Number`)
})

# export 6 txt files at the predefined path
for (p in 1:3) {
  file_name <- file.path(kotsovolos_path, paste0("priority_", p, ".txt"))
  writeLines(priority_list_cy[[p]], file_name, useBytes = TRUE)
}





















##



## export text
output_file <- file.path(kotsovolos_path, "kotsovolos_text_cy.txt")

writeLines(
  kotsovolos_cy$`Account Number`,
  con = output_file,
  useBytes = TRUE
)





### end of pipeline


}