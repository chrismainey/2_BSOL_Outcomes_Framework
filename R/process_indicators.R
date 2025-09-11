library(tidyverse)
library(readxl)
library(DBI)
library(odbc)
library(PHEindicatormethods)

#1. Read metadata file ---------------------------------------------------------
metadata <- read_xlsx("data/metadata.xlsx")

#2. Establish db connection ----------------------------------------------------
sql_connection <- dbConnect(
  odbc(),
  Driver   = "SQL Server",
  Server   = "MLCSU-BI-SQL",
  Database = "EAT_Reporting_BSOL",
  Trusted_Connection = "True"
)

#3. Staging data table ---------------------------------------------------------
staging_data <- dbGetQuery(
  sql_connection,
  "SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data]"
)

#4. Function to calculate values ----------------------------------------------- 

calculate_values <- function(staging_data, metadata) {
    
    # --- Standard population lookup
    esp2013_lookup <- PHEindicatormethods::esp2013 %>%
      as_tibble() %>%
      rename(stdpop = value) %>%
      mutate(age_group_code = c(1:18, 18)) %>%
      group_by(age_group_code) %>%
      summarise(stdpop = sum(stdpop), .groups = "drop")
    
    # --- Join metadata
    df <- staging_data %>%
      left_join(
        metadata %>% select(indicator_id, precalculated, multiplier),
        by = "indicator_id"
      )
    
    # --- Partition rows 
    needs_calc <- !is.na(df$denominator) & df$precalculated != "Yes"
    df_calc <- df[needs_calc, ]
    df_keep <- df[!needs_calc, ] %>% select(-c(precalculated, multiplier))
    
    # --- Helper to tidy outputs to a consistent schema ------------------------
    # For %, crude and ratio outputs
    tidy_output <- function(x) {
      x %>%
        mutate(
          indicator_value = value,
          lower_ci95 = lowercl,
          upper_ci95 = uppercl
        ) %>%
        select(
          indicator_id, start_date, end_date, numerator, denominator,
          indicator_value, lower_ci95, upper_ci95, imd_code, aggregation_id,
          age_group_code, sex_code, ethnicity_code, creation_date,
          value_type_code, source_code
        )
    }
    
    # -------- Percentage ------------------------------------------------------
    temp1 <- df_calc %>%
      filter(value_type_code == 2) %>%
      phe_proportion(
        x = numerator, n = denominator,
        confidence = 0.95, type = "standard",
        multiplier = multiplier
      ) %>%
      tidy_output()
    
    # -------- Crude rate & Ratio ----------------------------------------------
    temp2 <- df_calc %>%
      filter(value_type_code %in% c(3, 7)) %>%
      phe_rate(
        x = numerator, n = denominator,
        confidence = 0.95, type = "standard",
        multiplier = multiplier
      ) %>%
      tidy_output()
    
    # -------- Directly age standardised rate (DASR) ---------------------------
    # Define the grouping keys that represent ONE population unit 
    dasr_keys <- c(
      "indicator_id", "start_date", "end_date",
      "imd_code", "aggregation_id", "age_group_code",
      "sex_code", "ethnicity_code",
      "creation_date", "value_type_code", "source_code"
    )
    
    # All DASR rows (including denominator == 0)
    df_dasr_all <- df_calc %>%
      filter(value_type_code == 4) %>%
      left_join(esp2013_lookup, by = "age_group_code")
    
    # Subset eligible for calculation
    df_dasr_calc <- df_dasr_all %>%
      filter(precalculated != "Yes", !is.na(denominator), denominator > 0, !is.na(stdpop)) %>%
      mutate(numerator = if_else(is.na(numerator), 0, numerator))
    
    # Calculate group-level DASR 
    dasr_group_results <- df_dasr_calc %>%
      group_by(across(all_of(dasr_keys))) %>%
      calculate_dsr2(
        x = numerator,
        n = denominator,
        stdpop = stdpop,
        type = "standard",
        multiplier = 100000
      ) %>%
      rename(
        numerator = total_count,
        denominator = total_pop,
        indicator_value = value,
        lower_ci95 = lowercl,
        upper_ci95 = uppercl
      ) %>%
      ungroup()
    
    # Join group-level DASR back to ALL DASR rows 
    temp3 <- df_dasr_all %>%
      select(
        indicator_id, start_date, end_date, numerator, denominator,
        imd_code, aggregation_id, age_group_code, sex_code, ethnicity_code,
        creation_date, value_type_code, source_code
      ) %>%
      left_join(
        dasr_group_results %>%
          select(
            all_of(dasr_keys),
            indicator_value, lower_ci95, upper_ci95, numerator, denominator
          ),
        by = dasr_keys,
        suffix = c("", "_grp")
      ) %>%
      # prefer the group-level numerator/denominator when present; keep original otherwise
      mutate(
        numerator = coalesce(numerator_grp, numerator),
        denominator = coalesce(denominator_grp, denominator)
      ) %>%
      select(-ends_with("_grp")) %>%
      # final column order
      select(
        indicator_id, start_date, end_date, numerator, denominator,
        indicator_value, lower_ci95, upper_ci95, imd_code, aggregation_id,
        age_group_code, sex_code, ethnicity_code, creation_date,
        value_type_code, source_code
      )
    
    # -------- Combine all outputs ---------------------------------------------
    output <- bind_rows(
      df_keep,          # Rows not calculated (pre-calculated or denominator is NA)
      temp1,            # Percentage
      temp2,            # Crude rate & Ratio
      temp3             # DASR 
    )
    
    
    return(output)
  }
  

# Run function
result <- calculate_values(staging_data, metadata)




































  






















