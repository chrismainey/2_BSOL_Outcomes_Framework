library(tidyverse)
library(readxl)
library(DBI)
library(odbc)
library(PHEindicatormethods)

# read metadata file
metadata <- read_xlsx("data/metadata.xlsx")

# establish db connection
sql_connection <- dbConnect(
  odbc(),
  Driver   = "SQL Server",
  Server   = "MLCSU-BI-SQL",
  Database = "EAT_Reporting_BSOL",
  Trusted_Connection = "True"
)

# staging data table
staging_data <- dbGetQuery(
  sql_connection,
  "SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data]"
)

calculate_values <- function(staging_data, metadata, std_pop = NULL) {
  
  # Select metadata cols we need
  metadata_params <- metadata %>%
    select(indicator_id, precalculated, predefined_denominator, value_type, multiplier)
  
  # Join metadata to staging
  df <- staging_data %>%
    left_join(metadata_params, by = "indicator_id")
  
  # Rows to calculate: denominator present AND NOT pre-calculated
  calc_mask <- !is.na(df$denominator) & !(df$precalculated == "Yes")
  
  df_calc <- df[calc_mask, ]
  df_keep <- df[!calc_mask, ]
  
  ## ---- Percentage, ratio  ---- ##
  temp1 <- df_calc %>%
    filter(value_type %in% c("Percentage", "Ratio")) %>%
    phe_proportion(
      x   = numerator,
      n   = denominator,
      confidence  = 0.95,
      type        = "standard",      
      multiplier  = multiplier
    ) %>%
    mutate(
      indicator_value = value,
      lower_ci95      = lowercl,
      upper_ci95      = uppercl
    ) %>% 
    select(-c(value, lowercl, uppercl, precalculated, predefined_denominator, value_type, multiplier))

  ## ---- Crude rate  ---- ##
  temp2 <- df_calc %>%
    filter(value_type == "Crude rate") %>%
    phe_rate(
      x   = numerator,
      n   = denominator,
      confidence  = 0.95,
      type        = "standard",      
      multiplier  = multiplier
    ) %>%
    mutate(
      indicator_value = value,
      lower_ci95      = lowercl,
      upper_ci95      = uppercl
    ) %>% 
    select(-c(value, lowercl, uppercl, precalculated, predefined_denominator, value_type, multiplier))
  
  ## ---- DASR  ---- ##
  temp3 <- df_calc %>%
    filter(value_type == "Directly age standardised rate") %>%
    phe_rate(
      x   = numerator,
      n   = denominator,
      confidence  = 0.95,
      type        = "standard",      
      multiplier  = multiplier
    ) %>%
    mutate(
      indicator_value = value,
      lower_ci95      = lowercl,
      upper_ci95      = uppercl
    ) %>% 
    select(-c(value, lowercl, uppercl, precalculated, predefined_denominator, value_type, multiplier))
  
  return(perc)
}

# Example run (percentage only for now)
result_percentage_only <- calculate_values(staging_data, metadata)

## testing

# Select metadata cols we need
metadata_params <- metadata %>%
  select(indicator_id, precalculated, predefined_denominator, value_type, multiplier)

# Join metadata to staging
df <- staging_data %>%
  left_join(metadata_params, by = "indicator_id")

# Rows to calculate: denominator present AND NOT pre-calculated
calc_mask <- !is.na(df$denominator) & !(df$precalculated == "Yes")

df_calc <- df[calc_mask, ]
df_keep <- df[!calc_mask, ]


## dasr calculation

dasr_df <- df_calc %>% 
  filter(indicator_id== 10)


esp2013_lookup <- PHEindicatormethods::esp2013 %>% 
  as_tibble() %>% 
  rename(stdpop = value) %>% 
  mutate(age_group_code = c(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 18
  )) %>% 
  group_by(age_group_code) %>% 
  summarise(stdpop = sum(stdpop))

byars_ci <- function(o, n, ci=0.95){
  
  z <- qnorm(ci + ((1-ci)/2))
  
  olower <- o * ( 1 - (1/(9*o)) - (z / (3 * sqrt(o))))^3
  
  oupper <- (o+1) * ( 1 - (1/(9*(o+1))) - (z / (3 * sqrt((o+1)))))^3
  
  return(c(o/n,olower/n, oupper/n))
  
}


df_with_std <- dasr_df %>%
  left_join(esp2013_lookup, by = "age_group_code") %>%
  mutate(
    numerator = ifelse(is.na(numerator) | denominator == 0, 0, numerator),
    denominator = ifelse(denominator == 0, 1, denominator)  # Handle cases where Denominator == 0 to avoid errors
  ) %>% 
  group_by(indicator_id, start_date, end_date, imd_code, aggregation_id,
           age_group_code, sex_code, ethnicity_code, creation_date, value_type_code, source_code) %>% 
  calculate_dsr(
    x = numerator,
    n = denominator,
    stdpop = stdpop,
    type = "standard",
    multiplier = 100000
  ) %>% 
  rename(
    indicator_value = value,
    lower_ci95 = lowercl,
    upper_ci95 = uppercl
  ) %>% 
  select(indicator_id, start_date, end_date, numerator, denominator, indicator_value, lower_ci95, 
         upper_ci95, imd_code, aggregation_id,
         age_group_code, sex_code, ethnicity_code, creation_date, value_type_code, source_code)
  ungroup()


