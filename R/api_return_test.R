library(tidyverse)

dt <- read_csv("./data/test_data_20250908.csv")

dt |> 
  group_by(`Area Type`) |> 
  summarise(count = n())


b <-
  dt |> 
  filter(`Area Type` == "GPs") |> 
  group_by(`Parent Name`, `Area Code`) |> 
  summarise(n())


b |> 
  group_by(`Parent Name`) |> 
  summarise(n())
