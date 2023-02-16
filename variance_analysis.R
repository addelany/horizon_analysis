library(tidyverse)

## Stage 2

vars <- FLAREr:::arrow_env_vars()

s3_s2 <- arrow::s3_bucket("drivers/noaa/gefs-v12/stage2/parquet/0", endpoint_override = "s3.flare-forecast.org", anonymous = TRUE)

df_s2 <- arrow::open_dataset(s3_s2) |> 
  dplyr::filter(site_id == "bvre", 
                reference_datetime > lubridate::as_datetime('2023-01-01 00:00:00'), 
                reference_datetime < lubridate::as_datetime('2023-01-03 00:00:00')) %>% 
  group_by(reference_datetime, datetime, variable) %>% 
  summarize(var = var(prediction)) %>% 
  collect()

FLAREr:::unset_arrow_vars(vars)

df_s2 |> 
  mutate(horizon = as.numeric(datetime - reference_datetime)) |> 
  ggplot(aes(x = horizon, y = var)) +
  geom_line() + 
  facet_wrap(~variable, scale = "free")


## Stage 1

vars <- FLAREr:::arrow_env_vars()
s3_s1 <- arrow::s3_bucket("drivers/noaa/gefs-v12/stage1/0", endpoint_override = "s3.flare-forecast.org", anonymous = TRUE)
df_s1 <- arrow::open_dataset(s3_s1) |> 
  dplyr::filter(site_id == "bvre", 
                reference_datetime > lubridate::as_datetime('2023-01-01 00:00:00'), 
                reference_datetime < lubridate::as_datetime('2023-01-03 00:00:00')) %>% 
  group_by(reference_datetime, datetime, variable) %>% 
  summarize(var = sd(prediction)) %>% 
  collect() |> 
  mutate(horizon = as.numeric(datetime - reference_datetime))

FLAREr:::unset_arrow_vars(vars)

df_s1 |> 
  ggplot(aes(x = horizon, y = var)) +
  geom_line() + 
  facet_wrap(~variable, scale = "free")


## raw prediction values

s3 <- arrow::s3_bucket("drivers/noaa/gefs-v12/stage1/0", endpoint_override = "s3.flare-forecast.org", anonymous = TRUE)
df2 <- arrow::open_dataset(s3) |> 
  dplyr::filter(site_id == "bvre", 
                reference_datetime > lubridate::as_datetime('2023-01-01 00:00:00'), 
                reference_datetime < lubridate::as_datetime('2023-01-03 00:00:00')) %>% 
  collect() |> 
  mutate(horizon = as.numeric(datetime - reference_datetime))

ggplot() +
  geom_line(data = df2, aes(x = horizon, y = prediction, group = parameter)) + 
  facet_wrap(~variable, scale = "free")

ggplot() +
  geom_line(data = df, aes(x = horizon, y = var)) + 
  facet_wrap(~variable, scale = "free")
